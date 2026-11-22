package playground.redisfail;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

/**
 * 블로그 조회수 기록을 작게 옮긴 것. 클라이언트 설정은 운영과 같다:
 * 명령 타임아웃 300ms(TimeoutOptions), 연결·핸드셰이크 1초(RedisURI timeout), 자동 재연결.
 * 연결이 끊긴 동안의 동작(reject)만 실험에서 바꾼다.
 *
 * <ul>
 *   <li>POSTGRES: 한 트랜잭션에서 content_view INSERT … ON CONFLICT, 처음이면 view_count +1
 *   <li>REDIS: 한 트랜잭션 안에서 SET NX EX 24h로 판정 → 처음이면 view_count +1 → ZINCRBY, EXPIRE 8일 → COMMIT
 *   <li>REDIS_SCRIPT: 판정·순위 증가·만료를 Lua 스크립트 한 번(EVAL)으로 → 처음이면 view_count +1
 * </ul>
 */
final class Views implements AutoCloseable {
    enum Mode { POSTGRES, REDIS, REDIS_SCRIPT }

    enum Admission { FIRST, REPEAT, UNAVAILABLE }

    record Result(Admission admission, long nanos) {}

    static final Duration COMMAND_TIMEOUT = Duration.ofMillis(300);
    static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(1);
    static final long DEDUP_SECONDS = 86_400;
    static final long RANKING_SECONDS = 8 * 86_400;
    static final String SCRIPT = """
            if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1]) then
              redis.call('ZINCRBY', KEYS[2], 1, ARGV[2])
              redis.call('EXPIRE', KEYS[2], ARGV[3])
              return 1
            end
            return 0
            """;

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> redis;
    private final Db db;

    Views(int redisPort, Db db, boolean rejectWhileDisconnected) {
        this.db = db;
        client = RedisClient.create(RedisURI.builder()
                .withHost("127.0.0.1")
                .withPort(redisPort)
                .withTimeout(CONNECT_TIMEOUT)
                .build());
        client.setOptions(ClientOptions.builder()
                .autoReconnect(true)
                .disconnectedBehavior(rejectWhileDisconnected
                        ? ClientOptions.DisconnectedBehavior.REJECT_COMMANDS
                        : ClientOptions.DisconnectedBehavior.DEFAULT)
                .timeoutOptions(TimeoutOptions.enabled(COMMAND_TIMEOUT))
                .build());
        connection = client.connect(); // 앱이 트래픽을 받기 전에 연결을 여는 것과 같다
        redis = connection.sync();
    }

    Result record(Mode mode, long post, String session) {
        long start = System.nanoTime();
        Admission a = switch (mode) {
            case POSTGRES -> postgres(post, session);
            case REDIS -> redisThreeCalls(post, session);
            case REDIS_SCRIPT -> redisScript(post, session);
        };
        return new Result(a, System.nanoTime() - start);
    }

    /** Redis 판정과 순위만(PostgreSQL 없이). 메모리 실험에서 쓴다. */
    Admission redisOnly(long post, String session) {
        Admission a = admit(post, session);
        if (a == Admission.FIRST) rank(post);
        return a;
    }

    /** 최근 순위. Redis가 답하지 못하면 empty(앱은 누적 조회수 순으로 대신 보여 준다). */
    Optional<List<String>> ranking() {
        try {
            String today = dayKey();
            return Optional.of(redis.zunion(today, today).stream().toList());
        } catch (RedisException e) {
            return Optional.empty();
        }
    }

    private Admission postgres(long post, String session) {
        try (Connection c = db.ds.getConnection()) {
            int inserted;
            try (var ps = c.prepareStatement(
                    "INSERT INTO content_view(content_id,session_id) VALUES (?,?) ON CONFLICT DO NOTHING")) {
                ps.setLong(1, post);
                ps.setString(2, session);
                inserted = ps.executeUpdate();
            }
            if (inserted > 0) increment(c, post);
            c.commit();
            return inserted > 0 ? Admission.FIRST : Admission.REPEAT;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    /** 운영과 같은 순서: 한 트랜잭션 안에서 판정 → 조회수 +1 → 순위 → 만료 → COMMIT. */
    private Admission redisThreeCalls(long post, String session) {
        try (Connection c = db.ds.getConnection()) {
            Admission a = admit(post, session);
            if (a == Admission.FIRST) {
                increment(c, post);
                rank(post); // UPDATE한 행의 잠금을 쥔 채로 Redis를 두 번 더 오간다
            }
            c.commit();
            return a;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private Admission redisScript(long post, String session) {
        Admission a;
        try {
            Long first = redis.eval(SCRIPT, ScriptOutputType.INTEGER,
                    new String[] {viewKey(post, session), dayKey()},
                    Long.toString(DEDUP_SECONDS), Long.toString(post), Long.toString(RANKING_SECONDS));
            a = first != null && first == 1 ? Admission.FIRST : Admission.REPEAT;
        } catch (RedisException e) {
            a = Admission.UNAVAILABLE;
        }
        if (a == Admission.FIRST) countInPostgres(post);
        return a;
    }

    Admission admit(long post, String session) {
        try {
            String ok = redis.set(viewKey(post, session), "1", SetArgs.Builder.nx().ex(DEDUP_SECONDS));
            return "OK".equals(ok) ? Admission.FIRST : Admission.REPEAT;
        } catch (RedisException e) {
            return Admission.UNAVAILABLE; // 세지 않는다
        }
    }

    private void rank(long post) {
        try {
            String key = dayKey();
            redis.zincrby(key, 1, Long.toString(post));
            redis.expire(key, RANKING_SECONDS);
        } catch (RedisException ignored) {
            // 순위는 근사치다. 놓쳐도 조회수 원본에는 영향이 없다.
        }
    }

    private void countInPostgres(long post) {
        try (Connection c = db.ds.getConnection()) {
            increment(c, post);
            c.commit();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void increment(Connection c, long post) throws SQLException {
        try (var ps = c.prepareStatement("UPDATE view_count SET count=count+1 WHERE content_id=?")) {
            ps.setLong(1, post);
            ps.executeUpdate();
        }
    }

    static String viewKey(long post, String session) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(session.getBytes(StandardCharsets.UTF_8));
            return "view:" + post + ":" + HexFormat.of().formatHex(hash);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static String dayKey() {
        return "popular:DEV:" + LocalDate.now(ZoneOffset.UTC);
    }

    @Override
    public void close() {
        connection.close();
        client.shutdown(Duration.ZERO, Duration.ofSeconds(2));
    }
}
