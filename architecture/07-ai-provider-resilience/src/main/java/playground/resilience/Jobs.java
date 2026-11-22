package playground.resilience;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * AI 작업 대기열. 블로그 Content 서비스의 ai_job을 줄인 것이다.
 * 작업은 QUEUED → RUNNING(claim) → SUCCEEDED/FAILED, 또는 다시 시도할 실패면 QUEUED로 돌아가며 retries를 1 올리고
 * next_attempt_at까지 기다린다. 기다림은 스레드가 아니라 이 행에 적힌다.
 */
final class Jobs implements AutoCloseable {
    interface Work<T> {
        T run(Connection c) throws SQLException;
    }

    record Claim(int id, String kind, int attempts, int retries) {
        boolean system() {
            return kind.equals("SYSTEM");
        }
    }

    /** 임시 PostgreSQL 하나. 실험(변형)마다 새 database를 만든다. */
    static final class Server implements AutoCloseable {
        private final EmbeddedPostgres pg;
        private int seq;

        Server() throws Exception {
            pg = EmbeddedPostgres.builder()
                    .setServerConfig("listen_addresses", "127.0.0.1")
                    .setServerConfig("max_connections", "100")
                    .setPort(0)
                    .start();
        }

        Jobs fresh() throws SQLException {
            String name = "jobs" + (++seq);
            try (var c = pg.getPostgresDatabase().getConnection(); var s = c.createStatement()) {
                s.execute("CREATE DATABASE " + name);
            }
            var config = new HikariConfig();
            config.setJdbcUrl(pg.getJdbcUrl("postgres", name));
            config.setMaximumPoolSize(8);
            config.setPoolName(name);
            return new Jobs(new HikariDataSource(config));
        }

        @Override
        public void close() throws Exception {
            pg.close();
        }
    }

    private final HikariDataSource ds;

    private Jobs(HikariDataSource ds) throws SQLException {
        this.ds = ds;
        tx(c -> {
            try (var s = c.createStatement()) {
                s.execute("""
                        CREATE TABLE job(
                          id int PRIMARY KEY,
                          kind text NOT NULL CHECK (kind IN ('AUTHOR','SYSTEM')),
                          status text NOT NULL DEFAULT 'QUEUED',
                          attempts int NOT NULL DEFAULT 0,
                          retries int NOT NULL DEFAULT 0,
                          next_attempt_at timestamptz,
                          created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                          started_at timestamptz,
                          finished_at timestamptz,
                          error_code text)""");
            }
            return null;
        });
    }

    <T> T tx(Work<T> work) throws SQLException {
        try (var c = ds.getConnection()) {
            c.setAutoCommit(false);
            try {
                T result = work.run(c);
                c.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                c.rollback();
                throw e;
            }
        }
    }

    void submit(int id, String kind) throws SQLException {
        tx(c -> {
            try (var ps = c.prepareStatement("INSERT INTO job(id, kind) VALUES (?, ?)")) {
                ps.setInt(1, id);
                ps.setString(2, kind);
                ps.executeUpdate();
            }
            return null;
        });
    }

    /**
     * 기다림이 끝난 가장 오래된 작업 하나를 집는다. SKIP LOCKED라 여러 스레드가 서로 기다리지 않는다.
     * includeSystem=false면 시스템 작업(자동 요약)은 건너뛴다.
     */
    Optional<Claim> claim(boolean includeSystem) throws SQLException {
        return tx(c -> {
            try (var ps = c.prepareStatement("""
                    UPDATE job SET status = 'RUNNING', attempts = attempts + 1, next_attempt_at = NULL,
                           started_at = coalesce(started_at, clock_timestamp())
                     WHERE id = (SELECT id FROM job
                                  WHERE status = 'QUEUED'
                                    AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp())
                                    AND (? OR kind <> 'SYSTEM')
                                  ORDER BY created_at, id
                                  LIMIT 1
                                  FOR UPDATE SKIP LOCKED)
                    RETURNING id, kind, attempts, retries""")) {
                ps.setBoolean(1, includeSystem);
                try (var rs = ps.executeQuery()) {
                    return rs.next()
                            ? Optional.of(new Claim(rs.getInt(1), rs.getString(2), rs.getInt(3), rs.getInt(4)))
                            : Optional.<Claim>empty();
                }
            }
        });
    }

    void succeed(Claim claim) throws SQLException {
        end(claim, "SUCCEEDED", null);
    }

    void fail(Claim claim, String code) throws SQLException {
        end(claim, "FAILED", code);
    }

    private void end(Claim claim, String status, String code) throws SQLException {
        tx(c -> {
            try (var ps = c.prepareStatement("""
                    UPDATE job SET status = ?, error_code = ?, finished_at = clock_timestamp()
                     WHERE id = ? AND status = 'RUNNING' AND attempts = ?""")) {
                ps.setString(1, status);
                ps.setString(2, code);
                ps.setInt(3, claim.id());
                ps.setInt(4, claim.attempts());
                ps.executeUpdate();
            }
            return null;
        });
    }

    /** 다시 대기열로. 같은 claim(attempts)일 때만. */
    void retryLater(Claim claim, long delayMs) throws SQLException {
        tx(c -> {
            try (var ps = c.prepareStatement("""
                    UPDATE job SET status = 'QUEUED', retries = retries + 1,
                           next_attempt_at = clock_timestamp() + make_interval(secs => ?)
                     WHERE id = ? AND status = 'RUNNING' AND attempts = ?""")) {
                ps.setDouble(1, delayMs / 1000.0);
                ps.setInt(2, claim.id());
                ps.setInt(3, claim.attempts());
                ps.executeUpdate();
            }
            return null;
        });
    }

    /** 끝나지 않은 작업 수(QUEUED·RUNNING). */
    long unfinished() throws SQLException {
        return tx(c -> {
            try (var s = c.createStatement();
                    var rs = s.executeQuery("SELECT count(*) FROM job WHERE status IN ('QUEUED','RUNNING')")) {
                rs.next();
                return rs.getLong(1);
            }
        });
    }

    /** 상태별 수. 실패는 "FAILED:코드"로 나눈다. */
    Map<String, Long> outcomes() throws SQLException {
        return tx(c -> {
            var out = new LinkedHashMap<String, Long>();
            try (var s = c.createStatement();
                    var rs = s.executeQuery("""
                            SELECT CASE WHEN status = 'FAILED' THEN 'FAILED:' || error_code ELSE status END, count(*)
                              FROM job GROUP BY 1 ORDER BY 1""")) {
                while (rs.next()) out.put(rs.getString(1), rs.getLong(2));
            }
            return out;
        });
    }

    long sumRetries() throws SQLException {
        return tx(c -> {
            try (var s = c.createStatement(); var rs = s.executeQuery("SELECT coalesce(sum(retries), 0) FROM job")) {
                rs.next();
                return rs.getLong(1);
            }
        });
    }

    /** 요청부터 처음 시작까지 기다린 시간(ms). */
    long waitMs(int id) throws SQLException {
        return tx(c -> {
            try (var ps = c.prepareStatement(
                    "SELECT (extract(epoch FROM started_at - created_at) * 1000)::bigint FROM job WHERE id = ?")) {
                ps.setInt(1, id);
                try (var rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        });
    }

    /** 한 종류의 작업이 처음 요청된 때부터 마지막으로 끝난 때까지(ms). */
    long spanMs(String kind) throws SQLException {
        return tx(c -> {
            try (var ps = c.prepareStatement("""
                    SELECT (extract(epoch FROM max(finished_at) - min(created_at)) * 1000)::bigint
                      FROM job WHERE kind = ?""")) {
                ps.setString(1, kind);
                try (var rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        });
    }

    @Override
    public void close() {
        ds.close();
    }
}
