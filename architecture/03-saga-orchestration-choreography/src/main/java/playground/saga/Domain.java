package playground.saga;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.UUID;

/**
 * 네 서비스의 업무 규칙. 오케스트레이션과 코레오그래피가 같은 함수를 쓴다. 바뀌는 것은 "누가 언제 부르나"뿐이다.
 * 모든 함수는 호출자의 트랜잭션 안에서 실행되고, 같은 Saga ID로 다시 불려도 한 번만 반영된다.
 */
final class Domain {
    private Domain() {}

    // ---------------------------------------------------------------- Usage: 저자별 토큰 한도
    static final String USAGE_SCHEMA = """
            CREATE TABLE quota(author text PRIMARY KEY, lim bigint NOT NULL, used bigint NOT NULL DEFAULT 0,
              reserved bigint NOT NULL DEFAULT 0);
            CREATE TABLE reservation(saga uuid PRIMARY KEY, author text NOT NULL, amount bigint NOT NULL,
              status text NOT NULL, actual bigint);
            """;

    /** 한도 안이면 예약하고 true. 같은 Saga의 두 번째 호출은 처음 결과를 그대로 돌려준다. */
    static boolean reserve(Connection c, UUID saga, String author, long amount) throws SQLException {
        try (var ps = c.prepareStatement("""
                INSERT INTO reservation(saga, author, amount, status) VALUES (?,?,?,'PENDING')
                ON CONFLICT (saga) DO NOTHING""")) {
            ps.setObject(1, saga);
            ps.setString(2, author);
            ps.setLong(3, amount);
            if (ps.executeUpdate() == 0) {
                return !"REJECTED".equals(one(c, "SELECT status FROM reservation WHERE saga = ?", saga));
            }
        }
        int held;
        try (var ps = c.prepareStatement("""
                UPDATE quota SET reserved = reserved + ? WHERE author = ? AND used + reserved + ? <= lim""")) {
            ps.setLong(1, amount);
            ps.setString(2, author);
            ps.setLong(3, amount);
            held = ps.executeUpdate();
        }
        update(c, "UPDATE reservation SET status = ? WHERE saga = ?", held == 1 ? "RESERVED" : "REJECTED", saga);
        return held == 1;
    }

    /** 예약을 돌려준다. 이미 정산된 예약이면 아무것도 바꾸지 않는다. */
    static void release(Connection c, UUID saga) throws SQLException {
        try (var ps = c.prepareStatement("""
                UPDATE reservation SET status = 'RELEASED' WHERE saga = ? AND status = 'RESERVED'
                RETURNING author, amount""")) {
            ps.setObject(1, saga);
            try (var rs = ps.executeQuery()) {
                if (rs.next()) {
                    update(c, "UPDATE quota SET reserved = reserved - ? WHERE author = ?", rs.getLong(2), rs.getString(1));
                }
            }
        }
    }

    /** 예약을 실제 사용량으로 바꾼다. */
    static void commit(Connection c, UUID saga, long actual) throws SQLException {
        try (var ps = c.prepareStatement("""
                UPDATE reservation SET status = 'COMMITTED', actual = ? WHERE saga = ? AND status = 'RESERVED'
                RETURNING author, amount""")) {
            ps.setLong(1, actual);
            ps.setObject(2, saga);
            try (var rs = ps.executeQuery()) {
                if (rs.next()) {
                    update(c, "UPDATE quota SET reserved = reserved - ?, used = used + ? WHERE author = ?",
                            rs.getLong(2), actual, rs.getString(1));
                }
            }
        }
    }

    // ---------------------------------------------------------------- Content: 글과 요약
    static final String CONTENT_SCHEMA = """
            CREATE TABLE post(id bigint PRIMARY KEY, title text NOT NULL, locked_by uuid, summary text, summary_saga uuid);
            """;

    enum Lock { LOCKED, MISSING, BUSY }

    /** 요약을 붙이는 동안 다른 Saga가 같은 글을 건드리지 못하게 표시한다. */
    static Lock lock(Connection c, UUID saga, long post) throws SQLException {
        try (var ps = c.prepareStatement("SELECT locked_by FROM post WHERE id = ? FOR UPDATE")) {
            ps.setLong(1, post);
            try (var rs = ps.executeQuery()) {
                if (!rs.next()) return Lock.MISSING;
                Object holder = rs.getObject(1);
                if (holder != null) return holder.equals(saga) ? Lock.LOCKED : Lock.BUSY;
            }
        }
        update(c, "UPDATE post SET locked_by = ? WHERE id = ?", saga, post);
        return Lock.LOCKED;
    }

    static void unlock(Connection c, UUID saga, long post) throws SQLException {
        update(c, "UPDATE post SET locked_by = NULL WHERE id = ? AND locked_by = ?", post, saga);
    }

    /** 이 Saga가 잡은 글에만 요약을 붙이고 잠금을 푼다. 다시 불리면 0행이 바뀐다. */
    static void attach(Connection c, UUID saga, long post, String summary) throws SQLException {
        update(c, "UPDATE post SET summary = ?, summary_saga = ?, locked_by = NULL WHERE id = ? AND locked_by = ?",
                summary, saga, post, saga);
    }

    // ---------------------------------------------------------------- AI: 유료 생성 호출(흉내)
    static final String AI_SCHEMA = """
            CREATE TABLE job(saga uuid PRIMARY KEY, post bigint NOT NULL, status text NOT NULL, summary text, tokens bigint);
            """;

    record Generated(boolean ok, String summary, long tokens) {}

    /**
     * 요약을 만든다. failing에 든 글은 공급자 오류로, banned에 든 글은 금지어가 섞인 요약으로 끝난다.
     * 같은 Saga로 다시 불리면 저장된 결과를 돌려준다(다시 과금하지 않는다).
     */
    static Generated generate(Connection c, UUID saga, long post, Set<Long> failing, Set<Long> banned)
            throws SQLException {
        try (var ps = c.prepareStatement("SELECT status, summary, tokens FROM job WHERE saga = ?")) {
            ps.setObject(1, saga);
            try (var rs = ps.executeQuery()) {
                if (rs.next()) return new Generated("SUCCEEDED".equals(rs.getString(1)), rs.getString(2), rs.getLong(3));
            }
        }
        boolean ok = !failing.contains(post);
        String summary = ok ? (banned.contains(post) ? "글 " + post + " 요약(금지어 포함)" : "글 " + post + " 요약") : null;
        long tokens = ok ? 100 + post % 7 * 10 : 0;
        update(c, "INSERT INTO job(saga, post, status, summary, tokens) VALUES (?,?,?,?,?)",
                saga, post, ok ? "SUCCEEDED" : "FAILED", summary, tokens);
        return new Generated(ok, summary, tokens);
    }

    // ---------------------------------------------------------------- Moderation: 요약 검수(실험 5에서 추가하는 단계)
    static final String MODERATION_SCHEMA = """
            CREATE TABLE review(saga uuid PRIMARY KEY, approved boolean NOT NULL);
            """;

    static boolean review(Connection c, UUID saga, String summary) throws SQLException {
        String stored = one(c, "SELECT approved::text FROM review WHERE saga = ?", saga);
        if (stored != null) return Boolean.parseBoolean(stored);
        boolean approved = !summary.contains("금지어");
        update(c, "INSERT INTO review(saga, approved) VALUES (?,?)", saga, approved);
        return approved;
    }

    // ---------------------------------------------------------------- helpers
    static int update(Connection c, String sql, Object... args) throws SQLException {
        try (var ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) ps.setObject(i + 1, args[i]);
            return ps.executeUpdate();
        }
    }

    static String one(Connection c, String sql, Object... args) throws SQLException {
        try (var ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) ps.setObject(i + 1, args[i]);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }
}
