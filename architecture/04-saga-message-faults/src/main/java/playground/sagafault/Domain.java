package playground.sagafault;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 업무 규칙. 2편과 같지만 실험을 위해 두 가지 스위치가 있다.
 *
 * <ul>
 *   <li>naive: Usage의 해제·확정에서 "아직 예약 중일 때만"이라는 상태 조건을 뺀다. 같은 메시지가 두 번 처리되면
 *       두 번 반영된다. 상태를 보지 않고 숫자만 더하고 빼는 흔한 구현을 흉내 낸다.</li>
 *   <li>tombstone: 처음 보는 Saga의 예약을 해제하라는 요청이 오면 "취소됨" 표시를 남긴다. 뒤늦게 도착한 예약 요청은
 *       그 표시를 보고 예약하지 않는다.</li>
 * </ul>
 */
final class Domain {
    final boolean naive;
    final boolean tombstone;
    /** AI 공급자를 실제로 부른 횟수(같은 Saga의 재호출은 저장된 결과를 돌려주므로 세지 않는다). */
    final AtomicInteger providerCalls = new AtomicInteger();

    Domain(boolean naive, boolean tombstone) {
        this.naive = naive;
        this.tombstone = tombstone;
    }

    // ---------------------------------------------------------------- Usage
    static final String USAGE_SCHEMA = """
            CREATE TABLE quota(author text PRIMARY KEY, lim bigint NOT NULL, used bigint NOT NULL DEFAULT 0,
              reserved bigint NOT NULL DEFAULT 0);
            CREATE TABLE reservation(saga uuid PRIMARY KEY, author text NOT NULL, amount bigint NOT NULL,
              status text NOT NULL, actual bigint);
            """;

    /**
     * 한도 안이면 예약하고 true. 이미 이 Saga의 행이 있으면 새로 잡지 않고, 지금 잡혀 있거나 확정됐으면 true다.
     * 취소 표시(CANCELLED)나 해제(RELEASED)된 Saga는 다시 잡지 않는다.
     */
    boolean reserve(Connection c, UUID saga, String author, long amount) throws SQLException {
        try (var ps = c.prepareStatement("""
                INSERT INTO reservation(saga, author, amount, status) VALUES (?,?,?,'PENDING')
                ON CONFLICT (saga) DO NOTHING""")) {
            ps.setObject(1, saga);
            ps.setString(2, author);
            ps.setLong(3, amount);
            if (ps.executeUpdate() == 0) {
                String status = one(c, "SELECT status FROM reservation WHERE saga = ?", saga);
                return "RESERVED".equals(status) || "COMMITTED".equals(status);
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

    /** 예약을 돌려준다. naive가 아니면 RESERVED일 때만 돌려준다. */
    void release(Connection c, UUID saga) throws SQLException {
        String from = naive ? "status IN ('RESERVED', 'RELEASED')" : "status = 'RESERVED'";
        try (var ps = c.prepareStatement("UPDATE reservation SET status = 'RELEASED' WHERE saga = ? AND " + from
                + " RETURNING author, amount")) {
            ps.setObject(1, saga);
            try (var rs = ps.executeQuery()) {
                if (rs.next()) {
                    update(c, "UPDATE quota SET reserved = reserved - ? WHERE author = ?", rs.getLong(2), rs.getString(1));
                    return;
                }
            }
        }
        if (tombstone) {
            // 아직 예약 요청이 도착하지 않은 Saga다. 나중에 도착해도 잡지 않도록 표시만 남긴다.
            update(c, """
                    INSERT INTO reservation(saga, author, amount, status) VALUES (?, '', 0, 'CANCELLED')
                    ON CONFLICT (saga) DO NOTHING""", saga);
        }
    }

    /** 예약을 실제 사용량으로 바꾼다. naive가 아니면 RESERVED일 때만 반영한다. */
    void commit(Connection c, UUID saga, long actual) throws SQLException {
        String from = naive ? "status IN ('RESERVED', 'COMMITTED')" : "status = 'RESERVED'";
        try (var ps = c.prepareStatement("UPDATE reservation SET status = 'COMMITTED', actual = ? WHERE saga = ? AND "
                + from + " RETURNING author, amount")) {
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

    // ---------------------------------------------------------------- Content
    static final String CONTENT_SCHEMA = """
            CREATE TABLE post(id bigint PRIMARY KEY, title text NOT NULL, locked_by uuid, summary text, summary_saga uuid);
            """;

    enum Lock { LOCKED, MISSING, BUSY }

    Lock lock(Connection c, UUID saga, long post) throws SQLException {
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

    void unlock(Connection c, UUID saga, long post) throws SQLException {
        update(c, "UPDATE post SET locked_by = NULL WHERE id = ? AND locked_by = ?", post, saga);
    }

    void attach(Connection c, UUID saga, long post, String summary) throws SQLException {
        update(c, "UPDATE post SET summary = ?, summary_saga = ?, locked_by = NULL WHERE id = ? AND locked_by = ?",
                summary, saga, post, saga);
    }

    // ---------------------------------------------------------------- AI
    static final String AI_SCHEMA = """
            CREATE TABLE job(saga uuid PRIMARY KEY, post bigint NOT NULL, status text NOT NULL, summary text, tokens bigint);
            """;

    record Generated(boolean ok, String summary, long tokens) {}

    /** 같은 Saga로 다시 불리면 저장된 결과를 돌려준다(공급자를 다시 부르지 않는다). */
    Generated generate(Connection c, UUID saga, long post, Set<Long> failing) throws SQLException {
        try (var ps = c.prepareStatement("SELECT status, summary, tokens FROM job WHERE saga = ?")) {
            ps.setObject(1, saga);
            try (var rs = ps.executeQuery()) {
                if (rs.next()) return new Generated("SUCCEEDED".equals(rs.getString(1)), rs.getString(2), rs.getLong(3));
            }
        }
        providerCalls.incrementAndGet();
        boolean ok = !failing.contains(post);
        String summary = ok ? "글 " + post + " 요약" : null;
        long tokens = ok ? 100 + post % 7 * 10 : 0;
        update(c, "INSERT INTO job(saga, post, status, summary, tokens) VALUES (?,?,?,?,?)",
                saga, post, ok ? "SUCCEEDED" : "FAILED", summary, tokens);
        return new Generated(ok, summary, tokens);
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
