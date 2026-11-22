package playground.sagadur;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 업무 규칙(3편과 같음). 늘 켜 두는 것: 예약 행으로 같은 Saga의 예약을 한 번만 잡기, 처음 보는 Saga의 해제에 "취소됨" 표시 남기기,
 * 같은 Saga의 생성은 저장된 결과 돌려주기, 같은 Saga가 잡은 글에만 게시하기.
 * 실험용 스위치 naive는 사용량 확정의 "아직 예약 중일 때만" 조건을 뺀다.
 */
final class Domain {
    final boolean naive;
    final AtomicInteger providerCalls = new AtomicInteger();

    Domain(boolean naive) {
        this.naive = naive;
    }

    static final String USAGE_SCHEMA = """
            CREATE TABLE quota(author text PRIMARY KEY, lim bigint NOT NULL, used bigint NOT NULL DEFAULT 0,
              reserved bigint NOT NULL DEFAULT 0);
            CREATE TABLE reservation(saga uuid PRIMARY KEY, author text NOT NULL, amount bigint NOT NULL,
              status text NOT NULL, actual bigint);
            """;
    static final String CONTENT_SCHEMA = """
            CREATE TABLE post(id bigint PRIMARY KEY, title text NOT NULL, locked_by uuid, summary text, summary_saga uuid);
            """;
    static final String AI_SCHEMA = """
            CREATE TABLE job(saga uuid PRIMARY KEY, post bigint NOT NULL, status text NOT NULL, summary text, tokens bigint);
            """;

    boolean reserve(Connection c, UUID saga, String author, long amount) throws SQLException {
        if (update(c, "INSERT INTO reservation(saga, author, amount, status) VALUES (?,?,?,'PENDING') ON CONFLICT (saga) DO NOTHING",
                saga, author, amount) == 0) {
            String status = one(c, "SELECT status FROM reservation WHERE saga = ?", saga);
            return "RESERVED".equals(status) || "COMMITTED".equals(status);
        }
        int held = update(c, "UPDATE quota SET reserved = reserved + ? WHERE author = ? AND used + reserved + ? <= lim",
                amount, author, amount);
        update(c, "UPDATE reservation SET status = ? WHERE saga = ?", held == 1 ? "RESERVED" : "REJECTED", saga);
        return held == 1;
    }

    /** 예약을 돌려준다. 처음 보는 Saga면 "취소됨" 표시를 남겨 늦게 온 예약을 막는다(3편). */
    void release(Connection c, UUID saga) throws SQLException {
        String row = one(c, "UPDATE reservation SET status = 'RELEASED' WHERE saga = ? AND status = 'RESERVED'"
                + " RETURNING author || '|' || amount", saga);
        if (row != null) {
            String[] p = row.split("\\|");
            update(c, "UPDATE quota SET reserved = reserved - ? WHERE author = ?", Long.parseLong(p[1]), p[0]);
            return;
        }
        update(c, "INSERT INTO reservation(saga, author, amount, status) VALUES (?, '', 0, 'CANCELLED') ON CONFLICT (saga) DO NOTHING",
                saga);
    }

    /** 예약을 실제 사용량으로 확정한다. naive면 이미 확정된 예약도 다시 반영한다. */
    void commit(Connection c, UUID saga, long actual) throws SQLException {
        String from = naive ? "status IN ('RESERVED', 'COMMITTED')" : "status = 'RESERVED'";
        String row = one(c, "UPDATE reservation SET status = 'COMMITTED', actual = ? WHERE saga = ? AND " + from
                + " RETURNING author || '|' || amount", actual, saga);
        if (row != null) {
            String[] p = row.split("\\|");
            long amount = Long.parseLong(p[1]);
            // 두 번째 반영이면 이미 예약을 뺐으므로 사용량만 더한다(naive일 때만 생긴다).
            update(c, "UPDATE quota SET reserved = greatest(reserved - ?, 0), used = used + ? WHERE author = ?", amount, actual, p[0]);
        }
    }

    /** 예약이 이미 풀린 뒤에 확인된 사용량(시한 보상 뒤 늦게 끝난 생성)을 기록한다. 한 번만 반영한다. */
    void chargeLate(Connection c, UUID saga, String author, long actual) throws SQLException {
        if (update(c, "UPDATE reservation SET status = 'LATE', actual = ? WHERE saga = ? AND status IN ('RELEASED', 'CANCELLED')",
                actual, saga) == 1) {
            update(c, "UPDATE quota SET used = used + ? WHERE author = ?", actual, author);
        }
    }

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

    /** 게시한 요약을 내린다(이 Saga의 요약일 때만). */
    void detach(Connection c, UUID saga, long post) throws SQLException {
        update(c, "UPDATE post SET summary = NULL, summary_saga = NULL WHERE id = ? AND summary_saga = ?", post, saga);
    }

    record Generated(boolean ok, String summary, long tokens) {}

    Generated generate(Connection c, UUID saga, long post) throws SQLException {
        try (var ps = c.prepareStatement("SELECT status, summary, tokens FROM job WHERE saga = ?")) {
            ps.setObject(1, saga);
            try (var rs = ps.executeQuery()) {
                if (rs.next()) return new Generated("SUCCEEDED".equals(rs.getString(1)), rs.getString(2), rs.getLong(3));
            }
        }
        providerCalls.incrementAndGet();
        long tokens = 100 + post % 7 * 10;
        String summary = "글 " + post + " 요약";
        update(c, "INSERT INTO job(saga, post, status, summary, tokens) VALUES (?,?,'SUCCEEDED',?,?)", saga, post, summary, tokens);
        return new Generated(true, summary, tokens);
    }

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
