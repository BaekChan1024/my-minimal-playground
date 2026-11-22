package playground.sagaiso;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * "AI 요약 게시" Saga 하나. 단계마다 Usage 또는 Content database에서 자기 트랜잭션으로 커밋한다.
 * step()을 부를 때마다 한 단계씩 진행하므로, 실험이 두 Saga의 단계를 원하는 순서로 섞을 수 있다.
 * 한 단계가 실패하면 그때까지 쌓인 보상을 거꾸로 실행하고 끝난다.
 *
 * 기본 순서: 한도 예약 → 글 읽기(잠금) → 요약 생성 → 요약 게시 → 검수 → 사용량 확정
 * 생성 전 실패는 예약을 돌려주고, 생성 뒤 실패는 쓴 토큰을 사용량으로 확정한다.
 */
final class SummarySaga {
    /**
     * 대응책 스위치.
     *
     * @param reorder    검수를 게시보다 앞에 둔다(되돌릴 수 있는 단계를 앞으로: pessimistic view)
     * @param hidden     게시한 요약을 Saga가 끝날 때까지 감춘다(보이는 상태에 대한 semantic lock)
     * @param postLock   글에 "이 Saga가 작업 중" 표시를 남기고, 다른 Saga와 글 수정이 그동안 들어오지 못하게 한다(semantic lock)
     * @param casUndo    게시를 되돌릴 때 요약이 아직 내 것일 때만 되돌린다(compare-and-set)
     * @param reread     게시할 때 글 버전이 읽을 때와 같은지 다시 확인한다(reread value)
     * @param reserve    한도를 시작할 때 예약한다. false면 시작할 때 잔량만 확인하고 끝날 때 더한다
     */
    record Options(boolean reorder, boolean hidden, boolean postLock, boolean casUndo, boolean reread, boolean reserve) {
        static Options naive() {
            return new Options(false, false, false, false, false, true);
        }

        Options withReorder() {
            return new Options(true, hidden, postLock, casUndo, reread, reserve);
        }

        Options withHidden() {
            return new Options(reorder, true, postLock, casUndo, reread, reserve);
        }

        Options withPostLock() {
            return new Options(reorder, hidden, true, casUndo, reread, reserve);
        }

        Options withCasUndo() {
            return new Options(reorder, hidden, postLock, true, reread, reserve);
        }

        Options withReread() {
            return new Options(reorder, hidden, postLock, casUndo, true, reserve);
        }

        Options optimisticQuota() {
            return new Options(reorder, hidden, postLock, casUndo, reread, false);
        }
    }

    enum Step { RESERVE, READ, GENERATE, ATTACH, MODERATE, COMMIT }

    enum Outcome { RUNNING, COMPLETED, COMPENSATED, REJECTED }

    static final class Fail extends Exception {
        Fail(String reason) {
            super(reason);
        }
    }

    private interface Undo {
        void run() throws SQLException;
    }

    final String id;
    final String author;
    final long post;
    final long tokens;
    final boolean flagged;
    /** 요약 생성(공급자 호출)이 실패한다. 토큰을 쓰기 전의 실패라 예약을 돌려준다. */
    final boolean failGeneration;
    /** 요약 생성이 끝나 토큰을 이미 썼다. 이 뒤의 실패는 예약을 돌려주지 않고 사용량으로 확정한다(2편과 같은 규칙). */
    private boolean spent;
    private final Store.World w;
    private final Options o;
    private final List<Step> steps;
    private final Deque<Undo> undo = new ArrayDeque<>();
    private int next;
    Outcome outcome = Outcome.RUNNING;
    String reason;
    int readVersion;
    String summary;
    final List<String> log = new ArrayList<>();

    SummarySaga(Store.World w, Options o, String id, String author, long post, long tokens, boolean flagged) {
        this(w, o, id, author, post, tokens, flagged, false);
    }

    SummarySaga(Store.World w, Options o, String id, String author, long post, long tokens, boolean flagged,
            boolean failGeneration) {
        this.failGeneration = failGeneration;
        this.w = w;
        this.o = o;
        this.id = id;
        this.author = author;
        this.post = post;
        this.tokens = tokens;
        this.flagged = flagged;
        steps = o.reorder()
                ? List.of(Step.RESERVE, Step.READ, Step.GENERATE, Step.MODERATE, Step.ATTACH, Step.COMMIT)
                : List.of(Step.RESERVE, Step.READ, Step.GENERATE, Step.ATTACH, Step.MODERATE, Step.COMMIT);
    }

    boolean done() {
        return outcome != Outcome.RUNNING;
    }

    Step peek() {
        return done() ? null : steps.get(next);
    }

    /** 다음 단계 하나. 실패하면 보상까지 마친다. 끝났으면 false. */
    boolean step() throws SQLException {
        if (done()) return false;
        Step s = steps.get(next++);
        try {
            run(s);
            log.add(s.name());
            if (next == steps.size()) outcome = Outcome.COMPLETED;
        } catch (Fail f) {
            reason = f.getMessage();
            log.add(s.name() + "!" + reason);
            outcome = undo.isEmpty() ? Outcome.REJECTED : Outcome.COMPENSATED;
            while (!undo.isEmpty()) undo.pop().run();
        }
        return !done();
    }

    void runToEnd() throws SQLException {
        while (step()) {
            // 한 단계씩
        }
    }

    /** s 단계 직전까지 진행한다. */
    void runUntil(Step s) throws SQLException {
        while (!done() && peek() != s) step();
    }

    private void run(Step s) throws SQLException, Fail {
        switch (s) {
            case RESERVE -> reserve();
            case READ -> read();
            case GENERATE -> {
                if (failGeneration) throw new Fail("GENERATION_FAILED");
                summary = "v" + readVersion + " 기준 요약(" + id + ")";
                spent = true;
            }
            case ATTACH -> attach();
            case MODERATE -> {
                if (flagged) throw new Fail("MODERATION_REJECTED");
            }
            case COMMIT -> commit();
        }
    }

    private void reserve() throws SQLException, Fail {
        if (!o.reserve()) {
            // 낙관적: 지금 남은 양만 보고 지나간다. 아무것도 잡지 않는다.
            String room = w.usage(c -> Store.one(c, "SELECT lim - used FROM quota WHERE author = ?", author));
            if (Long.parseLong(room) < tokens) throw new Fail("QUOTA");
            // 생성 뒤에 실패하면 쓴 토큰을 사용량에 더한다. 생성 전 실패면 아무것도 하지 않는다.
            undo.push(() -> {
                if (spent) w.usage(c -> Store.update(c, "UPDATE quota SET used = used + ? WHERE author = ?", tokens, author));
            });
            return;
        }
        boolean held = w.usage(c -> {
            Store.update(c, "INSERT INTO reservation(saga, author, amount, status) VALUES (?,?,?,'RESERVED')", id, author, tokens);
            if (Store.update(c, "UPDATE quota SET reserved = reserved + ? WHERE author = ? AND used + reserved + ? <= lim",
                    tokens, author, tokens) == 1) return true;
            Store.update(c, "UPDATE reservation SET status = 'REJECTED' WHERE saga = ?", id);
            return false;
        });
        if (!held) throw new Fail("QUOTA");
        undo.push(() -> w.usage(c -> {
            if (spent) {
                // 토큰을 이미 썼다: 예약을 사용량으로 확정한다.
                String amount = Store.one(c, "UPDATE reservation SET status = 'COMMITTED', actual = ? WHERE saga = ?"
                        + " AND status = 'RESERVED' RETURNING amount", tokens, id);
                if (amount != null) Store.update(c, "UPDATE quota SET reserved = reserved - ?, used = used + ? WHERE author = ?",
                        Long.parseLong(amount), tokens, author);
                return null;
            }
            String amount = Store.one(c,
                    "UPDATE reservation SET status = 'RELEASED' WHERE saga = ? AND status = 'RESERVED' RETURNING amount", id);
            if (amount != null) Store.update(c, "UPDATE quota SET reserved = reserved - ? WHERE author = ?", Long.parseLong(amount), author);
            return null;
        }));
    }

    private void read() throws SQLException, Fail {
        String busy = w.content(c -> {
            String holder = Store.one(c, "SELECT coalesce(locked_by, '') FROM post WHERE id = ? FOR UPDATE", post);
            if (o.postLock()) {
                if (!holder.isEmpty() && !holder.equals(id)) return holder;
                Store.update(c, "UPDATE post SET locked_by = ? WHERE id = ?", id, post);
            }
            readVersion = Integer.parseInt(Store.one(c, "SELECT version FROM post WHERE id = ?", post));
            return null;
        });
        if (busy != null) throw new Fail("BUSY");
        if (o.postLock()) {
            undo.push(() -> w.content(c -> Store.update(c, "UPDATE post SET locked_by = NULL WHERE id = ? AND locked_by = ?", post, id)));
        }
    }

    private void attach() throws SQLException, Fail {
        record Prev(String summary, String saga, Integer version, boolean visible) {}
        var prev = new Prev[1];
        int changed = w.content(c -> {
            try (var ps = c.prepareStatement(
                    "SELECT summary, summary_saga, summary_version, summary_visible FROM post WHERE id = ? FOR UPDATE")) {
                ps.setLong(1, post);
                try (var rs = ps.executeQuery()) {
                    rs.next();
                    prev[0] = new Prev(rs.getString(1), rs.getString(2), (Integer) rs.getObject(3), rs.getBoolean(4));
                }
            }
            String where = "WHERE id = ?" + (o.reread() ? " AND version = " + readVersion : "")
                    + (o.postLock() ? " AND locked_by = '" + id + "'" : "");
            return Store.update(c, "UPDATE post SET summary = ?, summary_saga = ?, summary_version = ?, summary_visible = ? " + where,
                    summary, id, readVersion, !o.hidden(), post);
        });
        if (changed == 0) throw new Fail(o.reread() ? "STALE" : "LOCK_LOST");
        Prev p = prev[0];
        undo.push(() -> w.content(c -> Store.update(c,
                "UPDATE post SET summary = ?, summary_saga = ?, summary_version = ?, summary_visible = ? WHERE id = ?"
                        + (o.casUndo() ? " AND summary_saga = '" + id + "'" : ""),
                p.summary(), p.saga(), p.version(), p.visible(), post)));
    }

    private void commit() throws SQLException {
        w.usage(c -> {
            if (o.reserve()) {
                String amount = Store.one(c,
                        "UPDATE reservation SET status = 'COMMITTED', actual = ? WHERE saga = ? AND status = 'RESERVED' RETURNING amount",
                        tokens, id);
                Store.update(c, "UPDATE quota SET reserved = reserved - ?, used = used + ? WHERE author = ?",
                        Long.parseLong(amount), tokens, author);
            } else {
                Store.update(c, "UPDATE quota SET used = used + ? WHERE author = ?", tokens, author);
            }
            return null;
        });
        w.content(c -> {
            if (o.hidden()) Store.update(c, "UPDATE post SET summary_visible = true WHERE id = ? AND summary_saga = ?", post, id);
            if (o.postLock()) Store.update(c, "UPDATE post SET locked_by = NULL WHERE id = ? AND locked_by = ?", post, id);
            return null;
        });
    }
}
