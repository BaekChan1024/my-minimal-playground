package playground.sagafault;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 중앙 조율(2편과 같은 전이표, 검수 단계는 뺐다).
 * guard가 꺼지면 응답이 어느 상태에 도착하든 전이를 실행한다.
 * deadlineMs가 있으면 RESERVING에 그 시간 넘게 머문 Saga를 취소한다(RELEASING으로 가서 ReleaseQuota를 보낸다).
 */
final class Orchestration extends World {
    final Node orchestrator;
    private final String usageCmd = topic("usage.commands");
    private final String contentCmd = topic("content.commands");
    private final String aiCmd = topic("ai.commands");
    private final String replies = topic("saga.replies");
    private volatile boolean running = true;
    private Thread scanner;

    Orchestration(Infra infra, String prefix, Options options) throws Exception {
        super(infra, prefix, "orchestration", options);
        infra.topics(List.of(usageCmd, contentCmd, aiCmd, replies));
        orchestrator = node("orchestrator", """
                CREATE TABLE saga(id uuid PRIMARY KEY, author text NOT NULL, post bigint NOT NULL, tokens bigint NOT NULL,
                  state text NOT NULL, reason text, summary text, actual bigint,
                  started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                  updated_at timestamptz NOT NULL DEFAULT clock_timestamp());
                """);
        participants();
        steps();
    }

    @Override
    List<Node> nodes() {
        return new ArrayList<>(List.of(orchestrator, usage, content, ai));
    }

    @Override
    void startAll() {
        super.startAll();
        if (options.deadlineMs() > 0) scanner = Thread.ofPlatform().name(prefix + "-deadline").daemon().start(this::scan);
    }

    @Override
    UUID start(String author, long post, long tokens) throws SQLException {
        UUID id = UUID.randomUUID();
        orchestrator.tx((c, out) -> {
            Domain.update(c, "INSERT INTO saga(id, author, post, tokens, state) VALUES (?,?,?,?, 'RESERVING')",
                    id, author, post, tokens);
            out.send(usageCmd, Msg.of(id, "ReserveQuota", "author", author, "tokens", tokens));
            return null;
        });
        return id;
    }

    @Override
    String state(UUID saga) throws SQLException {
        return orchestrator.text("SELECT state FROM saga WHERE id = ?", saga);
    }

    /** 시한: 한도 예약 응답을 deadlineMs 넘게 기다린 Saga를 취소하고 예약 해제를 보낸다. */
    private void scan() {
        while (running) {
            try {
                orchestrator.tx((c, out) -> {
                    try (var ps = c.prepareStatement("""
                            UPDATE saga SET state = 'RELEASING', reason = 'TIMEOUT', updated_at = clock_timestamp()
                             WHERE state = 'RESERVING' AND updated_at < clock_timestamp() - make_interval(secs => ?)
                            RETURNING id""")) {
                        ps.setDouble(1, options.deadlineMs() / 1000.0);
                        try (var rs = ps.executeQuery()) {
                            while (rs.next()) out.send(usageCmd, Msg.of((UUID) rs.getObject(1), "ReleaseQuota"));
                        }
                    }
                    return null;
                });
                Thread.sleep(20);
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                if (running) System.err.println("deadline: " + e);
            }
        }
    }

    private void participants() {
        usage.on(usageCmd, "ReserveQuota", (m, c, out) -> out.send(replies, Msg.of(m.saga(),
                        rules.reserve(c, m.saga(), m.str("author"), m.num("tokens")) ? "QuotaReserved" : "QuotaRejected")))
                .on(usageCmd, "ReleaseQuota", (m, c, out) -> {
                    rules.release(c, m.saga());
                    out.send(replies, Msg.of(m.saga(), "QuotaReleased"));
                })
                .on(usageCmd, "CommitQuota", (m, c, out) -> {
                    rules.commit(c, m.saga(), m.num("actual"));
                    out.send(replies, Msg.of(m.saga(), "QuotaCommitted"));
                });
        content.on(contentCmd, "LockPost", (m, c, out) -> {
                    var lock = rules.lock(c, m.saga(), m.num("post"));
                    out.send(replies, lock == Domain.Lock.LOCKED ? Msg.of(m.saga(), "PostLocked")
                            : Msg.of(m.saga(), "PostLockFailed", "reason", lock.name()));
                })
                .on(contentCmd, "UnlockPost", (m, c, out) -> {
                    rules.unlock(c, m.saga(), m.num("post"));
                    out.send(replies, Msg.of(m.saga(), "PostUnlocked"));
                })
                .on(contentCmd, "AttachSummary", (m, c, out) -> {
                    rules.attach(c, m.saga(), m.num("post"), m.str("summary"));
                    out.send(replies, Msg.of(m.saga(), "SummaryAttached"));
                });
        ai.on(aiCmd, "Generate", (m, c, out) -> {
            var g = rules.generate(c, m.saga(), m.num("post"), failing);
            out.send(replies, g.ok() ? Msg.of(m.saga(), "Generated", "summary", g.summary(), "tokens", g.tokens())
                    : Msg.of(m.saga(), "GenerationFailed"));
        });
    }

    private void steps() {
        reply("QuotaReserved", "RESERVING", (s, c, out) -> {
            out.send(contentCmd, Msg.of(s.id, "LockPost", "post", s.post));
            return next("LOCKING");
        });
        reply("QuotaRejected", "RESERVING", (s, c, out) -> next("REJECTED"));
        reply("PostLocked", "LOCKING", (s, c, out) -> {
            out.send(aiCmd, Msg.of(s.id, "Generate", "post", s.post));
            return next("GENERATING");
        });
        reply("PostLockFailed", "LOCKING", (s, c, out) -> {
            out.send(usageCmd, Msg.of(s.id, "ReleaseQuota"));
            return next("RELEASING").reason("LOCK_" + s.msg.str("reason"));
        });
        reply("Generated", "GENERATING", (s, c, out) -> {
            out.send(contentCmd, Msg.of(s.id, "AttachSummary", "post", s.post, "summary", s.msg.str("summary")));
            return next("ATTACHING").summary(s.msg.str("summary")).actual(s.msg.num("tokens"));
        });
        reply("GenerationFailed", "GENERATING", (s, c, out) -> {
            out.send(contentCmd, Msg.of(s.id, "UnlockPost", "post", s.post));
            return next("UNLOCKING").reason("GENERATION_FAILED");
        });
        reply("PostUnlocked", "UNLOCKING", (s, c, out) -> {
            out.send(usageCmd, Msg.of(s.id, "ReleaseQuota"));
            return next("RELEASING");
        });
        reply("SummaryAttached", "ATTACHING", (s, c, out) -> {
            out.send(usageCmd, Msg.of(s.id, "CommitQuota", "actual", s.actual));
            return next("COMMITTING");
        });
        reply("QuotaReleased", "RELEASING", (s, c, out) -> next("COMPENSATED"));
        reply("QuotaCommitted", "COMMITTING", (s, c, out) -> next(s.reason == null ? "COMPLETED" : "COMPENSATED"));
    }

    private static final class Saga {
        UUID id;
        long post;
        String reason;
        long actual;
        Msg msg;
    }

    private static final class Next {
        final String state;
        String reason;
        String summary;
        Long actual;

        Next(String state) {
            this.state = state;
        }

        Next reason(String r) {
            reason = r;
            return this;
        }

        Next summary(String s) {
            summary = s;
            return this;
        }

        Next actual(long a) {
            actual = a;
            return this;
        }
    }

    private static Next next(String state) {
        return new Next(state);
    }

    private interface Step {
        Next apply(Saga saga, Connection c, Node.Out out) throws SQLException;
    }

    /** guard가 켜져 있으면 expected 상태일 때만 전이한다. 꺼져 있으면 지금 상태와 상관없이 전이한다. */
    private void reply(String type, String expected, Step step) {
        orchestrator.on(replies, type, (m, c, out) -> {
            var s = new Saga();
            String state;
            try (var ps = c.prepareStatement(
                    "SELECT state, post, reason, coalesce(actual, 0) FROM saga WHERE id = ? FOR UPDATE")) {
                ps.setObject(1, m.saga());
                try (var rs = ps.executeQuery()) {
                    if (!rs.next()) return;
                    state = rs.getString(1);
                    s.post = rs.getLong(2);
                    s.reason = rs.getString(3);
                    s.actual = rs.getLong(4);
                }
            }
            if (!expected.equals(state) && options.guard()) {
                guarded.incrementAndGet();
                return;
            }
            s.id = m.saga();
            s.msg = m;
            var n = step.apply(s, c, out);
            Domain.update(c, """
                    UPDATE saga SET state = ?, reason = coalesce(?, reason), summary = coalesce(?, summary),
                      actual = coalesce(?, actual), updated_at = clock_timestamp() WHERE id = ?""",
                    n.state, n.reason, n.summary, n.actual, m.saga());
        });
    }

    @Override
    public void close() throws Exception {
        running = false;
        if (scanner != null) scanner.join(5_000);
        super.close();
    }
}
