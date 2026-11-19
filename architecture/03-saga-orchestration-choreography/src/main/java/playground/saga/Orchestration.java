package playground.saga;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 중앙 조율. 오케스트레이터가 Saga마다 상태 행을 하나 갖고, 참여자에게 명령을 보내고, 응답을 받아 다음 명령을 정한다.
 * 참여자는 자기 명령 토픽만 읽고 공용 응답 토픽에 답한다. 다른 참여자의 존재를 모른다.
 */
final class Orchestration extends World {
    final Node orchestrator;
    final AtomicInteger stale = new AtomicInteger();
    private final String usageCmd = topic("usage.commands");
    private final String contentCmd = topic("content.commands");
    private final String aiCmd = topic("ai.commands");
    private final String moderationCmd = topic("moderation.commands");
    private final String replies = topic("saga.replies");

    Orchestration(Infra infra, String prefix, boolean moderated) throws Exception {
        super(infra, prefix, "orchestration", moderated);
        var topics = new ArrayList<>(List.of(usageCmd, contentCmd, aiCmd, replies));
        if (moderated) topics.add(moderationCmd);
        infra.topics(topics);
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
        var all = new ArrayList<>(List.of(orchestrator, usage, content, ai));
        if (moderated) all.add(moderation);
        return all;
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

    /** 참여자: 명령 하나를 받아 자기 DB를 바꾸고 결과를 응답 토픽에 보낸다. */
    private void participants() {
        usage.on(usageCmd, "ReserveQuota", (m, c, out) -> out.send(replies, Msg.of(m.saga(),
                        Domain.reserve(c, m.saga(), m.str("author"), m.num("tokens")) ? "QuotaReserved" : "QuotaRejected")))
                .on(usageCmd, "ReleaseQuota", (m, c, out) -> {
                    Domain.release(c, m.saga());
                    out.send(replies, Msg.of(m.saga(), "QuotaReleased"));
                })
                .on(usageCmd, "CommitQuota", (m, c, out) -> {
                    Domain.commit(c, m.saga(), m.num("actual"));
                    out.send(replies, Msg.of(m.saga(), "QuotaCommitted"));
                });
        content.on(contentCmd, "LockPost", (m, c, out) -> {
                    var lock = Domain.lock(c, m.saga(), m.num("post"));
                    out.send(replies, lock == Domain.Lock.LOCKED ? Msg.of(m.saga(), "PostLocked")
                            : Msg.of(m.saga(), "PostLockFailed", "reason", lock.name()));
                })
                .on(contentCmd, "UnlockPost", (m, c, out) -> {
                    Domain.unlock(c, m.saga(), m.num("post"));
                    out.send(replies, Msg.of(m.saga(), "PostUnlocked"));
                })
                .on(contentCmd, "AttachSummary", (m, c, out) -> {
                    Domain.attach(c, m.saga(), m.num("post"), m.str("summary"));
                    out.send(replies, Msg.of(m.saga(), "SummaryAttached"));
                });
        ai.on(aiCmd, "Generate", (m, c, out) -> {
            var g = Domain.generate(c, m.saga(), m.num("post"), failing, banned);
            out.send(replies, g.ok() ? Msg.of(m.saga(), "Generated", "summary", g.summary(), "tokens", g.tokens())
                    : Msg.of(m.saga(), "GenerationFailed"));
        });
        if (moderated) {
            moderation.on(moderationCmd, "Moderate", (m, c, out) -> out.send(replies,
                    Msg.of(m.saga(), Domain.review(c, m.saga(), m.str("summary")) ? "Approved" : "Rejected")));
        }
    }

    /** 오케스트레이터의 상태 전이표. 응답 하나 = 상태 하나 바꾸고 다음 명령 하나 보내기. */
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
            String summary = s.msg.str("summary");
            if (moderated) out.send(moderationCmd, Msg.of(s.id, "Moderate", "summary", summary));
            else out.send(contentCmd, Msg.of(s.id, "AttachSummary", "post", s.post, "summary", summary));
            return next(moderated ? "MODERATING" : "ATTACHING").summary(summary).actual(s.msg.num("tokens"));
        });
        reply("GenerationFailed", "GENERATING", (s, c, out) -> {
            out.send(contentCmd, Msg.of(s.id, "UnlockPost", "post", s.post));
            return next("UNLOCKING").reason("GENERATION_FAILED");
        });
        if (moderated) {
            reply("Approved", "MODERATING", (s, c, out) -> {
                out.send(contentCmd, Msg.of(s.id, "AttachSummary", "post", s.post, "summary", s.summary));
                return next("ATTACHING");
            });
            reply("Rejected", "MODERATING", (s, c, out) -> {
                out.send(contentCmd, Msg.of(s.id, "UnlockPost", "post", s.post));
                return next("UNLOCKING").reason("MODERATION_REJECTED");
            });
        }
        reply("PostUnlocked", "UNLOCKING", (s, c, out) -> {
            // 생성이 실패했으면 쓴 토큰이 없으니 예약을 돌려준다. 검수에서 떨어졌으면 토큰은 이미 썼으므로 사용량으로 확정한다.
            // (검수 단계가 없으면 이 분기는 실행되지 않는다. 검수를 추가할 때 바뀌는 로직이 이 한 곳에 모인다.)
            if ("MODERATION_REJECTED".equals(s.reason)) {
                out.send(usageCmd, Msg.of(s.id, "CommitQuota", "actual", s.actual));
                return next("COMMITTING");
            }
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
        String summary;
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

    /** 응답 type이 expected 상태의 Saga에 도착했을 때만 전이한다. 상태가 다르면 늦거나 중복된 응답이므로 무시한다. */
    private void reply(String type, String expected, Step step) {
        orchestrator.on(replies, type, (m, c, out) -> {
            var s = new Saga();
            String state;
            try (var ps = c.prepareStatement(
                    "SELECT state, post, reason, summary, coalesce(actual, 0) FROM saga WHERE id = ? FOR UPDATE")) {
                ps.setObject(1, m.saga());
                try (var rs = ps.executeQuery()) {
                    if (!rs.next()) return;
                    state = rs.getString(1);
                    s.post = rs.getLong(2);
                    s.reason = rs.getString(3);
                    s.summary = rs.getString(4);
                    s.actual = rs.getLong(5);
                }
            }
            if (!expected.equals(state)) {
                stale.incrementAndGet();
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
}
