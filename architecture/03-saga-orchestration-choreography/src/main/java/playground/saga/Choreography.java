package playground.saga;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 이벤트 연쇄. 조율자가 없다. 각 서비스가 자기 일을 마치면 "무슨 일이 있었다"를 자기 이벤트 토픽에 내고,
 * 다음 서비스는 그 이벤트를 구독해 자기 일을 한다. 보상도 실패 이벤트를 구독한 서비스가 스스로 한다.
 *
 * 요청을 받은 Content가 요청 행(request)을 두어 결과를 알려 줄 수 있게 했다. 이 행이 없으면 요청자는
 * Saga가 끝났는지 알 방법이 없다. 그래서 Content는 Usage의 마지막 이벤트까지 구독한다.
 */
final class Choreography extends World {
    private final String contentEvents = topic("content.events");
    private final String usageEvents = topic("usage.events");
    private final String aiEvents = topic("ai.events");
    private final String moderationEvents = topic("moderation.events");

    Choreography(Infra infra, String prefix, boolean moderated) throws Exception {
        super(infra, prefix, "choreography", moderated);
        var topics = new ArrayList<>(List.of(contentEvents, usageEvents, aiEvents));
        if (moderated) topics.add(moderationEvents);
        infra.topics(topics);
        content.sql("""
                CREATE TABLE request(saga uuid PRIMARY KEY, post bigint NOT NULL, author text NOT NULL,
                  tokens bigint NOT NULL, status text NOT NULL, reason text,
                  updated_at timestamptz NOT NULL DEFAULT clock_timestamp());
                """);
        wire();
    }

    @Override
    List<Node> nodes() {
        var all = new ArrayList<>(List.of(usage, content, ai));
        if (moderated) all.add(moderation);
        return all;
    }

    @Override
    UUID start(String author, long post, long tokens) throws SQLException {
        UUID id = UUID.randomUUID();
        content.tx((c, out) -> {
            Domain.update(c, "INSERT INTO request(saga, post, author, tokens, status) VALUES (?,?,?,?, 'REQUESTED')",
                    id, post, author, tokens);
            out.send(contentEvents, Msg.of(id, "SummaryRequested", "author", author, "tokens", tokens));
            return null;
        });
        return id;
    }

    /** 요청 행의 상태. Content가 아는 범위까지만 안다(예: Usage가 예약했는지, AI가 받았는지는 모른다). */
    @Override
    String state(UUID saga) throws SQLException {
        return content.text("SELECT status FROM request WHERE saga = ?", saga);
    }

    private void wire() {
        // Usage: Content가 낸 이벤트 네 종류를 알아야 한다(요청, 잠금 실패, 잠금 해제, 요약 게시).
        usage.on(contentEvents, "SummaryRequested", (m, c, out) -> out.send(usageEvents, Msg.of(m.saga(),
                        Domain.reserve(c, m.saga(), m.str("author"), m.num("tokens")) ? "QuotaReserved" : "QuotaRejected")))
                .on(contentEvents, "PostLockFailed", (m, c, out) -> {
                    Domain.release(c, m.saga());
                    out.send(usageEvents, Msg.of(m.saga(), "QuotaReleased"));
                })
                .on(contentEvents, "PostUnlocked", (m, c, out) -> {
                    // 왜 잠금이 풀렸는지에 따라 보상이 다르다. Usage가 Content의 사정을 알아야 하는 지점이다.
                    // 검수 단계를 추가할 때 이 분기도 함께 생긴다. 구독 목록은 그대로라 실험 6의 구독 비교에는 나오지 않는다.
                    if ("MODERATION_REJECTED".equals(m.str("reason"))) {
                        Domain.commit(c, m.saga(), m.num("tokens"));
                        out.send(usageEvents, Msg.of(m.saga(), "QuotaCommitted"));
                    } else {
                        Domain.release(c, m.saga());
                        out.send(usageEvents, Msg.of(m.saga(), "QuotaReleased"));
                    }
                })
                .on(contentEvents, "SummaryAttached", (m, c, out) -> {
                    Domain.commit(c, m.saga(), m.num("tokens"));
                    out.send(usageEvents, Msg.of(m.saga(), "QuotaCommitted"));
                });

        // Content: Usage·AI(·Moderation)의 이벤트를 구독한다.
        content.on(usageEvents, "QuotaReserved", (m, c, out) -> {
                    long post = post(c, m.saga());
                    var lock = Domain.lock(c, m.saga(), post);
                    if (lock == Domain.Lock.LOCKED) {
                        status(c, m.saga(), "LOCKED", null);
                        out.send(contentEvents, Msg.of(m.saga(), "PostLocked", "post", post));
                    } else {
                        status(c, m.saga(), "RELEASING", "LOCK_" + lock.name());
                        out.send(contentEvents, Msg.of(m.saga(), "PostLockFailed", "reason", lock.name()));
                    }
                })
                .on(usageEvents, "QuotaRejected", (m, c, out) -> status(c, m.saga(), "REJECTED", null))
                .on(usageEvents, "QuotaReleased", (m, c, out) -> status(c, m.saga(), "COMPENSATED", null))
                .on(usageEvents, "QuotaCommitted", (m, c, out) -> status(c, m.saga(),
                        "ATTACHED".equals(Domain.one(c, "SELECT status FROM request WHERE saga = ?", m.saga()))
                                ? "COMPLETED" : "COMPENSATED", null))
                .on(aiEvents, "GenerationFailed", (m, c, out) -> {
                    Domain.unlock(c, m.saga(), post(c, m.saga()));
                    status(c, m.saga(), "UNLOCKED", "GENERATION_FAILED");
                    out.send(contentEvents, Msg.of(m.saga(), "PostUnlocked", "reason", "GENERATION_FAILED"));
                });
        if (moderated) {
            content.on(moderationEvents, "Approved", this::attach)
                    .on(moderationEvents, "Rejected", (m, c, out) -> {
                        Domain.unlock(c, m.saga(), post(c, m.saga()));
                        status(c, m.saga(), "UNLOCKED", "MODERATION_REJECTED");
                        out.send(contentEvents, Msg.of(m.saga(), "PostUnlocked", "reason", "MODERATION_REJECTED",
                                "tokens", m.num("tokens")));
                    });
        } else {
            content.on(aiEvents, "SummaryGenerated", this::attach);
        }

        // AI: Content가 글을 잠갔다는 이벤트만 안다.
        ai.on(contentEvents, "PostLocked", (m, c, out) -> {
            var g = Domain.generate(c, m.saga(), m.num("post"), failing, banned);
            out.send(aiEvents, g.ok()
                    ? Msg.of(m.saga(), "SummaryGenerated", "summary", g.summary(), "tokens", g.tokens())
                    : Msg.of(m.saga(), "GenerationFailed"));
        });

        if (moderated) {
            moderation.on(aiEvents, "SummaryGenerated", (m, c, out) -> out.send(moderationEvents, Msg.of(m.saga(),
                    Domain.review(c, m.saga(), m.str("summary")) ? "Approved" : "Rejected",
                    "summary", m.str("summary"), "tokens", m.num("tokens"))));
        }
    }

    private void attach(Msg m, Connection c, Node.Out out) throws SQLException {
        Domain.attach(c, m.saga(), post(c, m.saga()), m.str("summary"));
        status(c, m.saga(), "ATTACHED", null);
        out.send(contentEvents, Msg.of(m.saga(), "SummaryAttached", "tokens", m.num("tokens")));
    }

    private static long post(Connection c, UUID saga) throws SQLException {
        return Long.parseLong(Domain.one(c, "SELECT post FROM request WHERE saga = ?", saga));
    }

    private static void status(Connection c, UUID saga, String status, String reason) throws SQLException {
        Domain.update(c, "UPDATE request SET status = ?, reason = coalesce(?, reason), updated_at = clock_timestamp()"
                + " WHERE saga = ?", status, reason, saga);
    }
}
