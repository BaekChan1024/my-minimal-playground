package playground.sagafault;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * 이벤트 연쇄(2편과 같은 흐름, 검수 단계는 뺐다).
 *
 * guard가 켜져 있으면 Content는 요청 행의 상태가 기대한 값일 때만 이벤트를 처리한다. 오케스트레이터의 상태 가드에
 * 해당하는 것이 코레오그래피에서는 각 서비스 안의 상태 확인이다. 꺼져 있으면 상태와 상관없이 처리한다.
 * deadlineMs가 있으면 요청자(Content)가 시한을 걸고, 넘으면 SummaryCancelled를 낸다. Usage가 이를 듣고 예약을 해제한다.
 */
final class Choreography extends World {
    private final String contentEvents = topic("content.events");
    private final String usageEvents = topic("usage.events");
    private final String aiEvents = topic("ai.events");
    private volatile boolean running = true;
    private Thread scanner;

    Choreography(Infra infra, String prefix, Options options) throws Exception {
        super(infra, prefix, "choreography", options);
        infra.topics(List.of(contentEvents, usageEvents, aiEvents));
        content.sql("""
                CREATE TABLE request(saga uuid PRIMARY KEY, post bigint NOT NULL, author text NOT NULL,
                  tokens bigint NOT NULL, status text NOT NULL, reason text,
                  updated_at timestamptz NOT NULL DEFAULT clock_timestamp());
                """);
        wire();
    }

    @Override
    List<Node> nodes() {
        return new ArrayList<>(List.of(usage, content, ai));
    }

    @Override
    void startAll() {
        super.startAll();
        if (options.deadlineMs() > 0) scanner = Thread.ofPlatform().name(prefix + "-deadline").daemon().start(this::scan);
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

    @Override
    String state(UUID saga) throws SQLException {
        return content.text("SELECT status FROM request WHERE saga = ?", saga);
    }

    /** 시한: 예약 결과를 deadlineMs 넘게 기다린 요청을 취소하고 SummaryCancelled를 낸다. */
    private void scan() {
        while (running) {
            try {
                content.tx((c, out) -> {
                    try (var ps = c.prepareStatement("""
                            UPDATE request SET status = 'CANCELLING', reason = 'TIMEOUT', updated_at = clock_timestamp()
                             WHERE status = 'REQUESTED' AND updated_at < clock_timestamp() - make_interval(secs => ?)
                            RETURNING saga""")) {
                        ps.setDouble(1, options.deadlineMs() / 1000.0);
                        try (var rs = ps.executeQuery()) {
                            while (rs.next()) out.send(contentEvents, Msg.of((UUID) rs.getObject(1), "SummaryCancelled"));
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

    private void wire() {
        usage.on(contentEvents, "SummaryRequested", (m, c, out) -> out.send(usageEvents, Msg.of(m.saga(),
                        rules.reserve(c, m.saga(), m.str("author"), m.num("tokens")) ? "QuotaReserved" : "QuotaRejected")))
                .on(contentEvents, "SummaryCancelled", (m, c, out) -> {
                    rules.release(c, m.saga());
                    out.send(usageEvents, Msg.of(m.saga(), "QuotaReleased"));
                })
                .on(contentEvents, "PostLockFailed", (m, c, out) -> {
                    rules.release(c, m.saga());
                    out.send(usageEvents, Msg.of(m.saga(), "QuotaReleased"));
                })
                .on(contentEvents, "PostUnlocked", (m, c, out) -> {
                    rules.release(c, m.saga());
                    out.send(usageEvents, Msg.of(m.saga(), "QuotaReleased"));
                })
                .on(contentEvents, "SummaryAttached", (m, c, out) -> {
                    rules.commit(c, m.saga(), m.num("tokens"));
                    out.send(usageEvents, Msg.of(m.saga(), "QuotaCommitted"));
                });

        content.on(usageEvents, "QuotaReserved", (m, c, out) -> {
                    if (!expect(c, m.saga(), Set.of("REQUESTED"))) return;
                    long post = post(c, m.saga());
                    var lock = rules.lock(c, m.saga(), post);
                    if (lock == Domain.Lock.LOCKED) {
                        status(c, m.saga(), "LOCKED", null);
                        out.send(contentEvents, Msg.of(m.saga(), "PostLocked", "post", post));
                    } else {
                        status(c, m.saga(), "RELEASING", "LOCK_" + lock.name());
                        out.send(contentEvents, Msg.of(m.saga(), "PostLockFailed", "reason", lock.name()));
                    }
                })
                .on(usageEvents, "QuotaRejected", (m, c, out) -> {
                    if (expect(c, m.saga(), Set.of("REQUESTED"))) status(c, m.saga(), "REJECTED", null);
                })
                .on(usageEvents, "QuotaReleased", (m, c, out) -> {
                    if (expect(c, m.saga(), Set.of("RELEASING", "UNLOCKED", "CANCELLING"))) {
                        status(c, m.saga(), "COMPENSATED", null);
                    }
                })
                .on(usageEvents, "QuotaCommitted", (m, c, out) -> {
                    if (expect(c, m.saga(), Set.of("ATTACHED"))) status(c, m.saga(), "COMPLETED", null);
                })
                .on(aiEvents, "GenerationFailed", (m, c, out) -> {
                    if (!expect(c, m.saga(), Set.of("LOCKED"))) return;
                    rules.unlock(c, m.saga(), post(c, m.saga()));
                    status(c, m.saga(), "UNLOCKED", "GENERATION_FAILED");
                    out.send(contentEvents, Msg.of(m.saga(), "PostUnlocked", "reason", "GENERATION_FAILED"));
                })
                .on(aiEvents, "SummaryGenerated", (m, c, out) -> {
                    if (!expect(c, m.saga(), Set.of("LOCKED"))) return;
                    rules.attach(c, m.saga(), post(c, m.saga()), m.str("summary"));
                    status(c, m.saga(), "ATTACHED", null);
                    out.send(contentEvents, Msg.of(m.saga(), "SummaryAttached", "tokens", m.num("tokens")));
                });

        ai.on(contentEvents, "PostLocked", (m, c, out) -> {
            var g = rules.generate(c, m.saga(), m.num("post"), failing);
            out.send(aiEvents, g.ok()
                    ? Msg.of(m.saga(), "SummaryGenerated", "summary", g.summary(), "tokens", g.tokens())
                    : Msg.of(m.saga(), "GenerationFailed"));
        });
    }

    /** guard가 켜져 있으면 요청 상태가 expected 중 하나일 때만 true. 꺼져 있으면 항상 true. */
    private boolean expect(Connection c, UUID saga, Set<String> expected) throws SQLException {
        String status = Domain.one(c, "SELECT status FROM request WHERE saga = ? FOR UPDATE", saga);
        if (!options.guard() || expected.contains(status)) return true;
        guarded.incrementAndGet();
        return false;
    }

    private static long post(Connection c, UUID saga) throws SQLException {
        return Long.parseLong(Domain.one(c, "SELECT post FROM request WHERE saga = ?", saga));
    }

    private static void status(Connection c, UUID saga, String status, String reason) throws SQLException {
        Domain.update(c, "UPDATE request SET status = ?, reason = coalesce(?, reason), updated_at = clock_timestamp()"
                + " WHERE saga = ?", status, reason, saga);
    }

    @Override
    public void close() throws Exception {
        running = false;
        if (scanner != null) scanner.join(5_000);
        super.close();
    }
}
