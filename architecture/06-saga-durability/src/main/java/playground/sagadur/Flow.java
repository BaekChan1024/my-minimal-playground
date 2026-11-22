package playground.sagadur;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 오케스트레이션 Saga 하나의 세계(2편의 오케스트레이션, 검수 단계 없음).
 * 오케스트레이터 + Usage·Content·AI, 서비스마다 database 하나, 명령 토픽 3개 + 응답 토픽 1개.
 */
final class Flow implements AutoCloseable {
    static final Set<String> FINAL = Set.of("COMPLETED", "COMPENSATED", "REJECTED");

    /** 피벗(요약 게시) 뒤 단계(사용량 확정)의 응답이 오지 않을 때. */
    enum AfterPivot { WAIT, RETRY, COMPENSATE }

    /**
     * @param direct     오케스트레이터가 Outbox 대신 "커밋하고 바로 보내기"를 쓴다
     * @param deadlineMs 0보다 크면 피벗 전 단계(예약·잠금·생성)를 이 시간까지 기다리고, 넘으면 보상한다
     * @param lateCharge 시한 보상 뒤에 생성 결과가 늦게 오면 쓴 토큰을 사용량에 기록한다
     * @param afterPivot 사용량 확정 응답이 retryMs 안에 오지 않을 때: 기다리기 / 다시 보내기(간격 두 배씩) / 보상하기
     * @param naive      Usage의 확정에서 "아직 예약 중일 때만" 조건을 뺀다
     */
    record Options(boolean direct, long deadlineMs, boolean lateCharge, AfterPivot afterPivot, long retryMs, boolean naive) {
        static Options plain() {
            return new Options(false, 0, false, AfterPivot.WAIT, 0, false);
        }
    }

    final Infra infra;
    final String prefix;
    final Options o;
    final Domain rules;
    final ConcurrentLinkedQueue<Node.Sent> trace = new ConcurrentLinkedQueue<>();
    final Node usage;
    final Node content;
    final Node ai;
    volatile Node orchestrator;
    final AtomicInteger guarded = new AtomicInteger();
    final AtomicInteger lateCharges = new AtomicInteger();
    final AtomicInteger resends = new AtomicInteger();
    final AtomicInteger droppedReplies = new AtomicInteger();
    /** 이 Saga들의 첫 번째 사용량 확정 응답을 Usage가 보내지 않는다(응답 유실 흉내). */
    final Set<UUID> dropFirstCommitReply = ConcurrentHashMap.newKeySet();
    private final String usageCmd;
    private final String contentCmd;
    private final String aiCmd;
    private final String replies;
    private volatile boolean running = true;
    private Thread scanner;

    Flow(Infra infra, String prefix, Options o) throws Exception {
        this.infra = infra;
        this.prefix = prefix;
        this.o = o;
        this.rules = new Domain(o.naive());
        usageCmd = prefix + ".usage.commands";
        contentCmd = prefix + ".content.commands";
        aiCmd = prefix + ".ai.commands";
        replies = prefix + ".saga.replies";
        infra.topics(List.of(usageCmd, contentCmd, aiCmd, replies));
        usage = node("usage", Domain.USAGE_SCHEMA);
        content = node("content", Domain.CONTENT_SCHEMA);
        ai = node("ai", Domain.AI_SCHEMA);
        orchestrator = node("orchestrator", """
                CREATE TABLE saga(id uuid PRIMARY KEY, author text NOT NULL, post bigint NOT NULL, tokens bigint NOT NULL,
                  state text NOT NULL, reason text, actual bigint, attempts int NOT NULL DEFAULT 0,
                  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(), next_retry_at timestamptz);
                """);
        orchestrator.direct = o.direct();
        participants();
        steps();
    }

    private Node node(String service, String schema) throws Exception {
        String name = prefix + "_" + service;
        var node = new Node(name, infra.database(name), infra.bootstrap, trace, true);
        node.sql(schema);
        return node;
    }

    List<Node> nodes() {
        return List.of(orchestrator, usage, content, ai);
    }

    void startAll() {
        nodes().forEach(Node::start);
        if (o.deadlineMs() > 0 || o.afterPivot() != AfterPivot.WAIT) {
            scanner = Thread.ofPlatform().name(prefix + "-scanner").daemon().start(this::scan);
        }
    }

    /** 같은 DB와 같은 소비 그룹으로 오케스트레이터를 새로 띄운다(죽은 프로세스를 다시 시작한 것에 해당). */
    void restartOrchestrator() throws Exception {
        var old = orchestrator;
        old.close();
        orchestrator = new Node(old.name, infra.connect(old.name), infra.bootstrap, trace, false);
        orchestrator.direct = o.direct();
        steps();
        orchestrator.start();
    }

    void seed(int posts, long limit) throws SQLException {
        content.tx((c, out) -> {
            for (long id = 1; id <= posts; id++) Domain.update(c, "INSERT INTO post(id, title) VALUES (?,?)", id, "글 " + id);
            return null;
        });
        usage.tx((c, out) -> Domain.update(c, "INSERT INTO quota(author, lim) VALUES ('author', ?)", limit));
    }

    UUID start(long post, long tokens) throws SQLException {
        UUID id = UUID.randomUUID();
        orchestrator.tx((c, out) -> {
            Domain.update(c, "INSERT INTO saga(id, author, post, tokens, state) VALUES (?, 'author', ?, ?, 'RESERVING')",
                    id, post, tokens);
            out.send(usageCmd, Msg.of(id, "ReserveQuota", "author", "author", "tokens", tokens));
            return null;
        });
        return id;
    }

    String state(UUID saga) throws SQLException {
        return orchestrator.text("SELECT state FROM saga WHERE id = ?", saga);
    }

    Map<String, Integer> states(Collection<UUID> sagas) throws SQLException {
        var m = new TreeMap<String, Integer>();
        for (var s : sagas) m.merge(state(s), 1, Integer::sum);
        return m;
    }

    /** 이 Saga에 대해 실제로 Kafka로 나간 메시지를 보낸 순서대로("서비스:종류"). */
    List<String> sequenceOf(UUID saga) {
        return trace.stream().filter(s -> s.msg().saga().equals(saga))
                .sorted(java.util.Comparator.comparingLong(Node.Sent::nanos))
                .map(s -> s.node().substring(s.node().indexOf('_') + 1) + ":" + s.msg().type()).toList();
    }

    // ------------------------------------------------------------------ 참여자
    private void participants() {
        usage.on(usageCmd, "ReserveQuota", (m, c, out) -> out.send(replies, Msg.of(m.saga(),
                        rules.reserve(c, m.saga(), m.str("author"), m.num("tokens")) ? "QuotaReserved" : "QuotaRejected")))
                .on(usageCmd, "ReleaseQuota", (m, c, out) -> {
                    rules.release(c, m.saga());
                    out.send(replies, Msg.of(m.saga(), "QuotaReleased"));
                })
                .on(usageCmd, "CommitQuota", (m, c, out) -> {
                    rules.commit(c, m.saga(), m.num("actual"));
                    if (dropFirstCommitReply.remove(m.saga())) {
                        droppedReplies.incrementAndGet();   // 확정은 반영했지만 응답이 사라졌다
                        return;
                    }
                    out.send(replies, Msg.of(m.saga(), "QuotaCommitted"));
                })
                .on(usageCmd, "ChargeLate", (m, c, out) -> rules.chargeLate(c, m.saga(), m.str("author"), m.num("actual")));
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
                })
                .on(contentCmd, "DetachSummary", (m, c, out) -> {
                    rules.detach(c, m.saga(), m.num("post"));
                    out.send(replies, Msg.of(m.saga(), "SummaryDetached"));
                });
        ai.on(aiCmd, "Generate", (m, c, out) -> {
            var g = rules.generate(c, m.saga(), m.num("post"));
            out.send(replies, Msg.of(m.saga(), "Generated", "summary", g.summary(), "tokens", g.tokens()));
        });
    }

    // ------------------------------------------------------------------ 오케스트레이터 전이표
    private record Row(UUID id, String author, long post, String reason, long actual) {}

    private interface Step {
        String apply(Row s, Msg m, Node.Out out) throws SQLException;
    }

    private void steps() {
        reply("QuotaReserved", "RESERVING", (s, m, out) -> {
            out.send(contentCmd, Msg.of(s.id(), "LockPost", "post", s.post()));
            return "LOCKING";
        });
        reply("QuotaRejected", "RESERVING", (s, m, out) -> "REJECTED");
        reply("PostLocked", "LOCKING", (s, m, out) -> {
            out.send(aiCmd, Msg.of(s.id(), "Generate", "post", s.post()));
            return "GENERATING";
        });
        reply("PostLockFailed", "LOCKING", (s, m, out) -> {
            out.send(usageCmd, Msg.of(s.id(), "ReleaseQuota"));
            return "RELEASING";
        });
        reply("Generated", "GENERATING", (s, m, out) -> {
            out.send(contentCmd, Msg.of(s.id(), "AttachSummary", "post", s.post(), "summary", m.str("summary")));
            return "ATTACHING";
        });
        reply("PostUnlocked", "UNLOCKING", (s, m, out) -> {
            out.send(usageCmd, Msg.of(s.id(), "ReleaseQuota"));
            return "RELEASING";
        });
        reply("SummaryAttached", "ATTACHING", (s, m, out) -> {
            out.send(usageCmd, Msg.of(s.id(), "CommitQuota", "actual", s.actual()));
            return "COMMITTING";
        });
        reply("SummaryDetached", "DETACHING", (s, m, out) -> {
            out.send(usageCmd, Msg.of(s.id(), "ReleaseQuota"));
            return "RELEASING";
        });
        reply("QuotaReleased", "RELEASING", (s, m, out) -> "COMPENSATED");
        reply("QuotaCommitted", "COMMITTING", (s, m, out) -> "COMPLETED");
    }

    private void reply(String type, String expected, Step step) {
        orchestrator.on(replies, type, (m, c, out) -> {
            Row s;
            String state;
            try (var ps = c.prepareStatement(
                    "SELECT state, author, post, reason, coalesce(actual, 0) FROM saga WHERE id = ? FOR UPDATE")) {
                ps.setObject(1, m.saga());
                try (var rs = ps.executeQuery()) {
                    if (!rs.next()) return;
                    state = rs.getString(1);
                    s = new Row(m.saga(), rs.getString(2), rs.getLong(3), rs.getString(4), rs.getLong(5));
                }
            }
            if (!expected.equals(state)) {
                guarded.incrementAndGet();
                // 시한으로 보상한 뒤에 생성 결과가 도착했다: 토큰은 이미 쓰였다.
                if (type.equals("Generated") && o.lateCharge() && "TIMEOUT".equals(s.reason())) {
                    out.send(usageCmd, Msg.of(s.id(), "ChargeLate", "author", s.author(), "actual", m.num("tokens")));
                    lateCharges.incrementAndGet();
                }
                return;
            }
            String next = step.apply(s, m, out);
            Long actual = type.equals("Generated") ? m.num("tokens") : null;
            Domain.update(c, "UPDATE saga SET state = ?, actual = coalesce(?, actual), attempts = 0, next_retry_at = "
                    + (next.equals("COMMITTING") && o.afterPivot() != AfterPivot.WAIT
                            ? "clock_timestamp() + make_interval(secs => " + o.retryMs() / 1000.0 + ")" : "NULL")
                    + ", updated_at = clock_timestamp() WHERE id = ?", next, actual, m.saga());
        });
    }

    // ------------------------------------------------------------------ 시한·재시도
    private void scan() {
        while (running) {
            try {
                if (!orchestrator.dead) {
                    orchestrator.tx((c, out) -> {
                        if (o.deadlineMs() > 0) deadlines(c, out);
                        if (o.afterPivot() != AfterPivot.WAIT) afterPivot(c, out);
                        return null;
                    });
                }
                Thread.sleep(20);
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                if (running) System.err.println("scanner: " + e);
            }
        }
    }

    /** 피벗 전 단계에 시한을 넘긴 Saga를 보상한다. 예약 중이면 해제, 잠금·생성 중이면 잠금부터 푼다. */
    private void deadlines(Connection c, Node.Out out) throws SQLException {
        try (var ps = c.prepareStatement("""
                UPDATE saga SET reason = 'TIMEOUT', updated_at = clock_timestamp(),
                  state = CASE WHEN state = 'RESERVING' THEN 'RELEASING' ELSE 'UNLOCKING' END
                 WHERE state IN ('RESERVING', 'LOCKING', 'GENERATING')
                   AND updated_at < clock_timestamp() - make_interval(secs => ?)
                RETURNING id, post, state""")) {
            ps.setDouble(1, o.deadlineMs() / 1000.0);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    UUID id = (UUID) rs.getObject(1);
                    if (rs.getString(3).equals("RELEASING")) out.send(usageCmd, Msg.of(id, "ReleaseQuota"));
                    else out.send(contentCmd, Msg.of(id, "UnlockPost", "post", rs.getLong(2)));
                }
            }
        }
    }

    /** 사용량 확정 응답이 오지 않았다. 다시 보내거나(간격 두 배씩), 보상한다. */
    private void afterPivot(Connection c, Node.Out out) throws SQLException {
        if (o.afterPivot() == AfterPivot.RETRY) {
            try (var ps = c.prepareStatement("""
                    UPDATE saga SET attempts = attempts + 1,
                      next_retry_at = clock_timestamp() + make_interval(secs => ? * power(2, attempts + 1))
                     WHERE state = 'COMMITTING' AND next_retry_at < clock_timestamp()
                    RETURNING id, actual""")) {
                ps.setDouble(1, o.retryMs() / 1000.0);
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        // 같은 명령을 새 메시지 ID로 다시 보낸다. Inbox는 이것을 중복으로 알아보지 못한다.
                        out.send(usageCmd, Msg.of((UUID) rs.getObject(1), "CommitQuota", "actual", rs.getLong(2)));
                        resends.incrementAndGet();
                    }
                }
            }
        } else {
            try (var ps = c.prepareStatement("""
                    UPDATE saga SET state = 'DETACHING', reason = 'TIMEOUT', updated_at = clock_timestamp(), next_retry_at = NULL
                     WHERE state = 'COMMITTING' AND next_retry_at < clock_timestamp()
                    RETURNING id, post""")) {
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) out.send(contentCmd, Msg.of((UUID) rs.getObject(1), "DetachSummary", "post", rs.getLong(2)));
                }
            }
        }
    }

    // ------------------------------------------------------------------ 기다리기·불변식
    /**
     * 모든 서비스의 미전송 Outbox가 0이고, 모든 소비 그룹이 끝까지 커밋했고, Saga 상태가 stable 동안 바뀌지 않으면 끝낸다.
     * 끝 상태에 닿지 못하고 멈춘 Saga가 있어도 끝난다(멈췄다는 것 자체가 관측 대상이다).
     */
    long settle(Collection<UUID> sagas, Duration stable, Duration timeout) throws Exception {
        long t0 = System.nanoTime();
        long until = t0 + timeout.toNanos();
        Map<String, Integer> last = null;
        long since = System.nanoTime();
        while (System.nanoTime() < until) {
            var now = states(sagas);
            boolean quiet = quiet();
            if (!quiet || !now.equals(last)) {
                last = now;
                since = System.nanoTime();
            } else if (System.nanoTime() - since >= stable.toNanos()) {
                return (System.nanoTime() - t0) / 1_000_000;
            }
            Thread.sleep(50);
        }
        throw new IllegalStateException("did not settle: " + last);
    }

    private boolean quiet() throws Exception {
        for (var n : nodes()) {
            if (n.dead) return false;
            if (n.pending() > 0) return false;
            if (!n.topics().isEmpty() && infra.lag(n.name, n.topics()) > 0) return false;
        }
        return true;
    }

    record Check(boolean ok, String detail) {}

    Check invariants() throws SQLException {
        var problems = new ArrayList<String>();
        long reserved = usage.count("SELECT coalesce(sum(reserved), 0) FROM quota");
        long open = usage.count("SELECT count(*) FROM reservation WHERE status IN ('PENDING', 'RESERVED')");
        long locked = content.count("SELECT count(*) FROM post WHERE locked_by IS NOT NULL");
        long used = usage.count("SELECT coalesce(sum(used), 0) FROM quota");
        long paid = ai.count("SELECT coalesce(sum(tokens), 0) FROM job WHERE status = 'SUCCEEDED'");
        long summaries = content.count("SELECT count(*) FROM post WHERE summary IS NOT NULL");
        if (reserved != 0) problems.add("quota.reserved=" + reserved);
        if (open != 0) problems.add("openReservations=" + open);
        if (locked != 0) problems.add("lockedPosts=" + locked);
        if (used != paid) problems.add("used=" + used + "!=paidTokens=" + paid);
        String detail = "reserved=" + reserved + " openReservations=" + open + " lockedPosts=" + locked + " summaries=" + summaries
                + " used=" + used + " paidTokens=" + paid + " providerCalls=" + rules.providerCalls.get();
        return new Check(problems.isEmpty(), problems.isEmpty() ? detail : detail + " PROBLEMS=" + problems);
    }

    @Override
    public void close() throws Exception {
        running = false;
        if (scanner != null) scanner.join(5_000);
        for (var n : nodes()) n.close();
    }
}
