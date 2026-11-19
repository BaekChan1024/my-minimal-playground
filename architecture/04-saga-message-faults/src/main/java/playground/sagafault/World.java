package playground.sagafault;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 한 실험의 서비스 묶음. 실험마다 새 database와 새 토픽을 만든다.
 *
 * guard: 오케스트레이터의 "기대한 상태일 때만 전이"와 코레오그래피 Content의 "요청 상태가 맞을 때만 처리"를 켜고 끈다.
 * deadlineMs: 0보다 크면 첫 단계(한도 예약)의 응답을 이 시간까지 기다리고, 넘으면 Saga를 취소한다.
 */
abstract class World implements AutoCloseable {
    static final Set<String> FINAL = Set.of("COMPLETED", "COMPENSATED", "REJECTED");

    record Options(boolean guard, boolean naive, boolean tombstone, long deadlineMs) {
        static Options normal() {
            return new Options(true, false, false, 0);
        }
    }

    final String mode;
    final String prefix;
    final Options options;
    final Domain rules;
    final Infra infra;
    final ConcurrentLinkedQueue<Node.Sent> trace = new ConcurrentLinkedQueue<>();
    final Set<Long> failing = ConcurrentHashMap.newKeySet();
    /** 상태가 맞지 않아 처리하지 않고 넘긴 메시지 수(가드가 막은 수). */
    final AtomicInteger guarded = new AtomicInteger();
    final Node usage;
    final Node content;
    final Node ai;

    World(Infra infra, String prefix, String mode, Options options) throws Exception {
        this.infra = infra;
        this.prefix = prefix;
        this.mode = mode;
        this.options = options;
        this.rules = new Domain(options.naive(), options.tombstone());
        usage = node("usage", Domain.USAGE_SCHEMA);
        content = node("content", Domain.CONTENT_SCHEMA);
        ai = node("ai", Domain.AI_SCHEMA);
    }

    Node node(String service, String schema) throws Exception {
        var node = new Node(prefix + "_" + service, infra.database(prefix + "_" + service), infra.bootstrap, trace);
        node.sql(schema);
        return node;
    }

    String topic(String name) {
        return prefix + "." + name;
    }

    abstract List<Node> nodes();

    abstract UUID start(String author, long post, long tokens) throws SQLException;

    abstract String state(UUID saga) throws SQLException;

    void seed(int posts, Set<Long> missing, Map<String, Long> limits) throws SQLException {
        content.tx((c, out) -> {
            for (long id = 1; id <= posts; id++) {
                if (!missing.contains(id)) Domain.update(c, "INSERT INTO post(id, title) VALUES (?,?)", id, "글 " + id);
            }
            return null;
        });
        usage.tx((c, out) -> {
            for (var e : limits.entrySet()) Domain.update(c, "INSERT INTO quota(author, lim) VALUES (?,?)", e.getKey(), e.getValue());
            return null;
        });
    }

    void startAll() {
        nodes().forEach(Node::start);
    }

    /**
     * 모든 Saga가 끝 상태에 닿고, 보내지 않은 Outbox 행이 없고, 모든 소비 그룹이 끝까지 커밋한 상태가
     * 두 번 연속 관측될 때까지 기다린다. 늦게 도착한 메시지까지 처리된 뒤에 불변식을 보기 위해서다.
     */
    long await(Collection<UUID> sagas, Duration timeout) throws Exception {
        long started = System.nanoTime();
        long until = started + timeout.toNanos();
        int quietChecks = 0;
        while (System.nanoTime() < until) {
            boolean done = true;
            for (var s : sagas) {
                if (!FINAL.contains(state(s))) {
                    done = false;
                    break;
                }
            }
            quietChecks = done && quiet() ? quietChecks + 1 : 0;
            if (quietChecks >= 2) return (System.nanoTime() - started) / 1_000_000;
            Thread.sleep(50);
        }
        var stuck = new LinkedHashMap<UUID, String>();
        for (var s : sagas) if (!FINAL.contains(state(s))) stuck.put(s, state(s));
        throw new IllegalStateException(mode + " sagas did not settle: " + stuck);
    }

    private boolean quiet() throws Exception {
        for (var n : nodes()) {
            if (n.pending() > 0) return false;
            var topics = n.subscriptions().keySet();
            if (!topics.isEmpty() && infra.lag(n.name, topics) > 0) return false;
        }
        return true;
    }

    List<String> sequence(UUID saga) {
        return trace.stream().filter(s -> s.msg().saga().equals(saga))
                .sorted(Comparator.comparingLong(Node.Sent::nanos))
                .map(s -> s.node().substring(prefix.length() + 1) + ":" + s.msg().type()).toList();
    }

    long messages() {
        return trace.size();
    }

    int duplicatesDropped() {
        return nodes().stream().mapToInt(n -> n.duplicates.get()).sum();
    }

    int restarts() {
        return nodes().stream().mapToInt(n -> n.restarts.get()).sum();
    }

    record Check(boolean ok, String detail) {}

    Check invariants() throws SQLException {
        var problems = new ArrayList<String>();
        long reserved = usage.count("SELECT coalesce(sum(reserved), 0) FROM quota");
        long open = usage.count("SELECT count(*) FROM reservation WHERE status IN ('PENDING', 'RESERVED')");
        long locked = content.count("SELECT count(*) FROM post WHERE locked_by IS NOT NULL");
        long used = usage.count("SELECT coalesce(sum(used), 0) FROM quota");
        long paid = ai.count("SELECT coalesce(sum(tokens), 0) FROM job WHERE status = 'SUCCEEDED'");
        if (reserved != 0) problems.add("quota.reserved=" + reserved);
        if (open != 0) problems.add("openReservations=" + open);
        if (locked != 0) problems.add("lockedPosts=" + locked);
        if (used != paid) problems.add("used=" + used + "!=paidTokens=" + paid);
        for (var n : nodes()) if (n.pending() != 0) problems.add(n.name + ".outboxPending=" + n.pending());
        String detail = "reserved=" + reserved + " openReservations=" + open + " lockedPosts=" + locked
                + " used=" + used + " paidTokens=" + paid + " providerCalls=" + rules.providerCalls.get();
        return new Check(problems.isEmpty(), problems.isEmpty() ? detail : detail + " PROBLEMS=" + problems);
    }

    @Override
    public void close() throws Exception {
        for (var n : nodes()) n.close();
    }
}
