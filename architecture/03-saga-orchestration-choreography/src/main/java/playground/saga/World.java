package playground.saga;

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

/**
 * 한 실험에서 쓰는 서비스 묶음. 실험마다 새 database와 새 토픽을 만들어 앞 실험의 상태가 섞이지 않게 한다.
 * 하위 클래스가 같은 Saga를 오케스트레이션 또는 코레오그래피로 연결한다.
 */
abstract class World implements AutoCloseable {
    static final Set<String> FINAL = Set.of("COMPLETED", "COMPENSATED", "REJECTED");

    final String mode;
    final String prefix;
    final boolean moderated;
    final Infra infra;
    final ConcurrentLinkedQueue<Node.Sent> trace = new ConcurrentLinkedQueue<>();
    final Set<Long> failing = ConcurrentHashMap.newKeySet();
    final Set<Long> banned = ConcurrentHashMap.newKeySet();
    final Node usage;
    final Node content;
    final Node ai;
    final Node moderation;

    World(Infra infra, String prefix, String mode, boolean moderated) throws Exception {
        this.infra = infra;
        this.prefix = prefix;
        this.mode = mode;
        this.moderated = moderated;
        usage = node("usage", Domain.USAGE_SCHEMA);
        content = node("content", Domain.CONTENT_SCHEMA);
        ai = node("ai", Domain.AI_SCHEMA);
        moderation = moderated ? node("moderation", Domain.MODERATION_SCHEMA) : null;
    }

    Node node(String service, String schema) throws Exception {
        var node = new Node(service, infra.database(prefix + "_" + service), infra.bootstrap, trace);
        node.sql(schema);
        return node;
    }

    String topic(String name) {
        return prefix + "." + name;
    }

    abstract List<Node> nodes();

    /** Saga 하나를 시작한다. 시작 기록과 첫 메시지는 시작하는 서비스의 한 트랜잭션에 들어간다. */
    abstract UUID start(String author, long post, long tokens) throws SQLException;

    /** 이 Saga가 지금 어디까지 갔는지, 이 방식에서 "한 곳"에 물어 얻을 수 있는 값. */
    abstract String state(UUID saga) throws SQLException;

    /** 글 1..posts를 만들고(missing 제외) 저자 한도를 넣는다. */
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

    /** 모든 Saga가 끝 상태에 닿고 Outbox가 비어 조용해질 때까지 기다린다. 걸린 시간(ms)을 돌려준다. */
    long await(Collection<UUID> sagas, Duration timeout) throws Exception {
        long started = System.nanoTime();
        long until = started + timeout.toNanos();
        while (System.nanoTime() < until) {
            boolean done = true;
            for (var s : sagas) {
                if (!FINAL.contains(state(s))) {
                    done = false;
                    break;
                }
            }
            if (done && quiet()) return (System.nanoTime() - started) / 1_000_000;
            Thread.sleep(10);
        }
        var stuck = new LinkedHashMap<UUID, String>();
        for (var s : sagas) if (!FINAL.contains(state(s))) stuck.put(s, state(s));
        throw new IllegalStateException(mode + " sagas did not finish: " + stuck);
    }

    private boolean quiet() throws SQLException {
        for (var n : nodes()) if (n.pending() > 0) return false;
        return true;
    }

    /** Saga 하나에 대해 실제로 Kafka로 나간 메시지를 보낸 순서대로. */
    List<String> sequence(UUID saga) {
        return trace.stream().filter(s -> s.msg().saga().equals(saga))
                .sorted(Comparator.comparingLong(Node.Sent::nanos))
                .map(s -> s.node() + ":" + s.msg().type()).toList();
    }

    long messages() {
        return trace.size();
    }

    Map<String, Map<String, Set<String>>> registry() {
        var view = new LinkedHashMap<String, Map<String, Set<String>>>();
        // 실험마다 붙는 토픽 접두어를 떼어 서로 다른 실험의 구독을 비교할 수 있게 한다.
        for (var n : nodes()) {
            var subs = new java.util.TreeMap<String, Set<String>>();
            n.subscriptions().forEach((t, types) -> subs.put(t.substring(prefix.length() + 1), types));
            view.put(n.name, subs);
        }
        return view;
    }

    /** 서비스 경계를 넘는 불변식. Saga가 모두 끝난 뒤에 확인한다. */
    record Check(boolean ok, String detail) {}

    Check invariants() throws SQLException {
        var problems = new ArrayList<String>();
        long reserved = usage.count("SELECT coalesce(sum(reserved), 0) FROM quota");
        long open = usage.count("SELECT count(*) FROM reservation WHERE status IN ('PENDING', 'RESERVED')");
        long locked = content.count("SELECT count(*) FROM post WHERE locked_by IS NOT NULL");
        long charged = usage.count("SELECT coalesce(sum(actual), 0) FROM reservation WHERE status = 'COMMITTED'");
        long used = usage.count("SELECT coalesce(sum(used), 0) FROM quota");
        long paid = ai.count("SELECT coalesce(sum(tokens), 0) FROM job WHERE status = 'SUCCEEDED'");
        long summaries = content.count("SELECT count(*) FROM post WHERE summary IS NOT NULL");
        if (reserved != 0) problems.add("quota.reserved=" + reserved);
        if (open != 0) problems.add("openReservations=" + open);
        if (locked != 0) problems.add("lockedPosts=" + locked);
        if (charged != used) problems.add("charged=" + charged + "!=used=" + used);
        if (charged != paid) problems.add("charged=" + charged + "!=paidTokens=" + paid);
        for (var n : nodes()) if (n.pending() != 0) problems.add(n.name + ".outboxPending=" + n.pending());
        String detail = "reserved=" + reserved + " lockedPosts=" + locked + " summaries=" + summaries
                + " charged=" + charged + " paidTokens=" + paid;
        return new Check(problems.isEmpty(), problems.isEmpty() ? detail : detail + " PROBLEMS=" + problems);
    }

    @Override
    public void close() throws Exception {
        for (var n : nodes()) n.close();
    }
}
