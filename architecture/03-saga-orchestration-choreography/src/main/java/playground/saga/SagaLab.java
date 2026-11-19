package playground.saga;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

/**
 * 같은 "AI 요약 게시" Saga를 오케스트레이션과 코레오그래피로 각각 실행해 비교한다.
 * 실제 임시 Kafka와 임시 PostgreSQL(서비스마다 database 하나)을 쓴다.
 * guided: 실험마다 질문을 보여 주고 Enter를 기다린다. verify: 질문 없이 모두 실행하고 판정한다.
 */
public final class SagaLab {
    private static final Duration WAIT = Duration.ofSeconds(60);
    private static final String AUTHOR = "author";
    private static boolean guided;
    private static Scanner in;
    private static final Map<String, Map<String, Map<String, Set<String>>>> plainRegistry = new HashMap<>();
    private static final Map<String, Map<String, String>> plainProducers = new HashMap<>();

    public static void main(String[] args) throws Exception {
        if (args.length > 1 || (args.length == 1 && !Set.of("guided", "verify").contains(args[0]))) {
            throw new IllegalArgumentException("guided|verify");
        }
        guided = args.length == 0 || args[0].equals("guided");
        in = new Scanner(System.in);
        if (!pause("""
                "한도 예약 → 글 잠금 → AI 요약 생성 → 요약 게시 → 사용량 확정" Saga를
                중앙 조율(오케스트레이션)과 이벤트 연쇄(코레오그래피)로 각각 실행해 여섯 조건에서 비교합니다.
                실제 임시 Kafka와 임시 PostgreSQL을 사용합니다. 서비스마다 database가 따로 있습니다.""")) return;
        try (var infra = new Infra()) {
            happyPath(infra);
            compensation(infra);
            whereIsIt(infra);
            mixedLoad(infra);
            coupling();
            addStep(infra);
        } catch (Quit q) {
            System.out.println("종료합니다. 임시 브로커와 DB를 정리했습니다.");
            return;
        }
        System.out.println("PASS experiments=6 realBroker=true realPostgres=true");
    }

    private static World world(Infra infra, String id, boolean orchestration, boolean moderated) throws Exception {
        String prefix = id + (orchestration ? "o" : "c");
        return orchestration ? new Orchestration(infra, prefix, moderated) : new Choreography(infra, prefix, moderated);
    }

    // 1. 정상 경로: 메시지가 몇 개, 어떤 순서로 오가는가.
    static void happyPath(Infra infra) throws Exception {
        if (!pause("[1/6] 성공하는 Saga 하나에 두 방식은 각각 메시지를 몇 개 주고받을까요?")) throw new Quit();
        for (boolean orch : List.of(true, false)) {
            try (var w = world(infra, "e1", orch, false)) {
                w.seed(1, Set.of(), Map.of(AUTHOR, 100_000L));
                w.startAll();
                var saga = w.start(AUTHOR, 1, 500);
                w.await(List.of(saga), WAIT);
                var seq = w.sequence(saga);
                var check = w.invariants();
                check("COMPLETED".equals(w.state(saga)), w.mode + " happy path should complete");
                check(check.ok(), w.mode + " invariants: " + check.detail());
                check(seq.size() == (orch ? 10 : 6), w.mode + " message count " + seq.size());
                plainRegistry.put(w.mode, w.registry());
                producers(w);
                System.out.println("PASS happy mode=" + w.mode + " messages=" + seq.size() + " sequence=" + seq);
                System.out.println("     " + check.detail());
            }
        }
    }

    // 2. 중간 실패: 보상이 어떤 순서로 일어나는가.
    static void compensation(Infra infra) throws Exception {
        if (!pause("""
                [2/6] 글이 없어서 잠금이 실패하는 경우, 그리고 AI 생성이 실패하는 경우에
                예약한 한도와 잠근 글은 누가 어떤 순서로 되돌릴까요?""")) throw new Quit();
        for (boolean orch : List.of(true, false)) {
            try (var w = world(infra, "e2", orch, false)) {
                w.seed(3, Set.of(2L), Map.of(AUTHOR, 100_000L));
                w.failing.add(3L);
                w.startAll();
                var ok = w.start(AUTHOR, 1, 500);
                var missing = w.start(AUTHOR, 2, 500);
                var genFail = w.start(AUTHOR, 3, 500);
                w.await(List.of(ok, missing, genFail), WAIT);
                var check = w.invariants();
                check("COMPLETED".equals(w.state(ok)), w.mode + " ok");
                check("COMPENSATED".equals(w.state(missing)), w.mode + " missing post should compensate");
                check("COMPENSATED".equals(w.state(genFail)), w.mode + " failed generation should compensate");
                check(check.ok(), w.mode + " invariants: " + check.detail());
                producers(w);
                System.out.println("PASS compensation mode=" + w.mode);
                System.out.println("     lockFailed(" + w.sequence(missing).size() + ")=" + w.sequence(missing));
                System.out.println("     generationFailed(" + w.sequence(genFail).size() + ")=" + w.sequence(genFail));
                System.out.println("     " + check.detail());
            }
        }
    }

    // 3. AI 서비스가 멈췄을 때 "지금 어디까지 갔나"를 어디에 물어야 하는가.
    static void whereIsIt(Infra infra) throws Exception {
        if (!pause("""
                [3/6] AI 서비스의 소비가 멈춘 동안 Saga 5개를 시작합니다.
                "어느 Saga가 어느 단계에서 멈췄나"를 알려면 각 방식에서 몇 개의 DB를 봐야 할까요?""")) throw new Quit();
        for (boolean orch : List.of(true, false)) {
            try (var w = world(infra, "e3", orch, false)) {
                w.seed(5, Set.of(), Map.of(AUTHOR, 100_000L));
                w.ai.pause();
                w.startAll();
                var sagas = new ArrayList<UUID>();
                for (long post = 1; post <= 5; post++) sagas.add(w.start(AUTHOR, post, 500));
                // 잠금 5개가 끝나고, 시작한 쪽이 그 결과까지 기록할 때까지 기다린다(AI는 멈춰 있다).
                waitUntil(() -> w.content.count("SELECT count(*) FROM post WHERE locked_by IS NOT NULL") == 5
                        && w.content.pending() == 0
                        && (orch ? ((Orchestration) w).orchestrator.count(
                                        "SELECT count(*) FROM saga WHERE state = 'GENERATING'") == 5
                                : w.content.count("SELECT count(*) FROM request WHERE status = 'LOCKED'") == 5),
                        "five posts locked");
                Thread.sleep(600);
                var views = new LinkedHashMap<String, String>();
                if (orch) {
                    var o = (Orchestration) w;
                    views.put("orchestrator", group(o.orchestrator,
                            "SELECT state || '=' || count(*) FROM saga WHERE state NOT IN ('COMPLETED','COMPENSATED','REJECTED')"
                                    + " AND updated_at < clock_timestamp() - interval '500 milliseconds' GROUP BY state"));
                } else {
                    views.put("content.request", group(w.content,
                            "SELECT status || '=' || count(*) FROM request GROUP BY status"));
                }
                views.put("usage.reservation", group(w.usage,
                        "SELECT status || '=' || count(*) FROM reservation GROUP BY status"));
                views.put("ai.job", "rows=" + w.ai.count("SELECT count(*) FROM job"));
                if (orch) {
                    check(views.get("orchestrator").equals("[GENERATING=5]"), "orchestrator should name the step");
                } else {
                    check(views.get("content.request").equals("[LOCKED=5]"), "content only knows it locked");
                }
                check(views.get("ai.job").equals("rows=0"), "ai did nothing while paused");
                long pausedAt = System.nanoTime();
                w.ai.resume();
                w.await(sagas, WAIT);
                long resumedMs = (System.nanoTime() - pausedAt) / 1_000_000;
                var check = w.invariants();
                check(check.ok(), w.mode + " invariants after resume: " + check.detail());
                for (var s : sagas) check("COMPLETED".equals(w.state(s)), w.mode + " should complete after resume");
                producers(w);
                System.out.println("PASS stuck mode=" + w.mode + " views=" + views + " completedAfterResumeMs=" + resumedMs);
            }
        }
    }

    // 5. 누가 누구의 메시지를 알아야 하는가(실험 1의 구독 등록과, 실험 1~4에서 실제로 메시지를 보낸 서비스로 센다).
    static void coupling() throws Exception {
        if (!pause("[5/6] 각 서비스는 다른 서비스가 정의한 메시지를 몇 종류나 알아야 할까요?")) throw new Quit();
        for (var mode : List.of("orchestration", "choreography")) {
            var registry = plainRegistry.get(mode);
            var producedBy = plainProducers.get(mode);
            var report = new LinkedHashMap<String, String>();
            for (var e : registry.entrySet()) {
                var foreignTypes = new TreeSet<String>();
                var from = new TreeSet<String>();
                for (var types : e.getValue().values()) {
                    for (var t : types) {
                        String producer = producedBy.get(t);
                        if (producer != null && !producer.equals(e.getKey())) {
                            foreignTypes.add(t);
                            from.add(producer);
                        }
                    }
                }
                report.put(e.getKey(), "types=" + foreignTypes.size() + " from=" + from);
            }
            System.out.println("PASS coupling mode=" + mode + " " + report);
            System.out.println("     subscriptions=" + registry);
        }
    }

    // 6. 단계 하나(요약 검수)를 끼워 넣으면 어느 서비스가 바뀌는가.
    static void addStep(Infra infra) throws Exception {
        if (!pause("""
                [6/6] 생성과 게시 사이에 "요약 검수" 단계를 추가합니다.
                기존 서비스 가운데 구독을 바꿔야 하는 곳은 어디일까요? 검수에서 떨어지면 토큰은 어떻게 처리할까요?""")) throw new Quit();
        for (boolean orch : List.of(true, false)) {
            try (var w = world(infra, "e5", orch, true)) {
                w.seed(2, Set.of(), Map.of(AUTHOR, 100_000L));
                w.banned.add(2L);
                w.startAll();
                var approved = w.start(AUTHOR, 1, 500);
                var rejected = w.start(AUTHOR, 2, 500);
                w.await(List.of(approved, rejected), WAIT);
                var check = w.invariants();
                check("COMPLETED".equals(w.state(approved)), w.mode + " approved");
                check("COMPENSATED".equals(w.state(rejected)), w.mode + " rejected");
                check(check.ok(), w.mode + " invariants: " + check.detail());
                long charged = w.usage.count("SELECT count(*) FROM reservation WHERE status = 'COMMITTED'");
                check(charged == 2, w.mode + " both generations are charged");
                check(w.content.count("SELECT count(*) FROM post WHERE summary IS NOT NULL") == 1, "only approved attached");
                var before = plainRegistry.get(w.mode);
                var after = w.registry();
                var changed = new TreeMap<String, String>();
                for (var e : after.entrySet()) {
                    var old = before.get(e.getKey());
                    if (old == null) changed.put(e.getKey(), "NEW " + e.getValue());
                    else if (!Objects.equals(old, e.getValue())) changed.put(e.getKey(), old + " -> " + e.getValue());
                }
                System.out.println("PASS addStep mode=" + w.mode + " changedSubscriptions=" + changed.keySet());
                changed.forEach((k, v) -> System.out.println("     " + k + ": " + v));
                System.out.println("     rejected=" + w.sequence(rejected));
                System.out.println("     " + check.detail());
            }
        }
    }

    // 4. 성공·실패·거절이 섞인 60개를 한꺼번에.
    static void mixedLoad(Infra infra) throws Exception {
        if (!pause("""
                [4/6] 성공·잠금 실패·생성 실패·한도 거절이 섞인 Saga 60개를 동시에 시작합니다.
                두 방식 모두 한도·잠금·과금이 맞아떨어질까요? 메시지는 각각 몇 개일까요?""")) throw new Quit();
        for (boolean orch : List.of(true, false)) {
            try (var w = world(infra, "e6", orch, false)) {
                var missing = new TreeSet<Long>();
                for (long p = 1; p <= 60; p++) {
                    if (p % 10 == 3) missing.add(p);
                    if (p % 10 == 7) w.failing.add(p);
                }
                w.seed(60, missing, Map.of(AUTHOR, 1_000_000L, "tight", 700L));
                w.startAll();
                var sagas = new ArrayList<UUID>();
                long started = System.nanoTime();
                for (long p = 1; p <= 60; p++) sagas.add(w.start(p % 10 == 5 ? "tight" : AUTHOR, p, 500));
                w.await(sagas, WAIT);
                long ms = (System.nanoTime() - started) / 1_000_000;
                var states = new TreeMap<String, Integer>();
                for (var s : sagas) states.merge(w.state(s), 1, Integer::sum);
                var check = w.invariants();
                check(check.ok(), w.mode + " invariants: " + check.detail());
                int stale = w instanceof Orchestration o ? o.stale.get() : 0;
                int duplicates = w.nodes().stream().mapToInt(n -> n.duplicates.get()).sum();
                producers(w);
                System.out.println("PASS mixed mode=" + w.mode + " sagas=60 states=" + states + " messages=" + w.messages()
                        + " elapsedMs=" + ms + " staleReplies=" + stale + " inboxDuplicates=" + duplicates);
                System.out.println("     " + check.detail());
            }
        }
    }

    /** 이 실행에서 실제로 메시지를 보낸 서비스를 메시지 종류별로 모은다(방식별 누적). */
    private static void producers(World w) {
        var map = plainProducers.computeIfAbsent(w.mode, m -> new HashMap<>());
        for (var s : w.trace) map.put(s.msg().type(), s.node());
    }

    private static String group(Node node, String sql) throws Exception {
        var rows = new ArrayList<String>();
        try (var c = node.db.getConnection(); var ps = c.prepareStatement(sql + " ORDER BY 1"); var rs = ps.executeQuery()) {
            while (rs.next()) rows.add(rs.getString(1));
        }
        return rows.toString();
    }

    private interface Condition {
        boolean met() throws Exception;
    }

    private static void waitUntil(Condition condition, String what) throws Exception {
        long until = System.nanoTime() + WAIT.toNanos();
        while (System.nanoTime() < until) {
            if (condition.met()) return;
            Thread.sleep(10);
        }
        throw new IllegalStateException("Timed out waiting for " + what);
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new IllegalStateException("FAIL " + message);
    }

    private static boolean pause(String question) {
        System.out.println();
        System.out.println(question);
        if (!guided) return true;
        System.out.print("Enter로 실행, q로 종료 > ");
        if (!in.hasNextLine()) return false;
        return !in.nextLine().trim().equalsIgnoreCase("q");
    }

    private static final class Quit extends RuntimeException {}
}
