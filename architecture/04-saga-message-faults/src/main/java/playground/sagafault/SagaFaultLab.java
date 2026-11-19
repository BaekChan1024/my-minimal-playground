package playground.sagafault;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * 2편의 Saga(오케스트레이션·코레오그래피)에 메시지 장애를 넣는다. 실제 임시 Kafka와 임시 PostgreSQL을 쓴다.
 * guided: 실험마다 질문을 보여 주고 Enter를 기다린다. verify: 질문 없이 모두 실행하고 판정한다.
 */
public final class SagaFaultLab {
    private static final Duration WAIT = Duration.ofSeconds(90);
    private static final String AUTHOR = "author";
    private static final Map<String, Long> LIMITS = Map.of(AUTHOR, 1_000_000L);
    private static boolean guided;
    private static Scanner in;
    private static int worldId;

    public static void main(String[] args) throws Exception {
        if (args.length > 1 || (args.length == 1 && !Set.of("guided", "verify").contains(args[0]))) {
            throw new IllegalArgumentException("guided|verify");
        }
        guided = args.length == 0 || args[0].equals("guided");
        in = new Scanner(System.in);
        if (!pause("""
                2편의 "AI 요약 게시" Saga에 메시지 장애를 넣습니다: 같은 메시지가 두 번 오기, 소비자가 오프셋을
                커밋하기 전에 죽기, 첫 요청이 늦게 도착해 취소가 먼저 도착하기, 끝난 Saga에 늦은 응답이 오기.
                실제 임시 Kafka와 임시 PostgreSQL을 사용합니다.""")) return;
        try (var infra = new Infra()) {
            duplicates(infra);
            redelivery(infra);
            cancelFirst(infra);
            lateReply(infra);
        } catch (Quit q) {
            System.out.println("종료합니다. 임시 브로커와 DB를 정리했습니다.");
            return;
        }
        System.out.println("PASS experiments=4 realBroker=true realPostgres=true");
    }

    private static World world(Infra infra, boolean orchestration, World.Options options) throws Exception {
        String prefix = "w" + (++worldId) + (orchestration ? "o" : "c");
        return orchestration ? new Orchestration(infra, prefix, options) : new Choreography(infra, prefix, options);
    }

    private record Config(String name, boolean inbox, boolean guard, boolean naive) {}

    // 1. 같은 메시지가 두 번 오면: Inbox, 상태 가드, 업무 규칙의 상태 조건 중 무엇이 막는가.
    static void duplicates(Infra infra) throws Exception {
        if (!pause("""
                [1/4] 릴레이가 QuotaReserved와 SummaryAttached를 두 번씩 보냅니다(보낸 뒤 기록 전에 죽은 경우).
                Inbox, 상태 가드, 업무 규칙의 상태 조건을 하나씩 끄면 어디서부터 사용량이 틀어질까요?""")) throw new Quit();
        var configs = List.of(
                new Config("A inbox+guard+rules", true, true, false),
                new Config("B guard+rules", false, true, false),
                new Config("C rules", false, false, false),
                new Config("D none", false, false, true),
                new Config("E inbox only", true, false, true));
        for (boolean orch : List.of(true, false)) {
            for (var cfg : configs) {
                try (var w = world(infra, orch, new World.Options(cfg.guard(), cfg.naive(), false, 0))) {
                    for (var n : w.nodes()) {
                        n.inbox = cfg.inbox();
                        n.duplicate.addAll(Set.of("QuotaReserved", "SummaryAttached"));
                    }
                    w.seed(10, Set.of(), LIMITS);
                    w.startAll();
                    var sagas = new ArrayList<UUID>();
                    for (long p = 1; p <= 10; p++) sagas.add(w.start(AUTHOR, p, 500));
                    w.await(sagas, WAIT);
                    var states = states(w, sagas);
                    var check = w.invariants();
                    boolean expectOk = !cfg.name().startsWith("D");
                    check(check.ok() == expectOk, w.mode + " " + cfg.name() + " invariants " + check.detail());
                    check(states.equals(Map.of("COMPLETED", 10)), w.mode + " " + cfg.name() + " states " + states);
                    check(w.rules.providerCalls.get() == 10, w.mode + " " + cfg.name() + " provider calls");
                    System.out.println("PASS dup mode=" + w.mode + " config=\"" + cfg.name() + "\" messages=" + w.messages()
                            + " inboxDropped=" + w.duplicatesDropped() + " guarded=" + w.guarded.get()
                            + " invariantsOk=" + check.ok());
                    System.out.println("     " + check.detail());
                }
            }
        }
    }

    // 2. 진짜 재전달: Usage가 처리를 커밋한 뒤 오프셋을 커밋하기 전에 죽는다.
    static void redelivery(Infra infra) throws Exception {
        if (!pause("""
                [2/4] Usage가 사용량 확정을 DB에 커밋한 직후, Kafka 오프셋을 커밋하기 전에 소비자가 죽습니다.
                새 소비자는 어디서부터 다시 읽을까요? Inbox가 없고 확정에 상태 조건도 없으면 사용량은 어떻게 될까요?""")) throw new Quit();
        for (boolean orch : List.of(true, false)) {
            for (boolean protectedRun : List.of(true, false)) {
                var options = new World.Options(true, !protectedRun, false, 0);
                try (var w = world(infra, orch, options)) {
                    for (var n : w.nodes()) n.inbox = protectedRun;
                    w.usage.crashAfter = orch ? "CommitQuota" : "SummaryAttached";
                    w.seed(3, Set.of(), LIMITS);
                    w.startAll();
                    var sagas = new ArrayList<UUID>();
                    for (long p = 1; p <= 3; p++) sagas.add(w.start(AUTHOR, p, 500));
                    w.await(sagas, WAIT);
                    var check = w.invariants();
                    check(w.restarts() == 1, w.mode + " restart once");
                    check(check.ok() == protectedRun, w.mode + " redelivery invariants " + check.detail());
                    if (protectedRun) check(w.duplicatesDropped() >= 1, w.mode + " inbox should drop the redelivered message");
                    System.out.println("PASS redelivery mode=" + w.mode + " config=" + (protectedRun ? "\"inbox+rules\"" : "\"none\"")
                            + " restarts=" + w.restarts() + " usageReceived=" + w.usage.received.get()
                            + " inboxDropped=" + w.duplicatesDropped() + " invariantsOk=" + check.ok());
                    System.out.println("     " + check.detail());
                }
            }
        }
    }

    // 3. 첫 요청(예약)이 늦게 도착해, 시한 초과로 보낸 취소(해제)가 먼저 도착한다.
    static void cancelFirst(Infra infra) throws Exception {
        if (!pause("""
                [3/4] 예약 요청을 3초 늦게 보내고, 시한은 0.3초로 둡니다. 취소(예약 해제)가 예약보다 먼저 Usage에 닿습니다.
                뒤늦게 도착한 예약은 잡힐까요? "취소됨" 표시를 남기면 무엇이 달라질까요?""")) throw new Quit();
        for (boolean orch : List.of(true, false)) {
            for (boolean tombstone : List.of(false, true)) {
                try (var w = world(infra, orch, new World.Options(true, false, tombstone, 300))) {
                    delayFirstStep(w, orch);
                    w.seed(3, Set.of(), LIMITS);
                    w.startAll();
                    var sagas = new ArrayList<UUID>();
                    for (long p = 1; p <= 3; p++) sagas.add(w.start(AUTHOR, p, 500));
                    w.await(sagas, WAIT);
                    var states = states(w, sagas);
                    var check = w.invariants();
                    long cancelled = w.usage.count("SELECT count(*) FROM reservation WHERE status = 'CANCELLED'");
                    cancelSentFirst(w, sagas, orch);
                    check(states.equals(Map.of("COMPENSATED", 3)), w.mode + " states " + states);
                    check(cancelled == (tombstone ? 3 : 0), w.mode + " cancelled markers " + cancelled);
                    check(check.ok() == tombstone, w.mode + " tombstone=" + tombstone + " " + check.detail());
                    System.out.println("PASS cancelFirst mode=" + w.mode + " tombstone=" + tombstone + " states=" + states
                            + " cancelledMarkers=" + cancelled + " invariantsOk=" + check.ok());
                    System.out.println("     " + check.detail());
                    System.out.println("     sequence=" + w.sequence(sagas.getFirst()));
                }
            }
        }
    }

    // 4. 취소로 끝난 Saga에 늦은 예약 응답이 도착한다. 상태 가드가 없으면?
    static void lateReply(Infra infra) throws Exception {
        if (!pause("""
                [4/4] 3번과 같은 조건에서 "취소됨" 표시도, 상태 가드도 끕니다.
                이미 취소로 끝난 Saga에 뒤늦게 "예약됨"이 도착하면 어떻게 될까요?""")) throw new Quit();
        for (boolean orch : List.of(true, false)) {
            try (var w = world(infra, orch, new World.Options(false, false, false, 300))) {
                delayFirstStep(w, orch);
                w.seed(3, Set.of(), LIMITS);
                w.startAll();
                var sagas = new ArrayList<UUID>();
                for (long p = 1; p <= 3; p++) sagas.add(w.start(AUTHOR, p, 500));
                Thread.sleep(1_500);
                var early = states(w, sagas);
                w.await(sagas, WAIT);
                var late = states(w, sagas);
                var check = w.invariants();
                cancelSentFirst(w, sagas, orch);
                long summaries = w.content.count("SELECT count(*) FROM post WHERE summary IS NOT NULL");
                check(early.equals(Map.of("COMPENSATED", 3)), w.mode + " cancelled first " + early);
                // 취소로 끝났던 Saga 3개 모두 요약이 게시됐다. 최종 상태 값은 방식마다 다르다(출력으로 비교한다).
                check(summaries == 3, w.mode + " revived " + late + " summaries=" + summaries);
                System.out.println("PASS lateReply mode=" + w.mode + " at1_5s=" + early + " final=" + late + " summaries=" + summaries
                        + " invariantsOk=" + check.ok());
                System.out.println("     " + check.detail());
                System.out.println("     sequence=" + w.sequence(sagas.getFirst()));
            }
        }
    }

    /** 첫 단계 메시지를 3초 늦게 보낸다. 오케스트레이션은 오케스트레이터의 ReserveQuota, 코레오그래피는 Content의 SummaryRequested. */
    private static void delayFirstStep(World w, boolean orch) {
        if (orch) ((Orchestration) w).orchestrator.delay.put("ReserveQuota", 3_000L);
        else w.content.delay.put("SummaryRequested", 3_000L);
    }

    /** 실험의 전제: 모든 Saga에서 취소(해제) 메시지가 늦춘 예약 요청보다 먼저 Kafka에 나갔다. 아니면 실험이 성립하지 않는다. */
    private static void cancelSentFirst(World w, List<UUID> sagas, boolean orch) {
        String cancel = orch ? "ReleaseQuota" : "SummaryCancelled";
        String request = orch ? "ReserveQuota" : "SummaryRequested";
        for (var s : sagas) {
            var seq = w.sequence(s).stream().map(x -> x.substring(x.indexOf(':') + 1)).toList();
            if (seq.indexOf(cancel) < 0 || seq.indexOf(request) < 0 || seq.indexOf(cancel) > seq.indexOf(request)) {
                var all = new ArrayList<List<String>>();
                for (var x : sagas) all.add(w.sequence(x));
                throw new IllegalStateException("FAIL precondition: cancel was not sent before the request " + all);
            }
        }
    }

    private static Map<String, Integer> states(World w, List<UUID> sagas) throws Exception {
        var states = new TreeMap<String, Integer>();
        for (var s : sagas) states.merge(w.state(s), 1, Integer::sum);
        return states;
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
