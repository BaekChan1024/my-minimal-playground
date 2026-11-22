package playground.sagadur;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import playground.sagadur.Flow.AfterPivot;
import playground.sagadur.Flow.Options;

/**
 * 오케스트레이터가 죽거나, 참여자가 응답하지 않거나, 피벗 뒤 단계의 응답이 사라질 때 Saga가 어떻게 되는지 본다.
 * 실제 임시 Kafka와 임시 PostgreSQL을 쓴다.
 * guided: 실험마다 질문을 보여 주고 Enter를 기다린다. verify: 질문 없이 모두 실행하고 판정한다.
 */
public final class DurabilityLab {
    private static final Duration STABLE = Duration.ofMillis(1_000);
    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static boolean guided;
    private static Scanner in;
    private static int seq;

    public static void main(String[] args) throws Exception {
        if (args.length > 1 || (args.length == 1 && !Set.of("guided", "verify").contains(args[0]))) {
            throw new IllegalArgumentException("guided|verify");
        }
        guided = args.length == 0 || args[0].equals("guided");
        in = new Scanner(System.in);
        if (!pause("""
                오케스트레이션 Saga에서 오케스트레이터가 죽거나, AI 서비스가 응답하지 않거나, 요약 게시 뒤 사용량 확정의
                응답이 사라질 때 무엇이 남는지 봅니다. 실제 임시 Kafka와 임시 PostgreSQL을 사용합니다.""")) return;
        try (var infra = new Infra()) {
            crash(infra);
            participantDown(infra);
            lostReply(infra);
        } catch (Quit q) {
            System.out.println("종료합니다. 임시 브로커와 DB를 정리했습니다.");
            return;
        }
        System.out.println("PASS experiments=3 realBroker=true realPostgres=true");
    }

    private static Flow flow(Infra infra, Options o) throws Exception {
        return new Flow(infra, "f" + (++seq), o);
    }

    // ------------------------------------------------------------------ 1. 오케스트레이터가 죽으면
    static void crash(Infra infra) throws Exception {
        if (!pause("""
                [1/3] Saga 20개가 진행 중일 때 오케스트레이터가 10번째 PostLocked를 처리해 상태를 GENERATING으로 커밋한 직후 죽습니다.
                같은 DB로 다시 띄우면 Saga는 모두 끝날까요? 명령을 Outbox에 쓰는 경우와 커밋한 뒤 바로 보내는 경우를 비교합니다.""")) {
            throw new Quit();
        }
        for (boolean direct : List.of(false, true)) {
            try (var f = flow(infra, new Options(direct, 0, false, AfterPivot.WAIT, 0, false))) {
                f.seed(20, 1_000_000);
                f.orchestrator.crashAfter = "PostLocked";
                f.orchestrator.crashAt = 10;
                f.startAll();
                var sagas = new ArrayList<UUID>();
                for (long p = 1; p <= 20; p++) sagas.add(f.start(p, 500));
                waitUntil(() -> f.orchestrator.dead, "orchestrator crash");
                Thread.sleep(300);
                f.restartOrchestrator();
                f.settle(sagas, STABLE, TIMEOUT);
                var states = f.states(sagas);
                var check = f.invariants();
                int dropped = f.orchestrator.duplicates.get();
                String mode = direct ? "commitThenSend" : "outbox";
                if (direct) {
                    check(states.equals(Map.of("COMPLETED", 19, "GENERATING", 1)), mode + " states " + states);
                    check(!check.ok(), mode + " should leave a reservation and a lock");
                    UUID stuck = null;
                    for (var s : sagas) if ("GENERATING".equals(f.state(s))) stuck = s;
                    var seqOfStuck = f.sequenceOf(stuck);
                    check(!seqOfStuck.contains("orchestrator:Generate"), "stuck saga never sent Generate");
                    System.out.println("PASS crash mode=" + mode + " states=" + states + " redeliveredDroppedByInbox=" + dropped
                            + " invariantsOk=" + check.ok());
                    System.out.println("     stuck=" + seqOfStuck);
                } else {
                    check(states.equals(Map.of("COMPLETED", 20)), mode + " states " + states);
                    check(check.ok(), mode + " invariants " + check.detail());
                    System.out.println("PASS crash mode=" + mode + " states=" + states + " redeliveredDroppedByInbox=" + dropped
                            + " invariantsOk=" + check.ok());
                }
                System.out.println("     " + check.detail());
            }
        }
    }

    // ------------------------------------------------------------------ 2. 참여자가 응답하지 않으면
    static void participantDown(Infra infra) throws Exception {
        if (!pause("""
                [2/3] AI 서비스가 멈춘 채 Saga 10개를 시작하고, 1초 뒤 같은 글에 Saga 10개를 더 시작합니다. 2초 뒤 AI가 돌아옵니다.
                시한이 없으면 글 잠금은 어떻게 될까요? 0.5초 시한으로 보상하면, 돌아온 AI가 처리한 생성 비용은 누가 기록할까요?""")) {
            throw new Quit();
        }
        var variants = new LinkedHashMap<String, Options>();
        variants.put("noDeadline", Options.plain());
        variants.put("deadline", new Options(false, 500, false, AfterPivot.WAIT, 0, false));
        variants.put("deadline+lateCharge", new Options(false, 500, true, AfterPivot.WAIT, 0, false));
        for (var v : variants.entrySet()) {
            String name = v.getKey();
            try (var f = flow(infra, v.getValue())) {
                f.seed(10, 1_000_000);
                f.ai.pause();
                f.startAll();
                var first = new ArrayList<UUID>();
                var second = new ArrayList<UUID>();
                for (long p = 1; p <= 10; p++) first.add(f.start(p, 500));
                Thread.sleep(1_000);
                for (long p = 1; p <= 10; p++) second.add(f.start(p, 500));
                Thread.sleep(1_000);
                var all = new ArrayList<UUID>(first);
                all.addAll(second);
                var at2s = f.states(all);
                long lockedAt2s = f.content.count("SELECT count(*) FROM post WHERE locked_by IS NOT NULL");
                long busy = f.trace.stream().filter(s -> s.msg().type().equals("PostLockFailed")).count();
                f.ai.resume();
                f.settle(all, STABLE, TIMEOUT);
                var fin = f.states(all);
                var check = f.invariants();
                long wasted = wastedGenerations(f, all);
                switch (name) {
                    case "noDeadline" -> {
                        check(lockedAt2s == 10 && busy == 10, "locks held while stuck");
                        check(f.states(first).equals(Map.of("COMPLETED", 10)), "first wave completes after AI returns");
                        check(check.ok() && wasted == 0, "no deadline invariants");
                    }
                    case "deadline" -> {
                        check(lockedAt2s == 0 && busy == 0 && at2s.equals(Map.of("COMPENSATED", 20)), "deadline releases " + at2s);
                        check(!check.ok() && wasted == 20, "late generations are not charged " + check.detail());
                    }
                    default -> {
                        check(lockedAt2s == 0 && busy == 0 && at2s.equals(Map.of("COMPENSATED", 20)), "deadline releases " + at2s);
                        check(check.ok() && wasted == 20 && f.lateCharges.get() == 20, "late charges recorded " + check.detail());
                    }
                }
                System.out.println("PASS participantDown variant=" + name + " at2s=" + at2s + " lockedAt2s=" + lockedAt2s
                        + " busy=" + busy + " final=" + fin + " wastedGenerations=" + wasted + " lateCharges=" + f.lateCharges.get()
                        + " invariantsOk=" + check.ok());
                System.out.println("     " + check.detail());
            }
        }
    }

    /** AI가 생성(과금)했지만 Saga가 완료되지 않은 건수. */
    private static long wastedGenerations(Flow f, List<UUID> sagas) throws Exception {
        var generated = new HashSet<UUID>();
        try (var c = f.ai.db.getConnection(); var ps = c.prepareStatement("SELECT saga FROM job"); var rs = ps.executeQuery()) {
            while (rs.next()) generated.add((UUID) rs.getObject(1));
        }
        long wasted = 0;
        for (var s : sagas) if (generated.contains(s) && !"COMPLETED".equals(f.state(s))) wasted++;
        return wasted;
    }

    // ------------------------------------------------------------------ 3. 피벗 뒤 응답이 사라지면
    static void lostReply(Infra infra) throws Exception {
        if (!pause("""
                [3/3] 요약을 게시한 뒤(피벗 뒤) Usage가 사용량을 확정했지만 그 응답이 사라집니다. Saga 5개.
                기다리기만 하면? 0.2초 뒤 같은 명령을 다시 보내면? 그때 Usage에 "아직 예약 중일 때만" 조건이 없으면? 보상하면?""")) {
            throw new Quit();
        }
        var variants = new LinkedHashMap<String, Options>();
        variants.put("wait", Options.plain());
        variants.put("retry", new Options(false, 0, false, AfterPivot.RETRY, 200, false));
        variants.put("retry+naiveUsage", new Options(false, 0, false, AfterPivot.RETRY, 200, true));
        variants.put("compensate", new Options(false, 0, false, AfterPivot.COMPENSATE, 200, false));
        for (var v : variants.entrySet()) {
            String name = v.getKey();
            try (var f = flow(infra, v.getValue())) {
                f.seed(5, 1_000_000);
                f.startAll();
                var sagas = new ArrayList<UUID>();
                // 응답 유실 대상은 시작한 직후 등록한다. 사용량 확정은 한참 뒤라 늦지 않는다.
                for (long p = 1; p <= 5; p++) {
                    var id = f.start(p, 500);
                    f.dropFirstCommitReply.add(id);
                    sagas.add(id);
                }
                f.settle(sagas, STABLE, TIMEOUT);
                var states = f.states(sagas);
                var check = f.invariants();
                long committedRows = f.usage.count("SELECT count(*) FROM reservation WHERE status = 'COMMITTED'");
                long summaries = f.content.count("SELECT count(*) FROM post WHERE summary IS NOT NULL");
                check(f.droppedReplies.get() == 5, name + " dropped replies");
                switch (name) {
                    case "wait" -> check(states.equals(Map.of("COMMITTING", 5)) && check.ok() && committedRows == 5, "wait stuck");
                    case "retry" -> check(states.equals(Map.of("COMPLETED", 5)) && check.ok() && f.resends.get() >= 5, "retry completes");
                    case "retry+naiveUsage" -> check(states.equals(Map.of("COMPLETED", 5)) && !check.ok(), "naive double charge");
                    default -> check(states.equals(Map.of("COMPENSATED", 5)) && check.ok() && summaries == 0 && committedRows == 5,
                            "compensate removes summaries but keeps the charge");
                }
                System.out.println("PASS lostReply variant=" + name + " states=" + states + " resends=" + f.resends.get()
                        + " committedReservations=" + committedRows + " summaries=" + summaries + " invariantsOk=" + check.ok());
                System.out.println("     " + check.detail());
            }
        }
    }

    // ------------------------------------------------------------------ helpers
    private interface Condition {
        boolean met() throws Exception;
    }

    private static void waitUntil(Condition c, String what) throws Exception {
        long until = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < until) {
            if (c.met()) return;
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
