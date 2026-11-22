package playground.resilience;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import playground.resilience.FakeProvider.Action;
import playground.resilience.Worker.Options;
import playground.resilience.Worker.RetryPolicy;

/**
 * AI 공급자가 실패할 때 작업 대기열이 어떻게 버티는지 네 가지로 실험한다.
 * 공급자는 로컬 HTTP 서버로 흉내 내고, 작업은 임시 PostgreSQL에 둔다. 시간은 실습에 맞게 줄였다(초 단위).
 * guided: 실험마다 질문을 보여 주고 Enter를 기다린다. verify: 질문 없이 모두 실행하고 판정한다.
 */
public final class ResilienceLab {
    private static boolean guided;
    private static Scanner in;

    public static void main(String[] args) throws Exception {
        if (args.length > 1 || (args.length == 1 && !Set.of("guided", "verify").contains(args[0]))) {
            System.err.println("Usage: ResilienceLab [guided|verify]");
            System.exit(2);
        }
        guided = args.length == 0 || args[0].equals("guided");
        in = new Scanner(System.in);
        if (!pause("""
                AI 요약 작업을 대기열에 넣고, 가짜 AI 공급자를 네 가지 방식으로 실패시킵니다.
                재시도·Circuit Breaker·Bulkhead를 하나씩 켜며 결과를 비교합니다. 임시 PostgreSQL을 사용합니다.""")) return;
        try (var db = new Jobs.Server()) {
            outage(db);
            lostResponse(db);
            slowSuccess(db);
            burst(db);
        } catch (Quit q) {
            System.out.println("종료합니다. 임시 DB를 정리했습니다.");
            return;
        }
        System.out.println("PASS experiments=4 realPostgres=true");
    }

    // ------------------------------------------------------------------ 1. 공급자가 2.5초 동안 503
    static void outage(Jobs.Server db) throws Exception {
        if (!pause("""
                [1/4] 작업 30개가 기다리는데 공급자가 2.5초 동안 503을 줍니다. 재시도하지 않을 때, 재시도만 할 때(최대 3번,
                0.2초부터 두 배), 재시도에 Circuit Breaker를 더할 때 각각 몇 개가 끝나고 공급자는 몇 번 불릴까요?""")) {
            throw new Quit();
        }
        var variants = new LinkedHashMap<String, Options>();
        variants.put("noRetry", Options.of(2, RetryPolicy.NONE));
        variants.put("retry", Options.of(2, RetryPolicy.SAFE_ONLY));
        variants.put("retry+breaker", Options.of(2, RetryPolicy.SAFE_ONLY).withBreaker(Worker.contentBreaker()));
        for (var v : variants.entrySet()) {
            try (var jobs = db.fresh(); var provider = new FakeProvider()) {
                for (int i = 1; i <= 30; i++) jobs.submit(i, "AUTHOR");
                provider.latencyMs = 50;
                long outageEnds = provider.elapsedMs() + 2500;
                provider.script = (job, n) -> provider.elapsedMs() < outageEnds ? Action.UNAVAILABLE : Action.OK;
                long t0 = System.nanoTime();
                try (var worker = new Worker(jobs, new ProviderClient(provider.url(), 2000), v.getValue())) {
                    worker.start();
                    check(worker.awaitDrained(20_000), "outage drained " + v.getKey());
                    long ms = (System.nanoTime() - t0) / 1_000_000;
                    var out = jobs.outcomes();
                    String name = v.getKey();
                    switch (name) {
                        case "noRetry" -> {
                            check(out.equals(Map.of("FAILED:PROVIDER_UNAVAILABLE", 30L)), "noRetry all failed " + out);
                            check(provider.calls.get() == 30, "noRetry one call each " + provider.calls.get());
                        }
                        case "retry" -> {
                            check(out.equals(Map.of("FAILED:PROVIDER_UNAVAILABLE", 30L)), "retry all failed " + out);
                            check(provider.unavailable.get() == 120, "retry four calls each " + provider.unavailable.get());
                        }
                        default -> {
                            check(out.equals(Map.of("SUCCEEDED", 30L)), "breaker all succeeded " + out);
                            check(provider.unavailable.get() <= 12, "breaker few calls during outage "
                                    + provider.unavailable.get());
                        }
                    }
                    System.out.println("PASS outage variant=" + name + " outcomes=" + out
                            + " callsDuringOutage=" + provider.unavailable.get() + " totalCalls=" + provider.calls.get()
                            + " retriesUsed=" + jobs.sumRetries() + " drainedMs=" + ms
                            + (worker.breaker == null ? "" : " breakerTransitions=" + worker.transitions.size()));
                }
            }
        }
    }

    // ------------------------------------------------------------------ 2. 처리한 뒤 응답이 사라짐
    static void lostResponse(Jobs.Server db) throws Exception {
        if (!pause("""
                [2/4] 공급자가 요청을 처리하고 비용을 매긴 뒤 응답을 잃습니다(5개는 시간 초과, 5개는 연결 끊김). 이 두 실패를
                다시 시도하지 않으면, 다시 시도하면, 다시 시도하되 요청에 멱등키를 실으면 비용은 몇 번 매겨질까요?""")) {
            throw new Quit();
        }
        var variants = new LinkedHashMap<String, Options>();
        variants.put("safeOnly", Options.of(2, RetryPolicy.SAFE_ONLY));
        variants.put("retryAll", Options.of(2, RetryPolicy.ALL));
        variants.put("retryAll+idempotencyKey", Options.of(2, RetryPolicy.ALL).withIdempotencyKey());
        for (var v : variants.entrySet()) {
            try (var jobs = db.fresh(); var provider = new FakeProvider()) {
                for (int i = 1; i <= 10; i++) jobs.submit(i, "AUTHOR");
                provider.script = (job, n) -> n > 1 ? Action.OK
                        : job % 2 == 1 ? Action.PROCESS_THEN_STALL : Action.PROCESS_THEN_DROP;
                try (var worker = new Worker(jobs, new ProviderClient(provider.url(), 500), v.getValue())) {
                    worker.start();
                    check(worker.awaitDrained(20_000), "lost drained " + v.getKey());
                    var out = jobs.outcomes();
                    int charged = provider.totalCharged();
                    long twice = provider.chargedMoreThanOnce();
                    switch (v.getKey()) {
                        case "safeOnly" -> {
                            check(out.equals(Map.of("FAILED:PROVIDER_CONNECTION_LOST", 5L, "FAILED:PROVIDER_TIMEOUT", 5L)),
                                    "safeOnly fails both kinds " + out);
                            check(charged == 10 && twice == 0, "safeOnly charged once " + charged);
                        }
                        case "retryAll" -> {
                            check(out.equals(Map.of("SUCCEEDED", 10L)), "retryAll completes " + out);
                            check(charged == 20 && twice == 10, "retryAll charges twice " + charged);
                        }
                        default -> {
                            check(out.equals(Map.of("SUCCEEDED", 10L)), "key completes " + out);
                            check(charged == 10 && twice == 0, "key charges once " + charged);
                        }
                    }
                    System.out.println("PASS lostResponse variant=" + v.getKey() + " outcomes=" + out
                            + " providerCalls=" + provider.calls.get() + " charged=" + charged
                            + " jobsChargedTwice=" + twice);
                }
            }
        }
    }

    // ------------------------------------------------------------------ 3. 느리지만 성공하는 호출
    static void slowSuccess(Jobs.Server db) throws Exception {
        var defaults = CircuitBreakerConfig.ofDefaults();
        if (!pause("""
                [3/4] 공급자가 실패 없이 1.2초씩 걸려 답합니다(긴 번역처럼). 실패율만 바꾸고 느린 호출 규칙은 Resilience4j
                기본값(%s%% 이상이 %s초보다 느리면 연다)을 둔 차단기는 어떻게 될까요? 실습에서는 %s초를 1초로 줄입니다."""
                .formatted((int) defaults.getSlowCallRateThreshold(),
                        defaults.getSlowCallDurationThreshold().toSeconds(),
                        defaults.getSlowCallDurationThreshold().toSeconds()))) {
            throw new Quit();
        }
        check(defaults.getSlowCallRateThreshold() == 100f
                && defaults.getSlowCallDurationThreshold().toSeconds() == 60, "resilience4j slow-call defaults");
        var variants = new LinkedHashMap<String, Options>();
        variants.put("slowCallDefaults", Options.of(1, RetryPolicy.SAFE_ONLY).withBreaker(Worker.slowCallDefaultsScaled()));
        variants.put("slowCallOff", Options.of(1, RetryPolicy.SAFE_ONLY).withBreaker(Worker.contentBreaker()));
        for (var v : variants.entrySet()) {
            try (var jobs = db.fresh(); var provider = new FakeProvider()) {
                for (int i = 1; i <= 6; i++) jobs.submit(i, "AUTHOR");
                provider.latencyMs = 1200;
                long t0 = System.nanoTime();
                try (var worker = new Worker(jobs, new ProviderClient(provider.url(), 3000), v.getValue())) {
                    worker.start();
                    check(worker.awaitDrained(30_000), "slow drained " + v.getKey());
                    long ms = (System.nanoTime() - t0) / 1_000_000;
                    var out = jobs.outcomes();
                    var metrics = worker.breaker.getMetrics();
                    check(out.equals(Map.of("SUCCEEDED", 6L)), "slow all succeed " + out);
                    if (v.getKey().equals("slowCallDefaults")) {
                        check(worker.transitions.contains("State transition from CLOSED to OPEN"),
                                "defaults open on slow successes " + worker.transitions);
                        check(metrics.getNumberOfFailedCalls() == 0, "no failures");
                    } else {
                        check(worker.transitions.isEmpty(), "fixed stays closed " + worker.transitions);
                    }
                    System.out.println("PASS slowSuccess variant=" + v.getKey() + " outcomes=" + out
                            + " failedCalls=" + metrics.getNumberOfFailedCalls()
                            + " opened=" + worker.transitions.stream().filter(t -> t.endsWith("to OPEN")).count()
                            + " state=" + worker.breaker.getState() + " drainedMs=" + ms);
                }
            }
        }
    }

    // ------------------------------------------------------------------ 4. 자동 요약이 몰릴 때 저자의 요청
    static void burst(Jobs.Server db) throws Exception {
        if (!pause("""
                [4/4] 글 20개가 한꺼번에 공개되어 자동 요약 20개가 대기열에 들어가고, 0.1초 뒤 저자가 요약을 요청합니다.
                호출은 0.3초씩 걸립니다. 스레드 1개, 2개, 2개 중 자동 요약은 1개까지로 묶으면 저자는 얼마나 기다릴까요?""")) {
            throw new Quit();
        }
        var variants = new LinkedHashMap<String, Options>();
        variants.put("oneThread", Options.of(1, RetryPolicy.SAFE_ONLY));
        variants.put("twoThreads", Options.of(2, RetryPolicy.SAFE_ONLY));
        variants.put("twoThreads+systemSlot1", Options.of(2, RetryPolicy.SAFE_ONLY).withSystemSlots(1));
        var waits = new LinkedHashMap<String, Long>();
        for (var v : variants.entrySet()) {
            try (var jobs = db.fresh(); var provider = new FakeProvider()) {
                provider.latencyMs = 300;
                for (int i = 1; i <= 20; i++) jobs.submit(i, "SYSTEM");
                try (var worker = new Worker(jobs, new ProviderClient(provider.url(), 3000), v.getValue())) {
                    worker.start();
                    Thread.sleep(100);
                    jobs.submit(100, "AUTHOR");
                    check(worker.awaitDrained(30_000), "burst drained " + v.getKey());
                    long wait = jobs.waitMs(100);
                    long systemSpan = jobs.spanMs("SYSTEM");
                    waits.put(v.getKey(), wait);
                    check(jobs.outcomes().equals(Map.of("SUCCEEDED", 21L)), "burst all succeed");
                    System.out.println("PASS burst variant=" + v.getKey() + " authorWaitMs=" + wait
                            + " systemJobsDoneAfterMs=" + systemSpan + " maxConcurrentCalls=" + provider.maxInFlight.get());
                }
            }
        }
        check(waits.get("twoThreads+systemSlot1") < 500, "author does not wait behind system jobs " + waits);
        check(waits.get("oneThread") > waits.get("twoThreads") && waits.get("twoThreads") > 1000,
                "more threads alone only shorten the wait " + waits);
    }

    // ------------------------------------------------------------------ 도우미
    static void check(boolean ok, String what) {
        if (!ok) {
            System.out.println("FAIL " + what);
            System.exit(1);
        }
    }

    private static boolean pause(String text) {
        System.out.println();
        System.out.println(text);
        if (!guided) return true;
        System.out.print("예상을 적어 두고 Enter(종료는 q): ");
        if (!in.hasNextLine()) return false;
        return !in.nextLine().trim().equalsIgnoreCase("q");
    }

    private static final class Quit extends RuntimeException {
        Quit() {
            super(null, null, false, false);
        }
    }
}
