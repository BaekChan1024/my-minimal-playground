package playground.resilience;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 작업을 하나씩 집어 공급자를 부르는 워커. 스레드마다 같은 루프를 돈다.
 *
 * 세 장치는 블로그 Content의 AiJobWorker와 같은 순서로 붙는다.
 * <ol>
 *   <li>Circuit Breaker: 작업을 <b>집기 전에</b> 허가를 묻는다. 열려 있으면 아무것도 집지 않는다.</li>
 *   <li>Bulkhead: 시스템 작업(자동 요약)에 쓸 자리를 얻지 못하면 시스템 작업을 건너뛰고 집는다.</li>
 *   <li>재시도: 정책이 허락한 실패면 대기열로 되돌리고 다음 시각을 행에 적는다.</li>
 * </ol>
 */
final class Worker implements AutoCloseable {
    /** 어떤 실패를 다시 시도할까. */
    enum RetryPolicy {
        /** 재시도하지 않는다. */
        NONE,
        /** 공급자가 처리하지 않았다고 확신할 수 있는 실패만(연결 실패·5xx·429). Content의 규칙. */
        SAFE_ONLY,
        /** 시간 초과와 끊긴 연결까지 모두. */
        ALL
    }

    static final Set<String> SAFE = Set.of("PROVIDER_UNREACHABLE", "PROVIDER_UNAVAILABLE", "PROVIDER_RATE_LIMITED");
    static final Set<String> UNWELL = Set.of(
            "PROVIDER_UNREACHABLE", "PROVIDER_UNAVAILABLE", "PROVIDER_RATE_LIMITED", "PROVIDER_TIMEOUT",
            "PROVIDER_CONNECTION_LOST");

    record Options(
            int threads,
            RetryPolicy retry,
            int maxRetries,
            long initialBackoffMs,
            long maxBackoffMs,
            CircuitBreakerConfig breaker,
            Integer systemSlots,
            boolean idempotencyKey) {

        static Options of(int threads, RetryPolicy retry) {
            return new Options(threads, retry, 3, 200, 2000, null, null, false);
        }

        Options withBreaker(CircuitBreakerConfig config) {
            return new Options(threads, retry, maxRetries, initialBackoffMs, maxBackoffMs, config, systemSlots,
                    idempotencyKey);
        }

        Options withSystemSlots(int slots) {
            return new Options(threads, retry, maxRetries, initialBackoffMs, maxBackoffMs, breaker, slots,
                    idempotencyKey);
        }

        Options withIdempotencyKey() {
            return new Options(threads, retry, maxRetries, initialBackoffMs, maxBackoffMs, breaker, systemSlots, true);
        }
    }

    /**
     * Content가 쓰는 설정(시간만 실습에 맞게 줄임): 최근 6번 중 3번 이상 판단할 수 있을 때 실패가 절반 이상이면 열고,
     * 1초 뒤 한 번의 시험 호출로 판단한다. 느린 성공만으로는 열리지 않게 느린 호출 기준을 아주 길게 둔다.
     */
    static CircuitBreakerConfig contentBreaker() {
        return base().slowCallRateThreshold(100).slowCallDurationThreshold(Duration.ofHours(1)).build();
    }

    /** 위와 같되 느린 호출 규칙은 Resilience4j 기본값(100%, 60초)을 쓰고, 60초만 1초로 줄인다. */
    static CircuitBreakerConfig slowCallDefaultsScaled() {
        var defaults = CircuitBreakerConfig.ofDefaults();
        return base().slowCallRateThreshold(defaults.getSlowCallRateThreshold())
                .slowCallDurationThreshold(Duration.ofSeconds(1))
                .build();
    }

    private static CircuitBreakerConfig.Builder base() {
        return CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(6)
                .minimumNumberOfCalls(3)
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(1))
                .permittedNumberOfCallsInHalfOpenState(1)
                .recordException(e -> e instanceof ProviderClient.Failure f && UNWELL.contains(f.code))
                .ignoreException(e -> !(e instanceof ProviderClient.Failure f) || f.code.equals("INTERRUPTED"));
    }

    private final Jobs jobs;
    private final ProviderClient client;
    private final Options options;
    final CircuitBreaker breaker;
    private final Bulkhead systemSlots;
    private final List<Thread> threads = new ArrayList<>();
    private volatile boolean stop;

    final AtomicInteger notPermitted = new AtomicInteger();
    final List<String> transitions = new CopyOnWriteArrayList<>();

    Worker(Jobs jobs, ProviderClient client, Options options) {
        this.jobs = jobs;
        this.client = client;
        this.options = options;
        this.breaker = options.breaker() == null ? null : CircuitBreaker.of("provider", options.breaker());
        if (breaker != null) {
            breaker.getEventPublisher().onStateTransition(e -> transitions.add(e.getStateTransition().toString()));
        }
        this.systemSlots = options.systemSlots() == null
                ? null
                : Bulkhead.of("system", BulkheadConfig.custom()
                        .maxConcurrentCalls(options.systemSlots())
                        .maxWaitDuration(Duration.ZERO)
                        .build());
    }

    void start() {
        for (int i = 0; i < options.threads(); i++) {
            var t = Thread.ofPlatform().name("worker-" + (i + 1)).start(this::loop);
            threads.add(t);
        }
    }

    private void loop() {
        while (!stop) {
            try {
                if (!runOnce()) Thread.sleep(20);
            } catch (InterruptedException e) {
                return;
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /** false: 할 일이 없었거나 차단기가 허락하지 않았다. */
    boolean runOnce() throws SQLException {
        if (breaker != null && !breaker.tryAcquirePermission()) {
            notPermitted.incrementAndGet();
            return false;
        }
        boolean systemSlot = systemSlots == null || systemSlots.tryAcquirePermission();
        var claimed = jobs.claim(systemSlot);
        if (claimed.isEmpty()) {
            if (breaker != null) breaker.releasePermission();
            if (systemSlots != null && systemSlot) systemSlots.releasePermission();
            return false;
        }
        var claim = claimed.get();
        if (systemSlots != null && systemSlot && !claim.system()) {
            systemSlots.releasePermission();
            systemSlot = false;
        }
        try {
            call(claim);
        } finally {
            if (systemSlots != null && systemSlot) systemSlots.onComplete();
        }
        return true;
    }

    private void call(Jobs.Claim claim) throws SQLException {
        long started = System.nanoTime();
        try {
            client.generate(claim.id(), options.idempotencyKey() ? "job-" + claim.id() : null);
            if (breaker != null) breaker.onSuccess(System.nanoTime() - started, TimeUnit.NANOSECONDS);
            jobs.succeed(claim);
        } catch (ProviderClient.Failure f) {
            if (breaker != null) breaker.onError(System.nanoTime() - started, TimeUnit.NANOSECONDS, f);
            if (repeatable(f.code) && claim.retries() < options.maxRetries()) {
                jobs.retryLater(claim, backoffMs(claim.retries(), f.retryAfterMs));
            } else {
                jobs.fail(claim, f.code);
            }
        }
    }

    private boolean repeatable(String code) {
        return switch (options.retry()) {
            case NONE -> false;
            case SAFE_ONLY -> SAFE.contains(code);
            case ALL -> UNWELL.contains(code);
        };
    }

    /** 두 배씩 늘고 절반은 무작위(equal jitter). 공급자가 더 오래 기다리라고 하면 그만큼. */
    long backoffMs(int retries, long retryAfterMs) {
        long step = Math.min(options.maxBackoffMs(), options.initialBackoffMs() << Math.min(retries, 20));
        long jittered = step / 2 + ThreadLocalRandom.current().nextLong(step - step / 2 + 1);
        return Math.max(jittered, Math.min(retryAfterMs, options.maxBackoffMs()));
    }

    /** 대기열이 빌 때까지(또는 limitMs까지) 기다린다. 끝까지 비지 않으면 false. */
    boolean awaitDrained(long limitMs) throws Exception {
        long end = System.nanoTime() + limitMs * 1_000_000;
        while (System.nanoTime() < end) {
            if (jobs.unfinished() == 0) return true;
            Thread.sleep(25);
        }
        return false;
    }

    @Override
    public void close() throws InterruptedException {
        stop = true;
        for (var t : threads) t.join(5000);
    }
}
