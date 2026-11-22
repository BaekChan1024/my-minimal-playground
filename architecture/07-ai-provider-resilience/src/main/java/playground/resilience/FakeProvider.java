package playground.resilience;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AI 공급자를 흉내 내는 로컬 HTTP 서버. 실제 공급자는 호출하지 않는다.
 *
 * 요청마다 {@link Script}가 정한 대로 답한다. "처리했다"는 것은 공급자가 생성을 마치고 비용을 매겼다는 뜻이고,
 * 정상 응답과 "처리한 뒤 응답을 잃음"(시간 초과·연결 끊김)이 여기에 해당한다. 503·429는 처리하지 않은 것이다.
 * 요청에 Idempotency-Key가 있고 같은 키로 이미 처리했다면, 다시 처리하지 않고 저장한 결과를 돌려준다.
 */
final class FakeProvider implements AutoCloseable {
    enum Action {
        /** 처리하고 200으로 답한다. */
        OK,
        /** 처리하지 않고 503. */
        UNAVAILABLE,
        /** 처리하지 않고 429 + Retry-After. */
        RATE_LIMITED,
        /** 처리한 뒤 클라이언트의 시간 초과보다 오래 붙잡고 있다가 끊는다. */
        PROCESS_THEN_STALL,
        /** 처리한 뒤 응답 없이 연결을 끊는다. */
        PROCESS_THEN_DROP
    }

    /** jobId와 그 작업의 몇 번째 호출인지(1부터)로 응답을 정한다. */
    interface Script {
        Action decide(int jobId, int callOfJob);
    }

    private final HttpServer server;
    private final long startNanos = System.nanoTime();
    volatile Script script = (job, n) -> Action.OK;
    volatile long latencyMs = 0;
    volatile long stallMs = 1500;
    volatile int retryAfterSeconds = 1;

    final AtomicInteger calls = new AtomicInteger();
    final AtomicInteger unavailable = new AtomicInteger();
    final AtomicInteger rateLimited = new AtomicInteger();
    /** 비용을 매긴(처리한) 횟수, 작업별. */
    final Map<Integer, AtomicInteger> charged = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger> callsOfJob = new ConcurrentHashMap<>();
    private final Map<String, Boolean> processedKeys = new ConcurrentHashMap<>();
    /** 동시에 처리 중인 요청 수의 최댓값. */
    final AtomicInteger inFlight = new AtomicInteger();
    final AtomicInteger maxInFlight = new AtomicInteger();

    FakeProvider() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newCachedThreadPool(r -> {
            var t = new Thread(r, "fake-provider");
            t.setDaemon(true);
            return t;
        }));
        server.createContext("/v1/generate", this::handle);
        server.start();
    }

    String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/v1/generate";
    }

    long elapsedMs() {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    int totalCharged() {
        return charged.values().stream().mapToInt(AtomicInteger::get).sum();
    }

    long chargedMoreThanOnce() {
        return charged.values().stream().filter(c -> c.get() > 1).count();
    }

    private void handle(HttpExchange exchange) throws IOException {
        int now = inFlight.incrementAndGet();
        maxInFlight.accumulateAndGet(now, Math::max);
        try {
            exchange.getRequestBody().readAllBytes();
            calls.incrementAndGet();
            int job = Integer.parseInt(exchange.getRequestHeaders().getFirst("X-Job"));
            int n = callsOfJob.computeIfAbsent(job, k -> new AtomicInteger()).incrementAndGet();
            String key = exchange.getRequestHeaders().getFirst("Idempotency-Key");
            if (key != null && processedKeys.containsKey(key)) {
                // 이미 처리한 요청: 다시 생성하지 않고 결과만 돌려준다(비용 없음).
                respond(exchange, 200, "{\"text\":\"cached\"}");
                return;
            }
            Action action = script.decide(job, n);
            switch (action) {
                case UNAVAILABLE -> {
                    unavailable.incrementAndGet();
                    respond(exchange, 503, "{}");
                }
                case RATE_LIMITED -> {
                    rateLimited.incrementAndGet();
                    exchange.getResponseHeaders().add("Retry-After", Integer.toString(retryAfterSeconds));
                    respond(exchange, 429, "{}");
                }
                case OK -> {
                    sleep(latencyMs);
                    charge(job, key);
                    respond(exchange, 200, "{\"text\":\"summary of " + job + "\"}");
                }
                case PROCESS_THEN_STALL -> {
                    charge(job, key);
                    sleep(stallMs);
                    exchange.close();
                }
                case PROCESS_THEN_DROP -> {
                    charge(job, key);
                    // 헤더를 보내지 않고 닫는다: 클라이언트는 응답 없이 연결이 끊긴 것을 본다.
                    exchange.close();
                }
            }
        } finally {
            inFlight.decrementAndGet();
        }
    }

    private void charge(int job, String key) {
        charged.computeIfAbsent(job, k -> new AtomicInteger()).incrementAndGet();
        if (key != null) processedKeys.put(key, true);
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (var out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static void sleep(long ms) {
        if (ms <= 0) return;
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
