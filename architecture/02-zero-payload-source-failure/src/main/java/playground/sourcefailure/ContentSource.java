package playground.sourcefailure;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 원본 서비스. 글 상태는 메모리 맵이고 조회 API는 실제 loopback HTTP다.
 * 장애는 세 가지로 만든다.
 * - failing: 특정 글 ID의 조회만 500 (그 글의 데이터나 처리에 문제가 있는 경우)
 * - down: 모든 조회와 /health가 503 (원본 서비스 전체 장애)
 * - latencyMs: 모든 응답 전에 서버가 지정 시간만큼 sleep (느린 원본, 합성 지연)
 */
final class ContentSource implements AutoCloseable {
    record Article(long id, long version, String title) {}

    /** 조회에 실패했다. status는 HTTP 상태(연결 실패는 0). */
    static final class LookupFailed extends Exception {
        final int status;
        LookupFailed(int status, String message) {
            super(message);
            this.status = status;
        }
    }

    static final ObjectMapper JSON = new ObjectMapper();

    private final Map<Long, Article> store = new ConcurrentHashMap<>();
    private final Set<Long> failing = ConcurrentHashMap.newKeySet();
    private final AtomicInteger calls = new AtomicInteger();
    private volatile boolean down;
    private volatile long latencyMs;
    private final ExecutorService executor = Executors.newFixedThreadPool(16);
    private final HttpServer server;
    private final ExecutorService clientExecutor = Executors.newFixedThreadPool(16);
    private final HttpClient client;
    private final String base;

    ContentSource() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/internal/contents", this::contents);
        server.createContext("/health", this::health);
        server.setExecutor(executor);
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort();
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).executor(clientExecutor).build();
    }

    Article save(long id, String title) {
        var previous = store.get(id);
        var article = new Article(id, previous == null ? 1 : previous.version() + 1, title);
        store.put(id, article);
        return article;
    }

    void fail(long id, boolean value) {
        if (value) failing.add(id);
        else failing.remove(id);
    }

    void down(boolean value) { down = value; }

    void latency(long ms) { latencyMs = ms; }

    int calls() { return calls.get(); }

    void resetCalls() { calls.set(0); }

    /** GET /internal/contents/{id}. 404는 "보여 줄 글 없음"으로 Optional.empty(). */
    Optional<Article> fetch(long id) throws LookupFailed {
        var response = get("/internal/contents/" + id);
        if (response.statusCode() == 404) return Optional.empty();
        if (response.statusCode() != 200) throw new LookupFailed(response.statusCode(), "http " + response.statusCode() + " id=" + id);
        return Optional.of(read(response.body(), new TypeReference<Article>() {}));
    }

    /** GET /internal/contents?ids=1,2,3. 여러 글을 한 번에 조회한다. 없는 글은 결과에서 빠진다. */
    List<Article> fetchMany(Collection<Long> ids) throws LookupFailed {
        var response = get("/internal/contents?ids=" + ids.stream().map(String::valueOf).collect(Collectors.joining(",")));
        if (response.statusCode() != 200) throw new LookupFailed(response.statusCode(), "http " + response.statusCode() + " bulk");
        return read(response.body(), new TypeReference<List<Article>>() {});
    }

    /** GET /health. 원본 서비스 전체가 응답할 수 있는지만 본다(특정 글의 실패와 무관). */
    boolean healthy() {
        try {
            return get("/health").statusCode() == 200;
        } catch (LookupFailed e) {
            return false;
        }
    }

    private HttpResponse<String> get(String path) throws LookupFailed {
        try {
            return client.send(HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(5)).GET().build(),
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new LookupFailed(0, "io " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LookupFailed(0, "interrupted");
        }
    }

    private static <T> T read(String body, TypeReference<T> type) {
        try {
            return JSON.readValue(body, type);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void contents(HttpExchange exchange) throws IOException {
        try (exchange) {
            calls.incrementAndGet();
            pause();
            if (down) {
                exchange.sendResponseHeaders(503, -1);
                return;
            }
            String path = exchange.getRequestURI().getPath();
            String query = exchange.getRequestURI().getQuery();
            Object body;
            if (path.equals("/internal/contents") && query != null && query.startsWith("ids=")) {
                var found = new ArrayList<Article>();
                for (String raw : query.substring(4).split(",")) {
                    long id = Long.parseLong(raw);
                    if (failing.contains(id)) {
                        exchange.sendResponseHeaders(500, -1);
                        return;
                    }
                    var article = store.get(id);
                    if (article != null) found.add(article);
                }
                body = found;
            } else {
                long id = Long.parseLong(path.substring("/internal/contents/".length()));
                if (failing.contains(id)) {
                    exchange.sendResponseHeaders(500, -1);
                    return;
                }
                var article = store.get(id);
                if (article == null) {
                    exchange.sendResponseHeaders(404, -1);
                    return;
                }
                body = article;
            }
            byte[] bytes = JSON.writeValueAsBytes(body);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
        }
    }

    private void health(HttpExchange exchange) throws IOException {
        try (exchange) {
            exchange.sendResponseHeaders(down ? 503 : 200, -1);
        }
    }

    private void pause() {
        long ms = latencyMs;
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
        executor.shutdownNow();
        client.close();
        clientExecutor.shutdownNow();
    }
}
