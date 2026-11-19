package playground.events;

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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 원본 서비스. 상태는 메모리에 두고, 소비자가 부를 조회 API는 실제 loopback HTTP로 연다.
 * 읽기는 항상 replica 맵에서 한다. 평소에는 쓰기와 함께 replica도 갱신되고,
 * lagReplica() 이후에는 syncReplica()를 부를 때까지 replica가 이전 상태에 머문다.
 * DB 복제를 재현한 것이 아니라 "조회가 커밋보다 늦은 상태를 돌려주는" 조건만 흉내 낸다.
 */
final class ContentSource implements AutoCloseable {
    /** 조회 API가 일시적으로 응답하지 못했다. 소비자는 나중에 다시 시도해야 한다. */
    static final class Unavailable extends Exception {
        Unavailable(String message) { super(message); }
    }

    private final Map<Long, Article> primary = new ConcurrentHashMap<>();
    private final Map<Long, Article> replica = new ConcurrentHashMap<>();
    private final AtomicInteger calls = new AtomicInteger();
    private volatile boolean down;
    private volatile boolean replicaLagging;
    private final ExecutorService executor = Executors.newFixedThreadPool(4);
    private final HttpServer server;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final String baseUrl;

    ContentSource() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/internal/contents/", this::handle);
        server.setExecutor(executor);
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/internal/contents/";
    }

    /** 저장할 때마다 version이 1 증가한다. 삭제도 하나의 변경(version)으로 남긴다. */
    Article save(long id, String status, String title, String body) {
        var previous = primary.get(id);
        var article = new Article(id, previous == null ? 1 : previous.version() + 1, status, title, body);
        primary.put(id, article);
        if (!replicaLagging) replica.put(id, article);
        return article;
    }

    Article delete(long id) {
        return save(id, Article.DELETED, null, null);
    }

    void lagReplica() { replicaLagging = true; }

    void syncReplica() {
        replica.putAll(primary);
        replicaLagging = false;
    }

    void down(boolean value) { down = value; }

    int calls() { return calls.get(); }

    void resetCalls() { calls.set(0); }

    /** 소비자 쪽 클라이언트. 404는 "지금 보여 줄 글이 없음", 5xx는 일시 장애로 해석한다. */
    Optional<Article> fetch(long id) throws Unavailable {
        HttpResponse<String> response;
        try {
            response = client.send(HttpRequest.newBuilder(URI.create(baseUrl + id)).timeout(Duration.ofSeconds(2)).GET().build(),
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new Unavailable("io: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new Unavailable("interrupted");
        }
        if (response.statusCode() == 404) return Optional.empty();
        if (response.statusCode() != 200) throw new Unavailable("http " + response.statusCode());
        try {
            return Optional.of(Events.JSON.readValue(response.body(), Article.class));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            calls.incrementAndGet();
            if (down) {
                exchange.sendResponseHeaders(503, -1);
                return;
            }
            long id = Long.parseLong(exchange.getRequestURI().getPath().substring("/internal/contents/".length()));
            var article = replica.get(id);
            if (article == null || article.status().equals(Article.DELETED)) {
                exchange.sendResponseHeaders(404, -1);
                return;
            }
            byte[] body = Events.JSON.writeValueAsBytes(article);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
        }
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
        client.close();
    }
}
