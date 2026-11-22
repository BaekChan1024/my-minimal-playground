package playground.resilience;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 공급자 호출과 실패 분류. 블로그 Content의 OpenAiTextGenerator와 같은 규칙으로 실패를 짧은 코드로 줄인다.
 * 핵심은 "요청이 공급자에 닿았을 수 있는가"다. 연결이 맺어지지 않았으면 UNREACHABLE, 요청이 나간 뒤 응답이 없으면
 * TIMEOUT이나 CONNECTION_LOST다. 뒤의 둘은 공급자가 이미 처리하고 비용을 매겼을 수 있다.
 */
final class ProviderClient {
    static final class Failure extends RuntimeException {
        final String code;
        final long retryAfterMs;

        Failure(String code, long retryAfterMs) {
            super(code, null, false, false);
            this.code = code;
            this.retryAfterMs = retryAfterMs;
        }
    }

    private final HttpClient http;
    private final URI endpoint;
    private final long requestTimeoutMs;

    ProviderClient(String url, long requestTimeoutMs) {
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(300)).build();
        this.endpoint = URI.create(url);
        this.requestTimeoutMs = requestTimeoutMs;
    }

    /** @param idempotencyKey null이면 보내지 않는다 */
    void generate(int jobId, String idempotencyKey) {
        var builder = HttpRequest.newBuilder(endpoint)
                .timeout(Duration.ofMillis(requestTimeoutMs))
                .header("X-Job", Integer.toString(jobId))
                .POST(HttpRequest.BodyPublishers.ofString("{\"post\":" + jobId + "}"));
        if (idempotencyKey != null) builder.header("Idempotency-Key", idempotencyKey);
        var exchange = http.sendAsync(builder.build(), HttpResponse.BodyHandlers.ofString());
        HttpResponse<String> response;
        try {
            response = exchange.get(requestTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            exchange.cancel(true);
            throw new Failure("PROVIDER_TIMEOUT", 0);
        } catch (ExecutionException e) {
            throw new Failure(transportFailure(e.getCause()), 0);
        } catch (InterruptedException e) {
            exchange.cancel(true);
            Thread.currentThread().interrupt();
            throw new Failure("INTERRUPTED", 0);
        }
        int status = response.statusCode();
        if (status == 200) return;
        if (status == 429) {
            long seconds = response.headers().firstValue("retry-after").filter(v -> v.matches("\\d{1,6}"))
                    .map(Long::parseLong).orElse(0L);
            throw new Failure("PROVIDER_RATE_LIMITED", seconds * 1000);
        }
        if (status == 504) throw new Failure("PROVIDER_TIMEOUT", 0);
        if (status == 502) throw new Failure("PROVIDER_CONNECTION_LOST", 0);
        if (status >= 500) throw new Failure("PROVIDER_UNAVAILABLE", 0);
        throw new Failure("PROVIDER_REJECTED", 0);
    }

    static String transportFailure(Throwable cause) {
        if (cause instanceof HttpConnectTimeoutException || cause instanceof ConnectException) {
            return "PROVIDER_UNREACHABLE";
        }
        if (cause instanceof HttpTimeoutException) return "PROVIDER_TIMEOUT";
        if (cause instanceof IOException) return "PROVIDER_CONNECTION_LOST";
        return "PROVIDER_CONNECTION_LOST";
    }
}
