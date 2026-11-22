package playground.redisfail;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * 로컬에 설치된 redis-server를 임시 디렉터리·임의 포트로 띄운다. 기본 사용자에는 블로그 운영과 같은 ACL
 * (+@all -@dangerous -@admin)을 걸고, 관측용 사용자 lab(실행마다 새 비밀번호)을 따로 둔다.
 * kill()은 SIGKILL이다. restart()는 같은 설정·같은 디렉터리로 다시 띄우므로 AOF가 있으면 다시 읽는다.
 */
final class RedisProcess implements AutoCloseable {
    static final String BINARY = locate();
    static final String VERSION = version(BINARY);

    final int port;
    private final Path dir;
    private final String adminPassword = UUID.randomUUID().toString();
    private Process process;
    private RedisClient adminClient;
    private StatefulRedisConnection<String, String> adminConnection;

    /** appendonly: "no" | "everysec" | "always". */
    RedisProcess(String appendonly) throws Exception {
        dir = Files.createTempDirectory("redis-lab");
        port = freePort();
        Files.writeString(dir.resolve("users.acl"), """
                user default on nopass ~* &* +@all -@dangerous -@admin
                user lab on >%s ~* &* +@all
                """.formatted(adminPassword));
        Files.writeString(dir.resolve("redis.conf"), """
                bind 127.0.0.1
                port %d
                protected-mode no
                dir %s
                save ""
                appendonly %s
                appendfsync %s
                maxmemory 0
                maxmemory-policy noeviction
                aclfile %s
                """.formatted(port, dir, appendonly.equals("no") ? "no" : "yes",
                        appendonly.equals("no") ? "everysec" : appendonly, dir.resolve("users.acl")));
        start();
    }

    void start() throws Exception {
        process = new ProcessBuilder(BINARY, dir.resolve("redis.conf").toString())
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(dir.resolve("redis.log").toFile()))
                .start();
        long until = System.nanoTime() + 10_000_000_000L;
        while (System.nanoTime() < until) {
            if (!process.isAlive()) {
                throw new IllegalStateException("redis-server exited: " + Files.readString(dir.resolve("redis.log")));
            }
            if (ping()) return;
            Thread.sleep(20);
        }
        throw new IllegalStateException("redis-server did not answer PING");
    }

    /** SIGKILL: no shutdown hook, no final AOF rewrite. */
    void kill() throws Exception {
        closeAdmin();
        process.destroyForcibly();
        process.waitFor();
    }

    void restart() throws Exception {
        kill();
        start();
    }

    String uri() {
        return "redis://127.0.0.1:" + port;
    }

    /** Observation only: INFO, CONFIG, DBSIZE, SCAN as the lab user. The application never uses it. */
    RedisCommands<String, String> admin() {
        if (adminConnection == null || !adminConnection.isOpen()) {
            closeAdmin();
            adminClient = RedisClient.create(RedisURI.builder()
                    .withHost("127.0.0.1")
                    .withPort(port)
                    .withAuthentication("lab", adminPassword)
                    .build());
            adminConnection = adminClient.connect();
        }
        return adminConnection.sync();
    }

    Map<String, String> info(String section) {
        var result = new LinkedHashMap<String, String>();
        for (String line : admin().info(section).split("\r?\n")) {
            int i = line.indexOf(':');
            if (i > 0 && !line.startsWith("#")) result.put(line.substring(0, i), line.substring(i + 1).trim());
        }
        return result;
    }

    long countKeys(String pattern) {
        long n = 0;
        var cursor = io.lettuce.core.ScanCursor.INITIAL;
        var args = io.lettuce.core.ScanArgs.Builder.matches(pattern).limit(1000);
        do {
            var page = admin().scan(cursor, args);
            n += page.getKeys().size();
            cursor = page;
        } while (!cursor.isFinished());
        return n;
    }

    private boolean ping() {
        try (var s = new Socket(InetAddress.getLoopbackAddress(), port)) {
            s.setSoTimeout(500);
            OutputStream out = s.getOutputStream();
            out.write("PING\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            InputStream in = s.getInputStream();
            byte[] buf = new byte[16];
            int n = in.read(buf);
            return n > 0 && new String(buf, 0, n, StandardCharsets.US_ASCII).startsWith("+PONG");
        } catch (IOException e) {
            return false;
        }
    }

    private void closeAdmin() {
        if (adminConnection != null) adminConnection.close();
        if (adminClient != null) adminClient.shutdown();
        adminConnection = null;
        adminClient = null;
    }

    @Override
    public void close() throws Exception {
        if (process != null && process.isAlive()) kill();
        else closeAdmin();
        try (Stream<Path> files = Files.walk(dir)) {
            files.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    static int freePort() throws IOException {
        try (var s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return s.getLocalPort();
        }
    }

    private static String locate() {
        String env = System.getenv("REDIS_SERVER");
        if (env != null && !env.isBlank()) return env;
        var dirs = new java.util.ArrayList<String>();
        String path = System.getenv("PATH");
        if (path != null) dirs.addAll(java.util.Arrays.asList(path.split(java.io.File.pathSeparator)));
        dirs.add("/opt/homebrew/bin");
        dirs.add("/usr/local/bin");
        for (String d : dirs) {
            Path candidate = Path.of(d, "redis-server");
            if (Files.isExecutable(candidate)) return candidate.toString();
        }
        throw new MissingRedis();
    }

    private static String version(String binary) {
        try {
            Process p = new ProcessBuilder(binary, "--version").redirectErrorStream(true).start();
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            p.waitFor();
            var m = Pattern.compile("v=(\\d+)\\.(\\d+)\\.(\\d+)").matcher(out);
            if (!m.find()) throw new IllegalStateException("cannot read redis-server version: " + out);
            if (Integer.parseInt(m.group(1)) < 7) {
                throw new IllegalStateException("Redis 7 이상이 필요합니다(설치된 버전 " + m.group() + ")");
            }
            return m.group(1) + "." + m.group(2) + "." + m.group(3);
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    static final class MissingRedis extends RuntimeException {
        MissingRedis() {
            super("redis-server를 찾지 못했습니다. macOS: brew install redis, 또는 REDIS_SERVER에 경로를 지정하세요.");
        }
    }
}
