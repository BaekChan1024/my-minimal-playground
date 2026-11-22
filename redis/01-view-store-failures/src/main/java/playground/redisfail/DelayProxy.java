package playground.redisfail;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

/**
 * 루프백 TCP 프록시. 받은 데이터를 방향별 지연 뒤에 넘긴다.
 * upNanos: 클라이언트 → 서버, downNanos: 서버 → 클라이언트. 실행 중에 바꿀 수 있다.
 */
final class DelayProxy implements AutoCloseable {
    volatile long upNanos;
    volatile long downNanos;
    /** 클라이언트 → 서버로 넘긴 덩어리 수. 순차 요청에서는 요청(왕복) 수와 같다. */
    final java.util.concurrent.atomic.AtomicLong upChunks = new java.util.concurrent.atomic.AtomicLong();

    private final int targetPort;
    private final ServerSocket server;
    private final ExecutorService pool = Executors.newCachedThreadPool(r -> {
        var t = new Thread(r, "delay-proxy");
        t.setDaemon(true);
        return t;
    });
    private final List<Socket> sockets = new CopyOnWriteArrayList<>();
    private volatile boolean closed;

    DelayProxy(int targetPort) throws IOException {
        this.targetPort = targetPort;
        server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        pool.submit(this::accept);
    }

    int port() {
        return server.getLocalPort();
    }

    void delay(long upMicros, long downMicros) {
        upNanos = upMicros * 1_000;
        downNanos = downMicros * 1_000;
    }

    private void accept() {
        while (!closed) {
            try {
                Socket client = server.accept();
                Socket target;
                try {
                    target = new Socket(InetAddress.getLoopbackAddress(), targetPort);
                } catch (IOException refused) {
                    client.close();
                    continue;
                }
                client.setTcpNoDelay(true);
                target.setTcpNoDelay(true);
                sockets.add(client);
                sockets.add(target);
                pool.submit(() -> pump(client, target, true));
                pool.submit(() -> pump(target, client, false));
            } catch (IOException e) {
                if (closed) return;
            }
        }
    }

    /**
     * 읽은 덩어리마다 "받은 시각 + 그때의 지연"에 내보낸다. 덩어리가 여러 개여도 지연이 쌓이지 않으므로
     * 실제 네트워크의 한 방향 지연처럼 동작한다. 순서는 유지한다.
     */
    private void pump(Socket from, Socket to, boolean up) {
        record Chunk(long dueNanos, byte[] bytes) {}
        var queue = new LinkedBlockingQueue<Chunk>();
        byte[] eof = new byte[0];
        pool.submit(() -> {
            try (var out = to.getOutputStream()) {
                while (true) {
                    Chunk c = queue.take();
                    if (c.bytes() == eof) break;
                    long left;
                    while ((left = c.dueNanos() - System.nanoTime()) > 0) LockSupport.parkNanos(left);
                    out.write(c.bytes());
                    out.flush();
                }
            } catch (IOException | InterruptedException ignored) {
                // either side closed
            } finally {
                closeQuietly(from);
                closeQuietly(to);
            }
        });
        byte[] buf = new byte[16 * 1024];
        try (var in = from.getInputStream()) {
            int n;
            while ((n = in.read(buf)) > 0) {
                long wait = up ? upNanos : downNanos;
                if (up) upChunks.incrementAndGet();
                queue.add(new Chunk(System.nanoTime() + wait, Arrays.copyOf(buf, n)));
            }
        } catch (IOException ignored) {
            // either side closed
        } finally {
            queue.add(new Chunk(0, eof));
        }
    }

    private static void closeQuietly(Socket s) {
        try {
            s.close();
        } catch (IOException ignored) {
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        server.close();
        for (Socket s : sockets) closeQuietly(s);
        pool.shutdownNow();
    }
}
