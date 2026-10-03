package playground.pool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ConnectionPoolLab {
    private final DataSource raw;
    private final ExecutorService workers = Executors.newFixedThreadPool(8);
    record Timing(double acquireMs, double sqlMs, double holdMs) {}
    record Activity(String state, String waitType, String waitEvent, int blockers) {}
    @FunctionalInterface interface Probe { boolean get() throws Exception; }

    ConnectionPoolLab(DataSource raw) { this.raw = raw; }
    static void check(boolean ok, String message) {
        if (!ok) throw new IllegalStateException(message);
    }
    static double elapsed(long start) { return (System.nanoTime() - start) / 1_000_000.0; }
    static void until(Probe condition) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.get()) {
            if (System.nanoTime() > end) throw new IllegalStateException("Observation deadline exceeded");
            Thread.sleep(10);
        }
    }
    static void exec(Connection c, String sql) throws SQLException {
        try (var statement = c.createStatement()) { statement.execute(sql); }
    }
    static int pid(Connection c) throws SQLException {
        try (var statement = c.createStatement(); var rows = statement.executeQuery("SELECT pg_backend_pid()")) {
            rows.next(); return rows.getInt(1);
        }
    }
    Activity activity(int pid) throws SQLException {
        // Dedicated observer connection: intentionally outside the measured pool.
        try (var c = raw.getConnection(); var s = c.prepareStatement("""
            SELECT state, wait_event_type, wait_event, cardinality(pg_blocking_pids(pid))
            FROM pg_stat_activity WHERE pid = ?
            """)) {
            s.setInt(1, pid);
            try (var r = s.executeQuery()) {
                check(r.next(), "Backend exists");
                return new Activity(r.getString(1), r.getString(2), r.getString(3), r.getInt(4));
            }
        }
    }
    HikariDataSource pool(int size, long timeout) throws Exception {
        var config = new HikariConfig();
        config.setDataSource(raw);
        config.setPoolName("database-01");
        config.setMaximumPoolSize(size);
        config.setMinimumIdle(size);
        config.setConnectionTimeout(timeout);
        var pool = new HikariDataSource(config);
        try {
            until(() -> pool.getHikariPoolMXBean().getIdleConnections() == size);
            return pool;
        } catch (Exception e) { pool.close(); throw e; }
    }
    static String metrics(HikariDataSource pool) {
        var bean = pool.getHikariPoolMXBean();
        return "active=" + bean.getActiveConnections() + " idle=" + bean.getIdleConnections()
            + " pending=" + bean.getThreadsAwaitingConnection();
    }
    static Timing request(HikariDataSource pool, AtomicBoolean sqlStarted, long holdMillis) throws Exception {
        long start = System.nanoTime();
        try (var c = pool.getConnection()) {
            double acquire = elapsed(start);
            long acquired = System.nanoTime();
            long queryStart = System.nanoTime();
            sqlStarted.set(true);
            try (var s = c.createStatement(); var r = s.executeQuery("SELECT 1")) {
                check(r.next() && r.getInt(1) == 1, "SELECT 1 result");
            }
            double sql = elapsed(queryStart);
            if (holdMillis > 0) Thread.sleep(holdMillis);
            return new Timing(acquire, sql, elapsed(acquired));
        }
    }
    void heldConnections() throws Exception {
        try (var pool = pool(2, 3000); var first = pool.getConnection(); var second = pool.getConnection()) {
            int a = pid(first), b = pid(second);
            until(() -> "idle".equals(activity(a).state()) && "idle".equals(activity(b).state()));
            var started = new AtomicBoolean();
            var waiting = workers.submit(() -> request(pool, started, 0));
            until(() -> pool.getHikariPoolMXBean().getThreadsAwaitingConnection() == 1);
            check(!started.get(), "Waiting request has not submitted SQL");
            String snapshot = metrics(pool);
            check(pool.getHikariPoolMXBean().getActiveConnections() == 2, "Two borrowers");
            Thread.sleep(300); // Controlled application work, not a query or real remote API.
            first.close();
            var result = waiting.get(5, TimeUnit.SECONDS);
            System.out.printf(Locale.ROOT,
                "PASS held-idle %s postgres=[idle,idle] waitingSqlStarted=false acquireMs=%.2f sqlMs=%.2f%n",
                snapshot, result.acquireMs(), result.sqlMs());
        }
    }
    void returnedBeforeWork() throws Exception {
        try (var pool = pool(2, 3000)) {
            try (var a = pool.getConnection(); var b = pool.getConnection()) { exec(a, "SELECT 1"); exec(b, "SELECT 1"); }
            var releaseWork = new CountDownLatch(1);
            var workStarted = new CountDownLatch(2);
            Callable<Void> work = () -> {
                workStarted.countDown();
                check(releaseWork.await(5, TimeUnit.SECONDS), "Work gate released");
                return null;
            };
            var a = workers.submit(work); var b = workers.submit(work);
            try {
                check(workStarted.await(5, TimeUnit.SECONDS), "Two non-DB tasks started");
                var result = workers.submit(() -> request(pool, new AtomicBoolean(), 0)).get(5, TimeUnit.SECONDS);
                check(!a.isDone() && !b.isDone(), "Third SQL completes while non-DB work remains");
                System.out.printf(Locale.ROOT,
                    "PASS returned-before-work thirdFinished=true otherWorkFinished=false acquireMs=%.2f sqlMs=%.2f%n",
                    result.acquireMs(), result.sqlMs());
            } finally { releaseWork.countDown(); }
            a.get(5, TimeUnit.SECONDS); b.get(5, TimeUnit.SECONDS);
        }
    }
    void slowSql() throws Exception {
        try (var pool = pool(2, 3000)) {
            var backend = new CompletableFuture<Integer>();
            var result = workers.submit(() -> {
                long start = System.nanoTime();
                try (var c = pool.getConnection()) {
                    double acquire = elapsed(start); backend.complete(pid(c));
                    long query = System.nanoTime(); exec(c, "SELECT pg_sleep(0.8)");
                    return new Timing(acquire, elapsed(query), 0);
                }
            });
            int pid = backend.get(5, TimeUnit.SECONDS);
            until(() -> "PgSleep".equals(activity(pid).waitEvent()));
            var snapshot = activity(pid); String poolState = metrics(pool);
            check("active".equals(snapshot.state()) && "Timeout".equals(snapshot.waitType()), "Server sleep observed");
            var time = result.get(5, TimeUnit.SECONDS);
            System.out.printf(Locale.ROOT, "PASS slow-sql %s postgres=%s acquireMs=%.2f sqlMs=%.2f%n",
                poolState, snapshot, time.acquireMs(), time.sqlMs());
        }
    }
    void lockCascade() throws Exception {
        try (var pool = pool(2, 3000); var blocker = pool.getConnection()) {
            blocker.setAutoCommit(false);
            exec(blocker, "UPDATE pool_counter SET value=value+1 WHERE id=1");
            int blockerPid = pid(blocker);
            var backend = new CompletableFuture<Integer>();
            var blocked = workers.submit(() -> {
                try (var c = pool.getConnection()) {
                    backend.complete(pid(c)); long start = System.nanoTime();
                    exec(c, "UPDATE pool_counter SET value=value+1 WHERE id=1");
                    return elapsed(start);
                }
            });
            try {
                int waitingPid = backend.get(5, TimeUnit.SECONDS);
                until(() -> "Lock".equals(activity(waitingPid).waitType()));
                var blockedState = activity(waitingPid);
                check(blockedState.blockers() == 1, "One blocking backend");
                until(() -> "idle in transaction".equals(activity(blockerPid).state()));
                var started = new AtomicBoolean();
                var third = workers.submit(() -> request(pool, started, 0));
                until(() -> pool.getHikariPoolMXBean().getThreadsAwaitingConnection() == 1);
                check(!started.get(), "Third request waits before SQL");
                String snapshot = metrics(pool);
                Thread.sleep(300);
                blocker.rollback();
                double blockedMs = blocked.get(5, TimeUnit.SECONDS);
                var thirdTime = third.get(5, TimeUnit.SECONDS);
                System.out.printf(Locale.ROOT,
                    "PASS lock-cascade %s blocker=idle-in-transaction blocked=%s thirdSqlStarted=false blockedSqlMs=%.2f thirdAcquireMs=%.2f%n",
                    snapshot, blockedState, blockedMs, thirdTime.acquireMs());
            } finally { blocker.rollback(); }
        }
    }
    void poolTimeout() throws Exception {
        try (var pool = pool(2, 500); var a = pool.getConnection(); var b = pool.getConnection()) {
            var started = new AtomicBoolean(); long start = System.nanoTime();
            try { request(pool, started, 0); throw new IllegalStateException("Expected acquisition timeout"); }
            catch (SQLTransientConnectionException expected) {
                check(!started.get(), "Acquisition timeout happens before SQL");
                System.out.printf(Locale.ROOT,
                    "PASS pool-timeout limitMs=500 elapsedMs=%.2f exception=%s sqlStarted=false%n",
                    elapsed(start), expected.getClass().getSimpleName());
            }
        }
    }
    void sqlTimeouts() throws Exception {
        try (var pool = pool(2, 3000); var c = pool.getConnection(); var blocker = pool.getConnection()) {
            c.setAutoCommit(false);
            exec(c, "SET LOCAL statement_timeout='300ms'");
            long start = System.nanoTime();
            try { exec(c, "SELECT pg_sleep(1)"); throw new IllegalStateException("Expected statement timeout"); }
            catch (SQLException expected) {
                check("57014".equals(expected.getSQLState()), "Statement canceled");
                System.out.printf(Locale.ROOT, "PASS statement-timeout limitMs=300 elapsedMs=%.2f sqlState=%s%n", elapsed(start), expected.getSQLState());
            } finally { c.rollback(); }
            blocker.setAutoCommit(false);
            exec(blocker, "UPDATE pool_counter SET value=value+1 WHERE id=1");
            try {
                exec(c, "SET LOCAL lock_timeout='300ms'");
                exec(c, "SET LOCAL statement_timeout='2s'");
                start = System.nanoTime();
                try { exec(c, "UPDATE pool_counter SET value=value+1 WHERE id=1"); throw new IllegalStateException("Expected lock timeout"); }
                catch (SQLException expected) {
                    check("55P03".equals(expected.getSQLState()), "Lock timeout");
                    System.out.printf(Locale.ROOT, "PASS lock-timeout limitMs=300 elapsedMs=%.2f sqlState=%s%n", elapsed(start), expected.getSQLState());
                } finally { c.rollback(); }
            } finally { blocker.rollback(); }
        }
    }
    void batch(int poolSize, boolean holdDuringWork) throws Exception {
        try (var pool = pool(poolSize, 3000)) {
            var ready = new CountDownLatch(8); var start = new CountDownLatch(1);
            List<Future<Timing>> results = new ArrayList<>();
            for (int i = 0; i < 8; i++) results.add(workers.submit(() -> {
                ready.countDown(); check(start.await(5, TimeUnit.SECONDS), "Batch start released");
                var result = request(pool, new AtomicBoolean(), holdDuringWork ? 200 : 0);
                if (!holdDuringWork) Thread.sleep(200);
                return result;
            }));
            long batchStart;
            try {
                check(ready.await(5, TimeUnit.SECONDS), "Eight workers ready");
            } finally { batchStart = System.nanoTime(); start.countDown(); }
            double acquire = 0, hold = 0;
            for (var result : results) { var value = result.get(5, TimeUnit.SECONDS); acquire += value.acquireMs(); hold += value.holdMs(); }
            System.out.printf(Locale.ROOT,
                "PASS batch workers=8 pool=%d workMs=200 holdDuringWork=%s totalMs=%.2f meanAcquireMs=%.2f meanHoldMs=%.2f completed=8%n",
                poolSize, holdDuringWork, elapsed(batchStart), acquire/8, hold/8);
        }
    }
    void run() throws Exception {
        try {
            try (var c = raw.getConnection()) {
                exec(c, "CREATE TABLE pool_counter(id int PRIMARY KEY, value int NOT NULL)");
                exec(c, "INSERT INTO pool_counter VALUES (1,0)");
            }
            heldConnections(); returnedBeforeWork(); slowSql(); lockCascade(); poolTimeout(); sqlTimeouts();
            batch(2, true); batch(4, true); batch(2, false);
            System.out.println("ALL 7 SCENARIOS PASSED. Timings are observations, not performance thresholds.");
        } finally {
            workers.shutdownNow();
            check(workers.awaitTermination(5, TimeUnit.SECONDS), "Workers terminated");
        }
    }
    public static void main(String[] args) throws Exception {
        String mode = args.length == 0 ? "guided" : args[0];
        if (!Set.of("guided", "verify").contains(mode)) throw new IllegalArgumentException("Use guided or verify");
        if (mode.equals("guided")) {
            System.out.println("예상: 풀 active=2인데 DB 두 세션이 idle이면, 세 번째 SELECT 1은 어디에서 기다릴까요?");
            System.out.println("같은 200ms 작업을 커넥션 반환 뒤로 옮기면? 풀 크기만 늘리면?");
            System.out.println("Enter: 임시 PostgreSQL의 일곱 실험 / q: 종료");
            var input = new Scanner(System.in);
            if (!input.hasNextLine() || input.nextLine().strip().equalsIgnoreCase("q")) return;
        }
        try (var pg = EmbeddedPostgres.builder().setServerConfig("listen_addresses", "127.0.0.1")
                .setServerConfig("shared_buffers", "32MB").setPort(0).start()) {
            var raw = pg.getPostgresDatabase();
            try (var c = raw.getConnection()) {
                System.out.printf("Java=%s PostgreSQL=%s JDBC=%s HikariCP=7.0.2 (Gradle lock)%n",
                    System.getProperty("java.version"), c.getMetaData().getDatabaseProductVersion(), c.getMetaData().getDriverVersion());
            }
            new ConnectionPoolLab(raw).run();
        }
    }
}
