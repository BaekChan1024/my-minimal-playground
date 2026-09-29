package playground.locking;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import jakarta.persistence.*;
import org.hibernate.Version;
import org.hibernate.resource.jdbc.spi.StatementInspector;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.*;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.core.SpringVersion;
import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class LockingLab {
    static void check(boolean ok, String message) { if (!ok) throw new IllegalStateException(message); }
    static void await(CountDownLatch gate) {
        try { check(gate.await(10, TimeUnit.SECONDS), "Coordination timeout"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
    }
    static final class EditConflict extends RuntimeException { }
    public static class SqlCapture implements StatementInspector {
        final Queue<String> updates = new ConcurrentLinkedQueue<>();
        @Override public String inspect(String sql) {
            if (sql.startsWith("update ")) updates.add(sql);
            return sql;
        }
    }
    final DataSource ds;
    final JdbcTemplate jdbc;
    final EntityManager em;
    final TransactionTemplate tx;
    final SqlCapture sql;
    LockingLab(DataSource ds, EntityManagerFactory factory, SqlCapture sql) {
        this.ds = ds; this.jdbc = new JdbcTemplate(ds); this.sql = sql;
        this.em = SharedEntityManagerCreator.createSharedEntityManager(factory);
        tx = new TransactionTemplate(new JpaTransactionManager(factory));
        tx.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        tx.setTimeout(20);
    }
    void inTx(Consumer<EntityManager> work) {
        tx.executeWithoutResult(status -> {
            jdbc.execute("SET LOCAL statement_timeout='15s'");
            jdbc.execute("SET LOCAL lock_timeout='12s'");
            work.accept(em);
        });
    }
    void init() {
        jdbc.execute("CREATE TABLE plain_post (id bigint PRIMARY KEY, title text NOT NULL)");
        jdbc.execute("CREATE TABLE versioned_post (id bigint PRIMARY KEY, title text NOT NULL, version bigint NOT NULL)");
        jdbc.execute("CREATE TABLE outbox_record (actor text PRIMARY KEY, aggregate_version bigint NOT NULL)");
    }
    void reset() {
        jdbc.execute("TRUNCATE plain_post,versioned_post,outbox_record");
        jdbc.update("INSERT INTO plain_post VALUES (1,'original')");
        jdbc.update("INSERT INTO versioned_post VALUES (1,'original',0)");
        sql.updates.clear();
    }
    void event(String actor, long version) { jdbc.update("INSERT INTO outbox_record VALUES (?,?)",actor,version); }
    int pid() { return jdbc.queryForObject("SELECT pg_backend_pid()", Integer.class); }
    String observed(String query) {
        try (var c = ds.getConnection(); var s = c.createStatement()) {
            check(c.getAutoCommit() && c.getTransactionIsolation() == Connection.TRANSACTION_READ_COMMITTED, "Observer connection");
            s.setQueryTimeout(5);
            try (var result = s.executeQuery(query)) { check(result.next(), "Observation empty"); return result.getString(1); }
        } catch (SQLException e) { throw new IllegalStateException(e); }
    }
    String events() { return observed("SELECT coalesce(string_agg(actor || ':' || aggregate_version,',' ORDER BY actor),'none') FROM outbox_record"); }
    String row() { return observed("SELECT title || ':' || version FROM versioned_post WHERE id=1"); }
    static String result(Future<?> future) throws Exception {
        try { future.get(18, TimeUnit.SECONDS); return "committed"; }
        catch (ExecutionException e) {
            Throwable root = e.getCause();
            for (Throwable t = root; t != null; t = t.getCause()) {
                if (t instanceof OptimisticLockException) return "OptimisticLockException";
                if (t instanceof EditConflict) return "EditConflict";
            }
            throw new IllegalStateException("Unexpected worker failure", root);
        }
    }
    void overlap(boolean versioned) throws Exception {
        reset();
        var bothRead = new CountDownLatch(2); var aDone = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(2); var aPid = new AtomicInteger(); var bPid = new AtomicInteger();
        try {
            Future<?> a = pool.submit(() -> {
                try { inTx(e -> {
                    aPid.set(pid());
                    if (versioned) {
                        var post=e.find(VersionedPost.class,1L); check(post.version()==0,"A version");
                        bothRead.countDown(); await(bothRead); post.title("A"); e.flush(); event("A",post.version());
                    } else {
                        var post=e.find(PlainPost.class,1L); check(post.title().equals("original"),"A read");
                        bothRead.countDown(); await(bothRead); post.title("A"); e.flush();
                    }
                }); } finally { aDone.countDown(); }
            });
            Future<?> b = pool.submit(() -> inTx(e -> {
                bPid.set(pid());
                if (versioned) {
                    var post=e.find(VersionedPost.class,1L); check(post.version()==0,"B version");
                    bothRead.countDown(); await(aDone); post.title("B"); e.flush(); event("B",post.version());
                } else {
                    var post=e.find(PlainPost.class,1L); check(post.title().equals("original"),"B read");
                    bothRead.countDown(); await(aDone); post.title("B"); e.flush();
                }
            }));
            String ar=result(a), br=result(b);
            check(aPid.get()!=bPid.get(),"Independent connections required");
            check(ar.equals("committed"),"A must commit");
            if(versioned) {
                check(br.equals("OptimisticLockException") && row().equals("A:1") && events().equals("A:1"),"Optimistic result");
                check(sql.updates.stream().anyMatch(s->s.contains("versioned_post")&&s.contains("and version=?")),"Missing version predicate");
                System.out.println("PASS overlap-versioned A=committed B="+br+" row="+row()+" outbox="+events()+" separateConnections=true");
            } else {
                check(br.equals("committed") && observed("SELECT title FROM plain_post WHERE id=1").equals("B") && events().equals("none"),"Lost update result");
                System.out.println("PASS overlap-unversioned A=committed B=committed title=B separateConnections=true");
            }
            sql.updates.stream().distinct().forEach(s->System.out.println("SQL "+s));
        } finally { bothRead.countDown(); bothRead.countDown(); aDone.countDown(); pool.shutdownNow(); check(pool.awaitTermination(18,TimeUnit.SECONDS),"Workers did not stop"); }
    }
    void editRequest(String actor, long expected, boolean compare) {
        inTx(e->{
            var post=e.find(VersionedPost.class,1L);
            if(compare && post.version()!=expected) throw new EditConflict();
            post.title(actor); e.flush(); event(actor,post.version());
        });
    }
    void staleRequest(boolean compare) {
        reset();
        // Both browser drafts were based on version 0. The HTTP-like saves are sequential.
        editRequest("A",0,compare);
        String b="committed";
        try { editRequest("B",0,compare); } catch(EditConflict ex) { b="EditConflict"; }
        check(row().equals(compare?"A:1":"B:2") && events().equals(compare?"A:1":"A:1,B:2"),"Stale request result");
        check(b.equals(compare?"EditConflict":"committed"),"Stale request failure");
        System.out.printf("PASS stale-request-%s expectedB=0 B=%s row=%s outbox=%s%n",compare?"check":"version-only",b,row(),events());
    }
    boolean blockedBy(int waiter, int blocker) {
        return Boolean.TRUE.equals(jdbc.queryForObject("SELECT ? = ANY(pg_blocking_pids(?))",Boolean.class,blocker,waiter));
    }
    void pessimistic(boolean compare) throws Exception {
        reset();
        var aLocked=new CountDownLatch(1); var releaseA=new CountDownLatch(1); var bStarted=new CountDownLatch(1);
        var aPid=new AtomicInteger(); var bPid=new AtomicInteger(); var bReadVersion=new AtomicInteger(-1);
        var pool=Executors.newFixedThreadPool(2);
        try {
            Future<?> a=pool.submit(()->inTx(e->{
                aPid.set(pid()); var post=e.find(VersionedPost.class,1L,LockModeType.PESSIMISTIC_WRITE);
                check(post.version()==0,"A initial version"); aLocked.countDown(); await(releaseA);
                post.title("A"); e.flush(); event("A",post.version());
            }));
            await(aLocked);
            Future<?> b=pool.submit(()->inTx(e->{
                bPid.set(pid()); bStarted.countDown();
                // Load with the lock from the start: do not reuse an entity loaded before waiting.
                var post=e.find(VersionedPost.class,1L,LockModeType.PESSIMISTIC_WRITE);
                bReadVersion.set((int)post.version());
                if(compare && post.version()!=0) throw new EditConflict();
                post.title("B"); e.flush(); event("B",post.version());
            }));
            await(bStarted);
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);
            boolean blocked=false;
            while(System.nanoTime()<deadline) {
                if(blockedBy(bPid.get(),aPid.get())) { blocked=true; break; }
                if(b.isDone()) break;
                Thread.sleep(20); // Poll DB evidence, never infer a lock from elapsed time alone.
            }
            check(blocked,"DB did not report B blocked by A");
            String during=row(); check(during.equals("original:0"),"Ordinary observer SELECT should see committed row");
            releaseA.countDown();
            String ar=result(a),br=result(b);
            check(ar.equals("committed")&&br.equals(compare?"EditConflict":"committed"),"Pessimistic outcomes");
            check(bReadVersion.get()==1 && row().equals(compare?"A:1":"B:2")&&events().equals(compare?"A:1":"A:1,B:2"),"Pessimistic final state");
            System.out.printf("PASS row-lock-%s blockedByA=true observerDuring=%s BloadedVersion=1 B=%s row=%s outbox=%s%n",compare?"check":"only",during,br,row(),events());
        } finally { releaseA.countDown(); pool.shutdownNow(); check(pool.awaitTermination(18,TimeUnit.SECONDS),"Workers did not stop"); }
    }
    public static void main(String[] args) throws Exception {
        String mode=args.length==0?"guided":args[0];
        if(!Set.of("guided","verify").contains(mode)) throw new IllegalArgumentException("Use guided or verify");
        if(mode.equals("guided")) {
            System.out.println("예상: @Version이면 오래 열린 편집 화면도 보호될까요? 행 잠금만으로 오래된 요청을 거절할까요?");
            System.out.println("Enter: 임시 PostgreSQL에서 여섯 실험 실행 / q: 종료");
            if(new Scanner(System.in).nextLine().strip().equalsIgnoreCase("q")) return;
        }
        try(var postgres=EmbeddedPostgres.builder().setServerConfig("listen_addresses","127.0.0.1")
                .setServerConfig("shared_buffers","32MB").setPort(0).start()) {
            var ds=postgres.getPostgresDatabase(); var capture=new SqlCapture();
            var factory=new LocalContainerEntityManagerFactoryBean(); factory.setDataSource(ds);
            factory.setPackagesToScan("playground.locking"); factory.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
            factory.setJpaPropertyMap(Map.of("hibernate.hbm2ddl.auto","none","hibernate.session_factory.statement_inspector",capture));
            factory.afterPropertiesSet();
            try {
                var lab=new LockingLab(ds,Objects.requireNonNull(factory.getObject()),capture);lab.init();
                try(var c=ds.getConnection()) { System.out.printf("Spring=%s Hibernate=%s PostgreSQL=%s JDBC=%s isolation=READ_COMMITTED%n",SpringVersion.getVersion(),Version.getVersionString(),c.getMetaData().getDatabaseProductVersion(),c.getMetaData().getDriverVersion()); }
                lab.overlap(false);lab.overlap(true);lab.staleRequest(false);lab.staleRequest(true);lab.pessimistic(false);lab.pessimistic(true);
                System.out.println("ALL 6 SCENARIOS PASSED. 결과 비교: spring/03-concurrent-editing/ANSWERS.md");
            } finally { factory.destroy(); }
        }
    }
}
