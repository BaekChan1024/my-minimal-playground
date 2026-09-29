package playground.spring;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.annotation.*;
import org.springframework.core.SpringVersion;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.annotation.*;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import javax.sql.DataSource;
import java.io.IOException;
import java.util.*;

public final class TransactionLab {
    public static void main(String[] args) throws Exception {
        boolean guided = args.length == 0 || args[0].equals("guided");
        if (args.length > 1 || (args.length == 1 && !Set.of("guided", "verify").contains(args[0])))
            throw new IllegalArgumentException("guided|verify");
        try (var in = new Scanner(System.in)) {
            if (guided) {
                System.out.println("예외가 나면 항상 롤백될까요? 내부 예외를 catch하면 커밋할 수 있을까요? [Enter/q]");
                if (!in.hasNextLine() || in.nextLine().equalsIgnoreCase("q")) return;
            }
            try (var postgres = EmbeddedPostgres.builder()
                    .setServerConfig("listen_addresses", "127.0.0.1")
                    .setServerConfig("shared_buffers", "32MB").setPort(0).start();
                 var context = new AnnotationConfigApplicationContext()) {
                context.registerBean(DataSource.class, () -> postgres.getPostgresDatabase());
                context.register(Config.class);
                context.refresh();
                var db = context.getBean(Db.class);
                var trace = context.getBean(Trace.class);
                var service = context.getBean(Service.class);
                var inner = context.getBean(Inner.class);
                check(AopUtils.isAopProxy(service) && AopUtils.isAopProxy(inner), "Expected actual Spring proxies");
                db.init();
                System.out.println("VERSIONS Spring=" + SpringVersion.getVersion()
                        + " PostgreSQL=" + db.jdbc.queryForObject("SHOW server_version", String.class));
                scenario(db, trace, "external-runtime", service::runtimeFailure, Fault.class, 0, 0, 0, "active=true");
                scenario(db, trace, "checked-default", service::checkedFailure, IOException.class, 1, 1, 0, "active=true");
                scenario(db, trace, "checked-rollbackFor", service::checkedRollback, IOException.class, 0, 0, 0, "active=true");
                scenario(db, trace, "self-invocation", service::selfCall, Fault.class, 1, 1, 0, "active=false");
                scenario(db, trace, "caught-local", service::catchLocal, null, 1, 0, 0, "active=true");
                scenario(db, trace, "caught-required", service::catchRequired, UnexpectedRollbackException.class, 0, 0, 0, "sameTx=true");
                scenario(db, trace, "requires-new-audit", service::independentAudit, Fault.class, 0, 0, 1, "differentTx=true outerResumed=true");
                System.out.println("PASS scenarios=7 proxyVerified=true committedRowsCheckedOutsideTransaction=true");
            }
        }
    }
    @FunctionalInterface interface Work { void run() throws Exception; }
    static void scenario(Db db, Trace trace, String name, Work work, Class<? extends Exception> expected,
                         int posts, int outbox, int audit, String expectedTrace) throws Exception {
        check(!TransactionSynchronizationManager.isActualTransactionActive(), "Runner must not own transaction");
        db.reset(); trace.value = ""; Exception actual = null;
        try { work.run(); } catch (Exception e) { actual = e; }
        check(expected == null ? actual == null : actual != null && actual.getClass() == expected,
                "Unexpected outcome: " + name + " / " + (actual == null ? "none" : actual.getClass().getSimpleName()));
        check(!TransactionSynchronizationManager.isActualTransactionActive(), "Transaction leaked");
        check(db.count("post_record") == posts && db.count("outbox_record") == outbox
                && db.count("audit_record") == audit, "Unexpected committed rows: " + name);
        check(trace.value.equals(expectedTrace), "Unexpected trace: " + name + " / " + trace.value);
        System.out.printf("PASS %s posts=%d outbox=%d audit=%d error=%s %s%n", name, posts, outbox, audit,
                actual == null ? "none" : actual.getClass().getSimpleName(), trace.value);
    }
    static void check(boolean value, String message) { if (!value) throw new IllegalStateException(message); }
    static final class Fault extends IllegalStateException { }
    public static class Trace { String value = ""; }
    public static class Db {
        final JdbcTemplate jdbc;
        Db(JdbcTemplate jdbc) { this.jdbc = jdbc; }
        void init() {
            for (String table : List.of("post_record", "outbox_record", "audit_record"))
                jdbc.execute("CREATE TABLE " + table + " (id integer PRIMARY KEY, note text NOT NULL)");
        }
        void reset() { jdbc.execute("TRUNCATE post_record,outbox_record,audit_record"); }
        void post() { jdbc.update("INSERT INTO post_record VALUES (1,'post')"); }
        void outbox() { jdbc.update("INSERT INTO outbox_record VALUES (1,'event')"); }
        void audit() { jdbc.update("INSERT INTO audit_record VALUES (1,'attempt observed')"); }
        long count(String table) { return jdbc.queryForObject("SELECT count(*) FROM " + table, Long.class); }
        String txid() { return jdbc.queryForObject("SELECT pg_current_xact_id()::text", String.class); }
    }
    public static class Inner {
        final Db db;
        final Trace trace;
        Inner(Db db, Trace trace) { this.db = db; this.trace = trace; }
        @Transactional
        public void requiredFailure(String outerId) {
            trace.value = "sameTx=" + outerId.equals(db.txid());
            db.outbox();
            throw new Fault();
        }
        @Transactional(propagation = Propagation.REQUIRES_NEW)
        public String auditAttempt() { db.audit(); return db.txid(); }
    }
    public static class Service {
        final Db db;
        final Trace trace;
        final Inner inner;
        Service(Db db, Trace trace, Inner inner) { this.db = db; this.trace = trace; this.inner = inner; }
        void recordActive() { trace.value = "active=" + TransactionSynchronizationManager.isActualTransactionActive(); }
        void both() { db.post(); db.outbox(); }
        @Transactional
        public void runtimeFailure() { recordActive(); both(); throw new Fault(); }
        @Transactional
        public void checkedFailure() throws IOException { recordActive(); both(); throw new IOException("synthetic failure"); }
        @Transactional(rollbackFor = IOException.class)
        public void checkedRollback() throws IOException { recordActive(); both(); throw new IOException("synthetic failure"); }
        public void selfCall() { this.runtimeFailure(); }
        @Transactional
        public void catchLocal() {
            recordActive(); db.post();
            try { throw new Fault(); } catch (Fault ignored) { /* Intentionally swallowed for the experiment. */ }
        }
        @Transactional
        public void catchRequired() {
            db.post(); String outerId = db.txid();
            try { inner.requiredFailure(outerId); } catch (Fault ignored) { /* Inner proxy already marked rollback-only. */ }
        }
        @Transactional
        public void independentAudit() {
            both(); String outerId = db.txid(); String auditId = inner.auditAttempt();
            trace.value = "differentTx=" + !outerId.equals(auditId) + " outerResumed=" + outerId.equals(db.txid());
            throw new Fault();
        }
    }
    @Configuration(proxyBeanMethods = false)
    @EnableTransactionManagement(proxyTargetClass = true)
    public static class Config {
        @Bean JdbcTemplate jdbc(DataSource ds) { return new JdbcTemplate(ds); }
        @Bean PlatformTransactionManager transactionManager(DataSource ds) { return new DataSourceTransactionManager(ds); }
        @Bean Db db(JdbcTemplate jdbc) { return new Db(jdbc); }
        @Bean Trace trace() { return new Trace(); }
        @Bean Inner inner(Db db, Trace trace) { return new Inner(db, trace); }
        @Bean Service service(Db db, Trace trace, Inner inner) { return new Service(db, trace, inner); }
    }
}
