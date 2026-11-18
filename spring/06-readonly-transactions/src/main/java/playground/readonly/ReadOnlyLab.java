package playground.readonly;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Version;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.resource.jdbc.spi.StatementInspector;
import org.hibernate.stat.Statistics;
import org.springframework.core.SpringVersion;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;
import org.springframework.orm.jpa.EntityManagerHolder;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.lang.management.ManagementFactory;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * What does {@code @Transactional(readOnly = true)} change, layer by layer?
 * Spring transaction state, Hibernate session, JDBC connection and PostgreSQL are observed separately.
 */
public class ReadOnlyLab {
    static void check(boolean condition, String message) { if (!condition) throw new IllegalStateException(message); }

    /** Counts only SQL that Hibernate itself sends. JdbcTemplate statements are not included. */
    public static class Counter implements StatementInspector {
        final AtomicInteger selects = new AtomicInteger(), inserts = new AtomicInteger(), updates = new AtomicInteger();
        @Override public String inspect(String sql) {
            String s = sql.stripLeading().toLowerCase(Locale.ROOT);
            if (s.startsWith("select")) selects.incrementAndGet();
            else if (s.startsWith("insert")) inserts.incrementAndGet();
            else if (s.startsWith("update")) updates.incrementAndGet();
            return sql;
        }
        void reset() { selects.set(0); inserts.set(0); updates.set(0); }
    }

    /** One JPA setup: EntityManagerFactory + JpaTransactionManager + JdbcTemplate on the same DataSource. */
    static final class Jpa implements AutoCloseable {
        final DataSource ds;
        final LocalContainerEntityManagerFactoryBean bean = new LocalContainerEntityManagerFactoryBean();
        final EntityManagerFactory emf;
        final JpaTransactionManager tm;
        final EntityManager em;
        final JdbcTemplate jdbc;
        final Counter counter = new Counter();
        Jpa(DataSource ds, String unit) {
            this.ds = ds;
            bean.setDataSource(ds);
            bean.setPersistenceUnitName(unit);
            bean.setPackagesToScan("playground.readonly");
            bean.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
            bean.setJpaPropertyMap(Map.of("hibernate.hbm2ddl.auto", "none",
                    "hibernate.session_factory.statement_inspector", counter,
                    "hibernate.generate_statistics", "true",
                    "hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect"));
            bean.afterPropertiesSet();
            emf = Objects.requireNonNull(bean.getObject());
            tm = new JpaTransactionManager(emf);
            em = SharedEntityManagerCreator.createSharedEntityManager(emf);
            jdbc = new JdbcTemplate(ds);
        }
        TransactionTemplate tx(boolean readOnly) { return tx(readOnly, TransactionDefinition.PROPAGATION_REQUIRED, tm); }
        TransactionTemplate tx(boolean readOnly, int propagation, JpaTransactionManager manager) {
            var t = new TransactionTemplate(manager);
            t.setReadOnly(readOnly); t.setPropagationBehavior(propagation); t.setTimeout(15);
            return t;
        }
        Session session() { return em.unwrap(Session.class); }
        Statistics stats() { return emf.unwrap(SessionFactory.class).getStatistics(); }
        String dbReadOnly() { return jdbc.queryForObject("SHOW transaction_read_only", String.class); }
        int pid() { return jdbc.queryForObject("SELECT pg_backend_pid()", Integer.class); }
        boolean jdbcReadOnly() {
            var c = DataSourceUtils.getConnection(ds);
            try { return c.isReadOnly(); } catch (SQLException e) { throw new IllegalStateException(e); }
            finally { DataSourceUtils.releaseConnection(c, ds); }
        }
        @Override public void close() { bean.destroy(); }
    }

    static HikariDataSource pool(EmbeddedPostgres pg, String db, int size, String readOnlyMode) {
        var config = new HikariConfig();
        config.setJdbcUrl(pg.getJdbcUrl("postgres", db));
        config.setMaximumPoolSize(size);
        config.setPoolName("lab-" + db + "-" + (readOnlyMode == null ? "default" : readOnlyMode));
        if (readOnlyMode != null) config.addDataSourceProperty("readOnlyMode", readOnlyMode);
        return new HikariDataSource(config);
    }

    static SQLException sqlCause(Throwable t) {
        for (; t != null; t = t.getCause()) if (t instanceof SQLException s) return s;
        return null;
    }
    /** Runs the action; returns "none" or "SQLSTATE exceptionType: first line of the DB message". */
    static String failure(Runnable action) {
        try { action.run(); return "none"; }
        catch (RuntimeException e) {
            var sql = sqlCause(e);
            if (sql == null) return e.getClass().getSimpleName();
            return sql.getSQLState() + " " + e.getClass().getSimpleName() + ": " + sql.getMessage().lines().findFirst().orElse("");
        }
    }

    final EmbeddedPostgres pg;
    final HikariDataSource main;
    final Jpa jpa;
    ReadOnlyLab(EmbeddedPostgres pg) {
        this.pg = pg;
        this.main = pool(pg, "postgres", 4, null);
        new JdbcTemplate(main).execute("CREATE TABLE post_record (id bigint PRIMARY KEY, title text NOT NULL, version bigint NOT NULL)");
        this.jpa = new Jpa(main, "main");
    }
    void reset() {
        jpa.jdbc.execute("TRUNCATE post_record");
        jpa.jdbc.update("INSERT INTO post_record VALUES (1,'original',0)");
        jpa.counter.reset();
    }
    String title() { return jpa.jdbc.queryForObject("SELECT title FROM post_record WHERE id=1", String.class); }
    long rows() { return jpa.jdbc.queryForObject("SELECT count(*) FROM post_record", Long.class); }

    // 1. readOnly=true/false: which flags change on each layer, and is the pooled connection reset?
    void flags() {
        try (var one = pool(pg, "postgres", 1, null); var flagsJpa = new Jpa(one, "flags")) {
            record Probe(String db, boolean jdbc, String flush, boolean defaultReadOnly, boolean sync, int pid) {}
            Supplier<Probe> probe = () -> new Probe(flagsJpa.dbReadOnly(), flagsJpa.jdbcReadOnly(),
                    flagsJpa.session().getHibernateFlushMode().name(), flagsJpa.session().isDefaultReadOnly(),
                    TransactionSynchronizationManager.isCurrentTransactionReadOnly(), flagsJpa.pid());
            Probe ro = flagsJpa.tx(true).execute(s -> probe.get());
            Probe rw = flagsJpa.tx(false).execute(s -> probe.get());
            check(ro.db().equals("on") && ro.jdbc() && ro.flush().equals("MANUAL") && ro.defaultReadOnly() && ro.sync(), "readOnly flags " + ro);
            check(rw.db().equals("off") && !rw.jdbc() && rw.flush().equals("AUTO") && !rw.defaultReadOnly() && !rw.sync(), "read-write flags " + rw);
            check(ro.pid() == rw.pid(), "pool size 1 must reuse one physical connection");
            System.out.printf("OBS flags readOnly=true  springSync=%s hibernateFlush=%s sessionDefaultReadOnly=%s jdbcConnectionReadOnly=%s pgTransactionReadOnly=%s%n",
                    ro.sync(), ro.flush(), ro.defaultReadOnly(), ro.jdbc(), ro.db());
            System.out.printf("OBS flags readOnly=false springSync=%s hibernateFlush=%s sessionDefaultReadOnly=%s jdbcConnectionReadOnly=%s pgTransactionReadOnly=%s%n",
                    rw.sync(), rw.flush(), rw.defaultReadOnly(), rw.jdbc(), rw.db());
            System.out.println("PASS flags samePhysicalConnection=true resetAfterReadOnly=true");
        }
    }

    // 2. Changing a managed entity in readOnly vs read-write transactions.
    void dirtyChecking() {
        for (boolean readOnly : new boolean[]{true, false}) {
            reset();
            long flushes = jpa.stats().getFlushCount();
            boolean entityReadOnly = Boolean.TRUE.equals(jpa.tx(readOnly).execute(s -> {
                Post p = jpa.em.find(Post.class, 1L);
                p.rename("changed");
                return jpa.session().isReadOnly(p);
            }));
            String after = title();
            int updates = jpa.counter.updates.get();
            long commitFlushes = jpa.stats().getFlushCount() - flushes;
            if (readOnly) check(entityReadOnly && after.equals("original") && updates == 0 && commitFlushes == 0, "readOnly dirty check");
            else check(!entityReadOnly && after.equals("changed") && updates == 1 && commitFlushes == 1, "read-write dirty check");
            System.out.printf("OBS dirty-check readOnly=%s entityReadOnly=%s flushes=%d hibernateUpdates=%d exception=none dbTitle=%s%n",
                    readOnly, entityReadOnly, commitFlushes, updates, after);
        }
        reset();
        jpa.tx(true).executeWithoutResult(s -> {
            Post p = jpa.em.find(Post.class, 1L);
            p.rename("changed");
            jpa.em.flush();
        });
        check(title().equals("original") && jpa.counter.updates.get() == 0, "explicit flush of read-only entity");
        System.out.printf("OBS dirty-check readOnly=true explicitFlush=true hibernateUpdates=%d exception=none dbTitle=%s%n",
                jpa.counter.updates.get(), title());
        System.out.println("PASS dirty-check readOnlyChangeSilentlyDropped=true");
    }

    // 3. Writes that do reach the database inside a readOnly transaction.
    void writes() {
        reset();
        String jdbcUpdate = failure(() -> jpa.tx(true).executeWithoutResult(s ->
                jpa.jdbc.update("UPDATE post_record SET title='jdbc' WHERE id=1")));
        check(jdbcUpdate.startsWith("25006") && title().equals("original"), "JDBC update must be rejected: " + jdbcUpdate);
        System.out.println("OBS writes jdbcUpdate=" + jdbcUpdate);

        reset();
        String bulk = failure(() -> jpa.tx(true).executeWithoutResult(s ->
                jpa.em.createQuery("update Post p set p.title='bulk' where p.id=1").executeUpdate()));
        check(bulk.startsWith("25006") && title().equals("original"), "JPQL bulk update must be rejected: " + bulk);
        System.out.println("OBS writes jpqlBulkUpdate=" + bulk);

        reset();
        String persistOnly = failure(() -> jpa.tx(true).executeWithoutResult(s -> jpa.em.persist(new Post(2, "draft"))));
        check(persistOnly.equals("none") && jpa.counter.inserts.get() == 0 && rows() == 1, "persist without flush");
        System.out.printf("OBS writes persistWithoutFlush exception=%s hibernateInserts=%d rows=%d%n", persistOnly, jpa.counter.inserts.get(), rows());

        reset();
        String persistFlush = failure(() -> jpa.tx(true).executeWithoutResult(s -> {
            jpa.em.persist(new Post(2, "draft"));
            jpa.em.flush();
        }));
        check(persistFlush.startsWith("25006") && jpa.counter.inserts.get() == 1 && rows() == 1, "persist+flush must be rejected: " + persistFlush);
        System.out.printf("OBS writes persistAndFlush hibernateInserts=%d rows=%d exception=%s%n", jpa.counter.inserts.get(), rows(), persistFlush);
        System.out.println("PASS writes sqlReachingDbRejected=3 silentlyDropped=1");
    }

    // 4. Nested @Transactional with REQUIRED/REQUIRES_NEW and validateExistingTransaction.
    void propagation() {
        // 4a. Outer read-write calls an inner readOnly method (like recordView -> views through a proxy).
        reset();
        var inner = new LinkedHashMap<String, Object>();
        jpa.tx(false).executeWithoutResult(o -> {
            int outerPid = jpa.pid();
            jpa.tx(true).executeWithoutResult(i -> {
                inner.put("samePid", jpa.pid() == outerPid);
                inner.put("pg", jpa.dbReadOnly());
                inner.put("sync", TransactionSynchronizationManager.isCurrentTransactionReadOnly());
                inner.put("flush", jpa.session().getHibernateFlushMode().name());
                jpa.jdbc.update("UPDATE post_record SET title='inner-write' WHERE id=1");
            });
        });
        check(inner.get("samePid").equals(true) && inner.get("pg").equals("off") && inner.get("sync").equals(false)
                && inner.get("flush").equals("AUTO") && title().equals("inner-write"), "inner readOnly is ignored " + inner);
        System.out.printf("OBS propagation outer=RW inner=readOnly/REQUIRED innerPg=%s innerSync=%s innerFlush=%s innerWrite=committed%n",
                inner.get("pg"), inner.get("sync"), inner.get("flush"));

        // 4b. Outer readOnly calls an inner read-write method with REQUIRED.
        reset();
        var innerRw = new LinkedHashMap<String, Object>();
        String entity = failure(() -> jpa.tx(true).executeWithoutResult(o ->
                jpa.tx(false).executeWithoutResult(i -> {
                    innerRw.put("pg", jpa.dbReadOnly());
                    innerRw.put("flush", jpa.session().getHibernateFlushMode().name());
                    jpa.em.find(Post.class, 1L).rename("inner-rw");
                })));
        check(entity.equals("none") && innerRw.get("pg").equals("on") && innerRw.get("flush").equals("MANUAL")
                && jpa.counter.updates.get() == 0 && title().equals("original"), "inner RW change in readOnly outer " + innerRw);
        reset();
        String sql = failure(() -> jpa.tx(true).executeWithoutResult(o ->
                jpa.tx(false).executeWithoutResult(i -> jpa.jdbc.update("UPDATE post_record SET title='inner-rw' WHERE id=1"))));
        check(sql.startsWith("25006") && title().equals("original"), "inner RW SQL in readOnly outer: " + sql);
        System.out.printf("OBS propagation outer=readOnly inner=RW/REQUIRED innerPg=%s innerFlush=%s entityChange=dropped(exception=%s) jdbcUpdate=%s%n",
                innerRw.get("pg"), innerRw.get("flush"), entity, sql);

        // 4c. validateExistingTransaction=true rejects the mismatch before the inner work runs.
        reset();
        var strict = new JpaTransactionManager(jpa.emf);
        strict.setValidateExistingTransaction(true);
        boolean[] innerRan = {false};
        String validated = "none";
        try {
            jpa.tx(true, TransactionDefinition.PROPAGATION_REQUIRED, strict).executeWithoutResult(o ->
                    jpa.tx(false, TransactionDefinition.PROPAGATION_REQUIRED, strict).executeWithoutResult(i -> innerRan[0] = true));
        } catch (IllegalTransactionStateException e) { validated = e.getClass().getSimpleName() + ": " + e.getMessage(); }
        check(validated.startsWith("IllegalTransactionStateException") && !innerRan[0], "validateExistingTransaction");
        System.out.printf("OBS propagation validateExistingTransaction=true innerRan=%s exception=%s%n", innerRan[0], validated);

        // 4d. REQUIRES_NEW opens a separate read-write transaction on another connection.
        reset();
        var newTx = new LinkedHashMap<String, Object>();
        jpa.tx(true).executeWithoutResult(o -> {
            int outerPid = jpa.pid();
            jpa.tx(false, TransactionDefinition.PROPAGATION_REQUIRES_NEW, jpa.tm).executeWithoutResult(i -> {
                newTx.put("otherPid", jpa.pid() != outerPid);
                newTx.put("pg", jpa.dbReadOnly());
                jpa.em.find(Post.class, 1L).rename("requires-new");
            });
            newTx.put("outerPgAfter", jpa.dbReadOnly());
        });
        check(newTx.get("otherPid").equals(true) && newTx.get("pg").equals("off") && newTx.get("outerPgAfter").equals("on")
                && title().equals("requires-new"), "REQUIRES_NEW " + newTx);
        System.out.printf("OBS propagation outer=readOnly inner=RW/REQUIRES_NEW otherConnection=true innerPg=%s outerPgAfter=%s dbTitle=%s%n",
                newTx.get("pg"), newTx.get("outerPgAfter"), title());
        System.out.println("PASS propagation scenarios=4");
    }

    /** Routes by Spring's current-transaction readOnly flag and records each lookup. */
    static class ReadOnlyRouting extends AbstractRoutingDataSource {
        final List<String> lookups = new CopyOnWriteArrayList<>();
        @Override protected Object determineCurrentLookupKey() {
            String key = TransactionSynchronizationManager.isCurrentTransactionReadOnly() ? "replica" : "primary";
            lookups.add(key);
            return key;
        }
    }

    // 5. Primary/replica routing by readOnly. "replica" is a second database in the same server, not real replication.
    void routing() {
        new JdbcTemplate(main).execute("CREATE DATABASE replica");
        var replicaReadOnlyConfig = new HikariConfig();
        replicaReadOnlyConfig.setJdbcUrl(pg.getJdbcUrl("postgres", "replica"));
        replicaReadOnlyConfig.setMaximumPoolSize(2);
        replicaReadOnlyConfig.setPoolName("lab-replica-hikari-readonly");
        replicaReadOnlyConfig.setReadOnly(true);
        try (var primary = pool(pg, "postgres", 2, null); var replica = pool(pg, "replica", 2, null);
             var replicaReadOnly = new HikariDataSource(replicaReadOnlyConfig)) {
            new JdbcTemplate(primary).execute("CREATE TABLE node (name text)");
            new JdbcTemplate(primary).update("INSERT INTO node VALUES ('primary')");
            new JdbcTemplate(replica).execute("CREATE TABLE node (name text)");
            new JdbcTemplate(replica).update("INSERT INTO node VALUES ('replica')");
            var routing = new ReadOnlyRouting();
            routing.setTargetDataSources(Map.of("primary", primary, "replica", replica));
            routing.setDefaultTargetDataSource(primary);
            routing.afterPropertiesSet();
            var lazyRouting = new LazyConnectionDataSourceProxy(routing);
            var lazyReadOnly = new LazyConnectionDataSourceProxy(primary);
            lazyReadOnly.setReadOnlyDataSource(replica);
            var lazyReadOnlyPool = new LazyConnectionDataSourceProxy(primary);
            lazyReadOnlyPool.setReadOnlyDataSource(replicaReadOnly);

            record Case(String name, DataSource ds, String readOnlyTarget, String pgReadOnly, boolean routed) {}
            var cases = List.of(
                    new Case("routing-only", routing, "primary", "on", true),
                    new Case("lazy+routing", lazyRouting, "replica", "on", true),
                    new Case("lazy+readOnlyDataSource", lazyReadOnly, "replica", "off", false),
                    new Case("lazy+readOnlyDataSource(pool readOnly)", lazyReadOnlyPool, "replica", "on", false));
            for (var c : cases) {
                try (var routed = new Jpa(c.ds(), "route-" + cases.indexOf(c))) {
                    routing.lookups.clear();
                    String[] ro = routed.tx(true).execute(s -> new String[]{
                            routed.jdbc.queryForObject("SELECT name FROM node", String.class), routed.dbReadOnly()});
                    List<String> roLookups = List.copyOf(routing.lookups);
                    String rw = routed.tx(false).execute(s -> routed.jdbc.queryForObject("SELECT name FROM node", String.class));
                    check(ro[0].equals(c.readOnlyTarget()) && ro[1].equals(c.pgReadOnly()) && rw.equals("primary"),
                            "routing " + c.name() + " ro=" + Arrays.toString(ro) + " rw=" + rw + " keys=" + roLookups);
                    System.out.printf("OBS routing %-40s readOnlyTx=%s pgReadOnly=%s routingKeys=%s readWriteTx=%s%n",
                            c.name(), ro[0], ro[1], c.routed() ? roLookups.toString() : "-", rw);
                }
            }
            System.out.println("PASS routing eagerConnectionWentToPrimary=true lazyProxyReachedReplica=true readOnlyDataSourceNotMarkedReadOnly=true");
        }
    }

    // 6. Persistence-context cost: snapshots, flush, allocation and commit time for many loaded entities.
    record Cost(long snapshots, long readOnlyEntries, long flushes, long allocBytes, long retainedBytes, long commitNanos) {}
    Cost load(boolean readOnly) {
        var tmx = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long baseline = heapAfterGc();
        long flushes = jpa.stats().getFlushCount();
        long[] commit = new long[2];
        Cost partial = jpa.tx(readOnly).execute(s -> {
            long before = tmx.getCurrentThreadAllocatedBytes();
            List<Post> posts = jpa.em.createQuery("select p from Post p", Post.class).getResultList();
            long alloc = tmx.getCurrentThreadAllocatedBytes() - before;
            long retained = heapAfterGc() - baseline;
            var pc = jpa.em.unwrap(SessionImplementor.class).getPersistenceContextInternal();
            long snapshots = 0, readOnlyEntries = 0;
            for (Post p : posts) {
                EntityEntry e = pc.getEntry(p);
                if (e.getLoadedState() != null) snapshots++;
                if (e.isReadOnly()) readOnlyEntries++;
            }
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override public void beforeCompletion() { commit[0] = System.nanoTime(); }
                @Override public void afterCompletion(int status) { commit[1] = System.nanoTime(); }
            });
            return new Cost(snapshots, readOnlyEntries, 0, alloc, retained, 0);
        });
        return new Cost(partial.snapshots(), partial.readOnlyEntries(), jpa.stats().getFlushCount() - flushes,
                partial.allocBytes(), partial.retainedBytes(), commit[1] - commit[0]);
    }
    void cost() {
        int n = 20_000;
        jpa.jdbc.execute("TRUNCATE post_record");
        jpa.jdbc.update("INSERT INTO post_record SELECT g, 'title-' || g, 0 FROM generate_series(1," + n + ") g");
        for (int i = 0; i < 3; i++) { load(true); load(false); }
        List<Cost> ro = new ArrayList<>(), rw = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            if (i % 2 == 0) { ro.add(load(true)); rw.add(load(false)); } else { rw.add(load(false)); ro.add(load(true)); }
        }
        for (var c : ro) check(c.snapshots() == 0 && c.readOnlyEntries() == n && c.flushes() == 0, "readOnly cost " + c);
        for (var c : rw) check(c.snapshots() == n && c.readOnlyEntries() == 0 && c.flushes() == 1, "read-write cost " + c);
        for (var entry : Map.of("true ", ro, "false", rw).entrySet().stream().sorted(Map.Entry.comparingByKey()).toList()) {
            var list = entry.getValue();
            long alloc = median(list.stream().mapToLong(Cost::allocBytes).toArray());
            long commit = median(list.stream().mapToLong(Cost::commitNanos).toArray());
            long retained = median(list.stream().mapToLong(Cost::retainedBytes).toArray());
            long minCommit = list.stream().mapToLong(Cost::commitNanos).min().orElseThrow();
            long maxCommit = list.stream().mapToLong(Cost::commitNanos).max().orElseThrow();
            System.out.printf("OBS cost readOnly=%s rows=%d snapshots=%d readOnlyEntries=%d commitFlushes=%d loadAllocKiB(median)=%d retainedKiB(median)=%d commitMs(median)=%.2f min=%.2f max=%.2f%n",
                    entry.getKey(), n, list.getFirst().snapshots(), list.getFirst().readOnlyEntries(), list.getFirst().flushes(),
                    alloc / 1024, retained / 1024, commit / 1e6, minCommit / 1e6, maxCommit / 1e6);
        }
        System.out.println("PASS cost rounds=7 warmup=3 (timings are local observations, not asserted)");
    }
    static long heapAfterGc() {
        var memory = ManagementFactory.getMemoryMXBean();
        for (int i = 0; i < 3; i++) System.gc();
        return memory.getHeapMemoryUsage().getUsed();
    }
    static long median(long[] values) { Arrays.sort(values); return values[values.length / 2]; }

    // 7. readOnly is a hint: with pgjdbc readOnlyMode=ignore, the database no longer rejects writes.
    void driverMode() {
        try (var ignore = pool(pg, "postgres", 1, "ignore"); var ignoreJpa = new Jpa(ignore, "ignore")) {
            reset();
            String[] flags = new String[2];
            String result = failure(() -> ignoreJpa.tx(true).executeWithoutResult(s -> {
                flags[0] = String.valueOf(ignoreJpa.jdbcReadOnly());
                flags[1] = ignoreJpa.dbReadOnly();
                ignoreJpa.jdbc.update("UPDATE post_record SET title='written-anyway' WHERE id=1");
            }));
            check(result.equals("none") && flags[0].equals("true") && flags[1].equals("off") && title().equals("written-anyway"), "readOnlyMode=ignore");
            System.out.printf("OBS driver readOnlyMode=ignore jdbcConnectionReadOnly=%s pgTransactionReadOnly=%s jdbcUpdate=committed dbTitle=%s%n",
                    flags[0], flags[1], title());
            System.out.println("PASS driver readOnlyIsHint=true");
        }
    }

    static String hikariVersion() {
        try (var in = HikariDataSource.class.getResourceAsStream("/META-INF/maven/com.zaxxer/HikariCP/pom.properties")) {
            var p = new Properties();
            if (in != null) p.load(in);
            return p.getProperty("version", "unknown");
        } catch (java.io.IOException e) { return "unknown"; }
    }

    // 8. Open EntityManager in View: one EntityManager spans a readOnly transaction and a later read-write one.
    void openInView() {
        for (boolean shareEntityManager : new boolean[]{false, true}) {
            reset();
            EntityManager request = shareEntityManager ? jpa.emf.createEntityManager() : null;
            if (request != null) TransactionSynchronizationManager.bindResource(jpa.emf, new EntityManagerHolder(request));
            var ro = new LinkedHashMap<String, Object>();
            String afterReadOnly, afterWrite;
            try {
                jpa.tx(true).executeWithoutResult(s -> {                 // readOnly service method changes an entity by mistake
                    Post p = jpa.em.find(Post.class, 1L);
                    p.rename("renamed-in-readonly");
                    ro.put("flush", jpa.session().getHibernateFlushMode().name());
                    ro.put("defaultReadOnly", jpa.session().isDefaultReadOnly());
                    ro.put("entityReadOnly", jpa.session().isReadOnly(p));
                    ro.put("pg", jpa.dbReadOnly());
                });
                afterReadOnly = title();
                jpa.counter.reset();
                jpa.tx(false).executeWithoutResult(s ->                  // later read-write method in the same request
                        jpa.jdbc.queryForObject("SELECT 1", Integer.class));
                afterWrite = title();
            } finally {
                if (request != null) { TransactionSynchronizationManager.unbindResource(jpa.emf); request.close(); }
            }
            int laterUpdates = jpa.counter.updates.get();
            boolean ok = ro.get("flush").equals("MANUAL") && ro.get("pg").equals("on") && afterReadOnly.equals("original")
                    && (shareEntityManager
                        ? ro.get("defaultReadOnly").equals(false) && ro.get("entityReadOnly").equals(false) && laterUpdates == 1 && afterWrite.equals("renamed-in-readonly")
                        : ro.get("defaultReadOnly").equals(true) && ro.get("entityReadOnly").equals(true) && laterUpdates == 0 && afterWrite.equals("original"));
            check(ok, "open-in-view shared=" + shareEntityManager + " " + ro + " afterReadOnly=" + afterReadOnly + " afterWrite=" + afterWrite + " updates=" + laterUpdates);
            System.out.printf("OBS open-in-view sharedEntityManager=%-5s readOnlyTx(flush=%s defaultReadOnly=%s entityReadOnly=%s pg=%s) afterReadOnlyTx=%s laterWriteTxUpdates=%d afterLaterWriteTx=%s%n",
                    shareEntityManager, ro.get("flush"), ro.get("defaultReadOnly"), ro.get("entityReadOnly"), ro.get("pg"), afterReadOnly, laterUpdates, afterWrite);
        }
        System.out.println("PASS open-in-view readOnlyChangeFlushedByLaterWriteTx=true");
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length == 0 ? "guided" : args[0];
        if (!Set.of("guided", "verify").contains(mode)) throw new IllegalArgumentException("Use guided or verify");
        if (mode.equals("guided")) {
            System.out.println("예상: readOnly 트랜잭션에서 엔티티를 바꾸면 예외가 날까요? 안쪽 메서드의 readOnly는 언제 적용될까요?");
            System.out.println("Enter: 임시 PostgreSQL에서 여덟 실험 실행 / q: 종료");
            if (new Scanner(System.in).nextLine().strip().equalsIgnoreCase("q")) return;
        }
        try (var pg = EmbeddedPostgres.builder().setServerConfig("listen_addresses", "127.0.0.1")
                .setServerConfig("shared_buffers", "32MB").setPort(0).start()) {
            var lab = new ReadOnlyLab(pg);
            try {
                try (var c = lab.main.getConnection()) {
                    System.out.printf("Spring=%s Hibernate=%s PostgreSQL=%s JDBC=%s HikariCP=%s%n", SpringVersion.getVersion(),
                            Version.getVersionString(), c.getMetaData().getDatabaseProductVersion(), c.getMetaData().getDriverVersion(),
                            hikariVersion());
                }
                lab.flags();
                lab.dirtyChecking();
                lab.writes();
                lab.propagation();
                lab.routing();
                lab.driverMode();
                lab.openInView();
                lab.cost();
                System.out.println("ALL 8 SCENARIOS PASSED. 비교 해설: spring/06-readonly-transactions/ANSWERS.md");
            } finally {
                lab.jpa.close();
                lab.main.close();
            }
        }
    }
}
