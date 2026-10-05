package playground.writecost;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.sql.*;
import java.util.*;

public final class IndexWriteCostLab {
    static final int ROWS = 10_000;
    record Plan(String type, long rows, long buffers, long records, long fpi, long walBytes) {}
    record Size(long heap, long indexes) {}
    record Update(Plan plan, long updated, long hot, long newPage, Size before, Size after) {}

    static void check(boolean ok, String message) {
        if (!ok) throw new IllegalStateException(message);
    }
    static void exec(Connection c, String sql) throws SQLException {
        try (var s = c.createStatement()) { s.execute(sql); }
    }
    static String scalar(Connection c, String sql) throws SQLException {
        try (var s = c.createStatement(); var r = s.executeQuery(sql)) { r.next(); return r.getString(1); }
    }
    static long number(Connection c, String sql) throws SQLException { return Long.parseLong(scalar(c, sql)); }
    static Plan explain(Connection c, String label, String sql) throws SQLException {
        String json = scalar(c, "EXPLAIN (ANALYZE, BUFFERS, WAL, TIMING OFF, FORMAT JSON) " + sql);
        try (var s = c.prepareStatement("""
            SELECT p->>'Node Type', (p->>'Actual Rows')::numeric::bigint,
              COALESCE((p->>'Shared Hit Blocks')::bigint,0)+COALESCE((p->>'Shared Read Blocks')::bigint,0),
              COALESCE((p->>'WAL Records')::bigint,0), COALESCE((p->>'WAL FPI')::bigint,0),
              COALESCE((p->>'WAL Bytes')::bigint,0)
            FROM (SELECT ?::jsonb->0->'Plan' AS p) x
            """)) {
            s.setString(1, json);
            try (var r = s.executeQuery()) {
                r.next();
                var plan = new Plan(r.getString(1), r.getLong(2), r.getLong(3), r.getLong(4), r.getLong(5), r.getLong(6));
                System.out.println("RESULT " + label + " " + plan);
                System.out.println("PLAN_JSON " + label + "\n" + json);
                return plan;
            }
        }
    }
    static Size size(Connection c, String table) throws SQLException {
        return new Size(number(c, "SELECT pg_relation_size('" + table + "')"),
                number(c, "SELECT pg_indexes_size('" + table + "')"));
    }
    static void create(Connection c, String table, int fillfactor, int indexes) throws SQLException {
        exec(c, "CREATE TABLE " + table + "(id integer NOT NULL, bucket integer NOT NULL, score integer NOT NULL, payload text NOT NULL) WITH(fillfactor=" + fillfactor + ", autovacuum_enabled=false)");
        if (indexes >= 1) exec(c, "CREATE INDEX " + table + "_id_idx ON " + table + "(id)");
        if (indexes >= 2) exec(c, "CREATE INDEX " + table + "_score_idx ON " + table + "(score)");
        if (indexes >= 3) exec(c, "CREATE INDEX " + table + "_bucket_id_idx ON " + table + "(bucket,id)");
    }
    static String insertSql(String table) {
        return "INSERT INTO " + table + " SELECT i,i%100,i,repeat(md5(i::text),4) FROM generate_series(1," + ROWS + ") i";
    }
    static Update update(Connection c, String table, int fillfactor, int indexes) throws SQLException {
        create(c, table, fillfactor, indexes);
        exec(c, insertSql(table));
        exec(c, "VACUUM (ANALYZE) " + table);
        Size before = size(c, table);
        // Equal checkpoint boundary for each measured write, on this disposable server only.
        exec(c, "CHECKPOINT");
        c.setAutoCommit(false);
        Update result;
        try {
            Plan plan = explain(c, table, "UPDATE " + table + " SET score=score+1");
            try (var s = c.createStatement(); var r = s.executeQuery("SELECT n_tup_upd,n_tup_hot_upd,n_tup_newpage_upd FROM pg_stat_xact_user_tables WHERE relid='" + table + "'::regclass")) {
                check(r.next(), "Transaction counters must exist");
                result = new Update(plan, r.getLong(1), r.getLong(2), r.getLong(3), before, size(c, table));
            }
            check(result.updated() == ROWS, "All rows updated exactly once");
            check(number(c, "SELECT count(*) FROM " + table + " WHERE score=id+1") == ROWS, "Updated values verified");
            c.commit();
        } catch (SQLException | RuntimeException e) { c.rollback(); throw e; }
        finally { c.setAutoCommit(true); }
        System.out.println("UPDATE_SUMMARY " + table + " " + result);
        return result;
    }
    static void run(Connection c) throws SQLException {
        exec(c, "SET max_parallel_workers_per_gather=0");
        exec(c, "SET jit=off");
        exec(c, "SET statement_timeout='30s'");
        for (String key : List.of("shared_buffers", "fsync", "synchronous_commit", "full_page_writes", "wal_level", "wal_compression", "checkpoint_timeout", "max_wal_size", "track_counts", "jit", "max_parallel_workers_per_gather"))
            System.out.println("SETTING " + key + "=" + scalar(c, "SHOW " + key));

        Map<Integer, Plan> inserts = new LinkedHashMap<>();
        Map<Integer, Size> sizes = new LinkedHashMap<>();
        for (int count : List.of(0, 1, 3)) {
            String table = "ins" + count;
            create(c, table, 100, count);
            exec(c, "CHECKPOINT");
            inserts.put(count, explain(c, table + "-insert", insertSql(table)));
            check(number(c, "SELECT count(*) FROM " + table) == ROWS, "Insert row count");
            sizes.put(count, size(c, table));
            exec(c, "ANALYZE " + table);
            System.out.println("SIZE " + table + " " + sizes.get(count));
        }
        for (String table : List.of("ins1", "ins3"))
            check(number(c, "SELECT count(*) FROM ((SELECT * FROM ins0 EXCEPT ALL SELECT * FROM " + table + ") UNION ALL (SELECT * FROM " + table + " EXCEPT ALL SELECT * FROM ins0)) differences") == 0, "Exact inserted data equality");
        System.out.println("PASS same-inserted-data-with-0-1-3-indexes");
        check(inserts.get(0).walBytes() < inserts.get(1).walBytes() && inserts.get(1).walBytes() < inserts.get(3).walBytes(), "More insert WAL for these B-tree indexes");
        check(sizes.get(0).heap() == sizes.get(1).heap() && sizes.get(1).heap() == sizes.get(3).heap(), "Equal heap bytes after insert");
        check(sizes.get(0).indexes() == 0 && sizes.get(1).indexes() > 0 && sizes.get(3).indexes() > sizes.get(1).indexes(), "Index allocation increases");
        System.out.println("PASS additional-indexes-add-insert-wal-and-space");

        Map<Integer, Plan> reads = new LinkedHashMap<>();
        for (int count : List.of(0, 1, 3)) {
            reads.put(count, explain(c, "ins" + count + "-read", "SELECT payload FROM ins" + count + " WHERE id=4242"));
            check(reads.get(count).rows() == 1, "One matching row");
            check("t".equals(scalar(c, "SELECT payload=repeat(md5('4242'),4) FROM ins" + count + " WHERE id=4242")), "Same read value");
        }
        check(reads.get(0).type().equals("Seq Scan") && reads.get(1).type().equals("Index Scan") && reads.get(3).type().equals("Index Scan"), "Chosen access paths");
        check(reads.get(1).buffers() < reads.get(0).buffers() && reads.get(1).buffers() == reads.get(3).buffers(), "Extra indexes add no buffer benefit for this lookup");
        System.out.println("PASS one-index-helps-this-read-extra-indexes-do-not");

        Update hot = update(c, "hot50", 50, 1);
        Update indexed = update(c, "indexed50", 50, 2);
        Update dense = update(c, "dense100", 100, 1);
        check(hot.hot() == ROWS && hot.newPage() == 0, "Reserved space permits all HOT updates here");
        check(indexed.hot() == 0 && indexed.newPage() == 0, "Changing B-tree key prevents HOT even with same-page space");
        check(indexed.plan().walBytes() > hot.plan().walBytes(), "Indexed score update emits more WAL here");
        System.out.println("PASS indexed-column-change-prevents-hot-despite-page-space");
        check(dense.hot() < hot.hot() && dense.newPage() > 0, "Non-indexed column alone does not guarantee HOT");
        System.out.println("PASS hot-also-needs-same-page-space");
        check(hot.before().heap() > dense.before().heap(), "Lower fillfactor reserves space at a heap allocation cost");
        check(hot.after().heap() == hot.before().heap() && dense.after().heap() > dense.before().heap(), "Dense first update needs more heap pages here");
        System.out.println("PASS fillfactor-space-tradeoff-observed");
        System.out.println("ALL 6 CHECKPOINTS PASSED. WAL bytes are not disk throughput or commit latency; exact counts can vary by environment.");
    }
    public static void main(String[] args) throws Exception {
        String mode = args.length == 0 ? "guided" : args[0];
        if (!Set.of("guided", "verify").contains(mode)) throw new IllegalArgumentException("Use guided or verify");
        if (mode.equals("guided")) {
            System.out.println("예상: 같은 1만 행을 넣을 때 인덱스 0·1·3개는 WAL과 공간이 같을까요?");
            System.out.println("score를 바꿀 때 score 인덱스 유무와 fillfactor 50·100이 HOT 갱신에 어떤 영향을 줄까요?");
            System.out.println("Enter: 임시 PostgreSQL 여섯 체크포인트 / q: 종료");
            var input = new Scanner(System.in);
            if (!input.hasNextLine() || input.nextLine().strip().equalsIgnoreCase("q")) return;
        }
        try (var pg = EmbeddedPostgres.builder().setServerConfig("listen_addresses", "127.0.0.1")
                .setServerConfig("shared_buffers", "32MB").setServerConfig("fsync", "on")
                .setServerConfig("synchronous_commit", "on").setServerConfig("full_page_writes", "on")
                .setServerConfig("wal_level", "replica").setServerConfig("wal_compression", "off")
                .setServerConfig("checkpoint_timeout", "1h").setServerConfig("max_wal_size", "1GB")
                .setPort(0).start(); var c = pg.getPostgresDatabase().getConnection()) {
            System.out.printf("Java=%s PostgreSQL=%s JDBC=%s blockSize=%s%n", System.getProperty("java.version"), c.getMetaData().getDatabaseProductVersion(), c.getMetaData().getDriverVersion(), scalar(c, "SHOW block_size"));
            run(c);
        }
    }
}
