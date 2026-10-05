package playground.plan;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.sql.*;
import java.util.*;

public final class QueryPlanLab {
    record Plan(String node, long estimated, long actual, long loops, long hit, long read, String json) {
        long buffers() { return hit + read; }
        boolean index() { return json.contains("Index Scan") || json.contains("Index Only Scan"); }
    }
    static void check(boolean ok, String message) {
        if (!ok) throw new IllegalStateException(message);
    }
    static void exec(Connection c, String sql) throws SQLException {
        try (var s=c.createStatement()) { s.execute(sql); }
    }
    static String scalar(Connection c, String sql) throws SQLException {
        try(var s=c.createStatement(); var rs=s.executeQuery(sql)) { rs.next(); return rs.getString(1); }
    }
    static Plan explain(Connection c, String label, String query) throws SQLException {
        String json=scalar(c,"EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) "+query);
        Plan p;
        try(var s=c.prepareStatement("""
            WITH doc AS (SELECT ?::jsonb -> 0 -> 'Plan' AS p)
            SELECT p->>'Node Type', (p->>'Plan Rows')::bigint, (p->>'Actual Rows')::numeric::bigint,
                   (p->>'Actual Loops')::numeric::bigint, (p->>'Shared Hit Blocks')::bigint,
                   (p->>'Shared Read Blocks')::bigint FROM doc
            """)) {
            s.setString(1,json);
            try(var rs=s.executeQuery()) {
                rs.next();p=new Plan(rs.getString(1),rs.getLong(2),rs.getLong(3),rs.getLong(4),rs.getLong(5),rs.getLong(6),json);
            }
        }
        System.out.printf(Locale.ROOT,"RESULT %s node=%s estimated=%d actual=%d loops=%d sharedHit=%d sharedRead=%d%n",
            label,p.node(),p.estimated(),p.actual(),p.loops(),p.hit(),p.read());
        System.out.println("PLAN_JSON "+label+"\n"+json);
        return p;
    }
    static void stats(Connection c, String stage) throws SQLException {
        System.out.println("STATS "+stage+" "+scalar(c,"""
            SELECT json_build_object('n_distinct',n_distinct,'bucket_7_frequency',
              most_common_freqs[array_position(most_common_vals::text::int[],7)])::text FROM pg_stats
            WHERE schemaname='public' AND tablename='skew_data' AND attname='bucket'
            """));
    }
    static void run(Connection c) throws SQLException {
        exec(c,"SET max_parallel_workers_per_gather=0");
        exec(c,"SET jit=off");
        exec(c,"SET statement_timeout='30s'");
        for(String setting:List.of("shared_buffers","random_page_cost","seq_page_cost","default_statistics_target","max_parallel_workers_per_gather","jit"))
            System.out.println("SETTING "+setting+"="+scalar(c,"SHOW "+setting));
        exec(c,"CREATE TABLE range_data (id integer PRIMARY KEY,payload text NOT NULL) WITH (autovacuum_enabled=false)");
        exec(c,"INSERT INTO range_data SELECT i,repeat(md5(i::text),4) FROM generate_series(1,100000) i");
        exec(c,"ALTER TABLE range_data ALTER COLUMN id SET STATISTICS 1000");
        exec(c,"ANALYZE range_data");
        var narrow=explain(c,"range-narrow","SELECT * FROM range_data WHERE id<=100");
        check(narrow.index() && narrow.actual()==100 && narrow.loops()==1,"Narrow range uses index and returns 100 rows");
        System.out.println("PASS narrow-range-index");
        var broad=explain(c,"range-broad","SELECT * FROM range_data WHERE id<=90000");
        check(broad.node().equals("Seq Scan") && broad.actual()==90000,"Broad range chooses sequential scan");
        System.out.println("PASS broad-range-sequential");
        // Diagnostic comparison in this session only, then restore defaults.
        Plan scan;
        exec(c,"SET enable_indexscan=off");exec(c,"SET enable_bitmapscan=off");
        try { scan=explain(c,"range-narrow-sequential","SELECT * FROM range_data WHERE id<=100"); }
        finally { exec(c,"RESET enable_indexscan");exec(c,"RESET enable_bitmapscan"); }
        check(scan.node().equals("Seq Scan") && scan.actual()==narrow.actual() && scan.buffers()>narrow.buffers()*10,
            "Same 100 rows with much more buffer access under sequential scan");
        System.out.println("PASS same-result-different-buffer-work");

        exec(c,"CREATE TABLE skew_data (id integer PRIMARY KEY,bucket integer NOT NULL,payload text NOT NULL) WITH (autovacuum_enabled=false)");
        exec(c,"INSERT INTO skew_data SELECT i,i%1000,repeat(md5(i::text),4) FROM generate_series(1,100000) i");
        exec(c,"CREATE INDEX skew_bucket_idx ON skew_data(bucket)");
        // Full sample for this small table, not a production tuning recommendation.
        exec(c,"ALTER TABLE skew_data ALTER COLUMN bucket SET STATISTICS 1000");
        exec(c,"ANALYZE skew_data");
        var initial=explain(c,"stats-initial","SELECT * FROM skew_data WHERE bucket=7");
        check(initial.index() && initial.actual()==100 && initial.estimated()==100,"Initial uniform distribution estimated correctly");
        System.out.println("PASS initial-statistics");
        stats(c,"initial");
        exec(c,"UPDATE skew_data SET bucket=7 WHERE id<=90000");
        // Remove dead tuples before BOTH comparisons; deliberately omit ANALYZE.
        exec(c,"VACUUM (TRUNCATE FALSE) skew_data");
        var stale=explain(c,"stats-stale","SELECT * FROM skew_data WHERE bucket=7");
        check(stale.actual()==90010 && stale.estimated()*50<stale.actual(),"Stale distribution badly underestimates 90010 matches");
        System.out.println("PASS stale-statistics-underestimate");
        stats(c,"stale");
        exec(c,"ANALYZE skew_data");
        var fresh=explain(c,"stats-refreshed","SELECT * FROM skew_data WHERE bucket=7");
        check(fresh.actual()==stale.actual() && Math.abs(fresh.estimated()-fresh.actual())<=100 && fresh.node().equals("Seq Scan"),
            "ANALYZE corrects estimate and changes scan for unchanged data");
        System.out.println("PASS analyze-corrects-estimate-and-plan");
        stats(c,"refreshed");
        System.out.println("ALL 6 CHECKPOINTS PASSED. No timing threshold or universal selectivity threshold asserted.");
    }
    public static void main(String[] args) throws Exception {
        String mode=args.length==0?"guided":args[0];
        if(!Set.of("guided","verify").contains(mode)) throw new IllegalArgumentException("Use guided or verify");
        if(mode.equals("guided")) {
            System.out.println("예상: 10만 행 중 100행과 9만 행을 읽을 때 같은 인덱스를 쓸까요?");
            System.out.println("일치하는 행이 100개에서 90010개로 늘었는데 통계가 그대로라면 어떤 계획을 고를까요?");
            System.out.println("Enter: 임시 PostgreSQL의 여섯 체크포인트 / q: 종료");
            var input=new Scanner(System.in);
            if(!input.hasNextLine() || input.nextLine().strip().equalsIgnoreCase("q"))return;
        }
        try(var pg=EmbeddedPostgres.builder().setServerConfig("listen_addresses","127.0.0.1")
                .setServerConfig("shared_buffers","32MB").setPort(0).start();var c=pg.getPostgresDatabase().getConnection()) {
            System.out.printf("Java=%s PostgreSQL=%s JDBC=%s blockSize=%s%n",System.getProperty("java.version"),
                c.getMetaData().getDatabaseProductVersion(),c.getMetaData().getDriverVersion(),scalar(c,"SHOW block_size"));
            run(c);
        }
    }
}
