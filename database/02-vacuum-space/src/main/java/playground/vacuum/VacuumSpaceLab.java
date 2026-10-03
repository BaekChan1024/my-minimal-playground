package playground.vacuum;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

public final class VacuumSpaceLab {
    private final DataSource db;
    record Size(long rows, long heap, long indexes, long total, long dead, long free) {}
    VacuumSpaceLab(DataSource db) { this.db = db; }
    static void check(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
    static void exec(Connection c, String sql) throws SQLException {
        try (var s = c.createStatement()) { s.execute(sql); }
    }
    static long number(Connection c, String sql) throws SQLException {
        try (var s = c.createStatement(); var r = s.executeQuery(sql)) { r.next(); return r.getLong(1); }
    }
    Size size(Connection c, String table) throws SQLException {
        // table names are fixed lab constants, never untrusted input.
        long rows = number(c, "SELECT count(*) FROM " + table);
        try (var s = c.createStatement(); var r = s.executeQuery("SELECT pg_relation_size('"+table+"'), "
                +"pg_indexes_size('"+table+"'), pg_total_relation_size('"+table+"'), "
                +"dead_tuple_count, free_space FROM pgstattuple('"+table+"')")) {
            r.next(); return new Size(rows,r.getLong(1),r.getLong(2),r.getLong(3),r.getLong(4),r.getLong(5));
        }
    }
    static void print(String stage, Size value) { System.out.println(stage+" "+value); }
    void seed(Connection c, String table, int rows) throws SQLException {
        exec(c,"CREATE TABLE "+table+"(id integer PRIMARY KEY, payload text NOT NULL) WITH (autovacuum_enabled=false)");
        exec(c,"INSERT INTO "+table+" SELECT i, repeat(md5(i::text),16) FROM generate_series(1,"+rows+") i");
        exec(c,"ANALYZE "+table);
    }
    void reuseAndRewrite(Connection c) throws SQLException {
        seed(c,"space_data",20000);
        var initial=size(c,"space_data");print("seed",initial);
        exec(c,"DELETE FROM space_data WHERE id % 10 <> 0");
        var deleted=size(c,"space_data");print("after-delete",deleted);
        check(deleted.rows()==2000 && deleted.heap()==initial.heap(),"DELETE reduces visible rows, not heap file length here");
        System.out.println("PASS delete-visible-rows-without-shrinking-heap");

        exec(c,"VACUUM (TRUNCATE FALSE, ANALYZE) space_data");
        var vacuumed=size(c,"space_data");print("after-vacuum",vacuumed);
        check(vacuumed.rows()==2000 && vacuumed.heap()==initial.heap(),"Ordinary VACUUM preserves heap length with truncation disabled");
        check(vacuumed.dead()==0 && vacuumed.free()>initial.free()+initial.heap()/2,"Most deleted space is reusable after cleanup; SELECT may already prune pages");
        System.out.println("PASS vacuum-leaves-reusable-space-inside-existing-file");

        exec(c,"INSERT INTO space_data SELECT i, repeat(md5(i::text),16) FROM generate_series(20001,38000) i");
        var reused=size(c,"space_data");print("after-reinsert",reused);
        check(reused.rows()==20000,"Reinserted 18000 rows");
        check(reused.heap()<=initial.heap()+8192,"Same-sized rows reuse old heap pages, at most one extra page");
        check(reused.free()<vacuumed.free()/2,"Free bytes consumed by new tuples");
        System.out.println("PASS reinsert-reuses-heap-space");

        exec(c,"DELETE FROM space_data WHERE id % 10 <> 0");
        exec(c,"VACUUM (TRUNCATE FALSE, ANALYZE) space_data");
        var beforeFull=size(c,"space_data");print("before-full",beforeFull);
        long oldFile=number(c,"SELECT pg_relation_filenode('space_data')");
        exec(c,"VACUUM (FULL, ANALYZE) space_data");
        var compact=size(c,"space_data");print("after-full",compact);
        long newFile=number(c,"SELECT pg_relation_filenode('space_data')");
        check(compact.rows()==beforeFull.rows() && compact.heap()<beforeFull.heap()/2,"FULL keeps live rows and compacts sparse table");
        check(newFile!=oldFile,"FULL rewrites relation storage");
        System.out.println("PASS full-rewrites-and-shrinks filenodeChanged=true");
    }
    void oldSnapshot(Connection c) throws SQLException {
        seed(c,"snapshot_data",10000);
        try (var reader=db.getConnection()) {
            reader.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            reader.setAutoCommit(false);
            check(number(reader,"SELECT count(*) FROM snapshot_data")==10000,"Snapshot established by SELECT");
            try {
                exec(c,"DELETE FROM snapshot_data WHERE id % 10 <> 0");
                var deleted=size(c,"snapshot_data");
                exec(c,"VACUUM (TRUNCATE FALSE, ANALYZE) snapshot_data");
                var retained=size(c,"snapshot_data");print("snapshot-open-after-vacuum",retained);
                check(number(reader,"SELECT count(*) FROM snapshot_data")==10000,"Old snapshot still sees deleted versions");
                check(retained.rows()==1000,"New reader sees committed deletion");
                check(retained.free()-deleted.free()<deleted.heap()/10,"Old versions not reclaimed while visible to snapshot");
                System.out.println("snapshot readers: old=10000 new=1000");
            } finally { reader.rollback(); }
        }
        var before=size(c,"snapshot_data");
        exec(c,"VACUUM (TRUNCATE FALSE, ANALYZE) snapshot_data");
        var released=size(c,"snapshot_data");print("snapshot-closed-after-vacuum",released);
        check(released.rows()==1000 && released.free()>before.free()+before.heap()/2,"Closing old snapshot permits cleanup");
        check(released.dead()==0,"No dead tuples after final cleanup");
        System.out.println("PASS old-snapshot-delays-reclamation");
    }
    void transactionBoundary(Connection c) throws SQLException {
        c.setAutoCommit(false);
        try {
            try { exec(c,"VACUUM space_data");throw new IllegalStateException("Expected transaction-block restriction"); }
            catch (SQLException expected) {
                check("25001".equals(expected.getSQLState()),"VACUUM refused inside transaction block");
                System.out.println("PASS vacuum-outside-transaction sqlState="+expected.getSQLState());
            }
        } finally { c.rollback();c.setAutoCommit(true); }
    }
    void run() throws Exception {
        try (var c=db.getConnection()) {
            // Temporary local cluster only; pgstattuple is a PostgreSQL supplied contrib module.
            exec(c,"CREATE EXTENSION pgstattuple");
            reuseAndRewrite(c);oldSnapshot(c);transactionBoundary(c);
            System.out.println("ALL 6 CHECKPOINTS PASSED. Byte counts describe this local run, not disk-capacity guarantees.");
        }
    }
    public static void main(String[] args) throws Exception {
        String mode=args.length==0?"guided":args[0];
        if(!Set.of("guided","verify").contains(mode))throw new IllegalArgumentException("Use guided or verify");
        if(mode.equals("guided")) {
            System.out.println("예상: 2만 행 중 90%를 DELETE하면 파일 크기도 90% 줄까요?");
            System.out.println("VACUUM 뒤 파일 크기가 같다면 무엇을 확인해야 할까요? 오래된 스냅샷이 열려 있다면?");
            System.out.println("Enter: 임시 PostgreSQL의 여섯 체크포인트 / q: 종료");
            var input=new Scanner(System.in);
            if(!input.hasNextLine() || input.nextLine().strip().equalsIgnoreCase("q"))return;
        }
        try(var pg=EmbeddedPostgres.builder().setServerConfig("listen_addresses","127.0.0.1")
                .setServerConfig("shared_buffers","32MB").setPort(0).start()) {
            var db=pg.getPostgresDatabase();
            try(var c=db.getConnection()) {
                System.out.printf("Java=%s PostgreSQL=%s JDBC=%s blockSize=%d%n",System.getProperty("java.version"),
                    c.getMetaData().getDatabaseProductVersion(),c.getMetaData().getDriverVersion(),number(c,"SHOW block_size"));
            }
            new VacuumSpaceLab(db).run();
        }
    }
}
