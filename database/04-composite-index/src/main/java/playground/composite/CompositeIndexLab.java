package playground.composite;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.sql.*;
import java.util.*;

public final class CompositeIndexLab {
    record Node(String type,String index,String direction,long estimated,long actual,long loops,long hit,long read,long searches,long heap) {
        long buffers(){return hit+read;}
    }
    record Plan(List<Node> nodes) {
        Node root(){return nodes.get(0);}
        boolean sorted(){return nodes.stream().anyMatch(n->n.type().contains("Sort"));}
        Node scan(){return nodes.stream().filter(n->n.type().contains("Scan")).findFirst().orElseThrow();}
    }
    static void check(boolean ok,String message){if(!ok)throw new IllegalStateException(message);}
    static void exec(Connection c,String sql)throws SQLException{try(var s=c.createStatement()){s.execute(sql);}}
    static String scalar(Connection c,String sql)throws SQLException{try(var s=c.createStatement();var rs=s.executeQuery(sql)){rs.next();return rs.getString(1);}}
    static Plan explain(Connection c,String label,String sql)throws SQLException{
        String json=scalar(c,"EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) "+sql);
        var nodes=new ArrayList<Node>();
        try(var s=c.prepareStatement("""
            WITH RECURSIVE nodes(p,path) AS (
              SELECT ?::jsonb->0->'Plan', ARRAY[]::bigint[]
              UNION ALL
              SELECT child, path||ord FROM nodes,
                LATERAL jsonb_array_elements(COALESCE(p->'Plans','[]'::jsonb)) WITH ORDINALITY AS x(child,ord)
            ) SELECT p->>'Node Type',p->>'Index Name',p->>'Scan Direction',
              (p->>'Plan Rows')::numeric::bigint,(p->>'Actual Rows')::numeric::bigint,
              (p->>'Actual Loops')::numeric::bigint,(p->>'Shared Hit Blocks')::bigint,
              (p->>'Shared Read Blocks')::bigint,(p->>'Index Searches')::bigint,
              (p->>'Heap Fetches')::bigint FROM nodes ORDER BY path
            """)){
            s.setString(1,json);
            try(var rs=s.executeQuery()){while(rs.next())nodes.add(new Node(rs.getString(1),rs.getString(2),rs.getString(3),rs.getLong(4),rs.getLong(5),rs.getLong(6),rs.getLong(7),rs.getLong(8),rs.getLong(9),rs.getLong(10)));}
        }
        var p=new Plan(List.copyOf(nodes));
        System.out.printf("RESULT %s root=%s actual=%d rootBuffers=%d sort=%s%n",label,p.root().type(),p.root().actual(),p.root().buffers(),p.sorted());
        for(var n:nodes)System.out.println("NODE "+label+" "+n);
        System.out.println("PLAN_JSON "+label+"\n"+json);
        return p;
    }
    static List<Integer> seqs(Connection c,String sql)throws SQLException{
        var list=new ArrayList<Integer>();
        try(var s=c.createStatement();var rs=s.executeQuery(sql)){while(rs.next())list.add(rs.getInt("seq"));}
        return list;
    }
    static List<Integer> sorted(List<Integer> rows){var copy=new ArrayList<>(rows);Collections.sort(copy);return copy;}
    static void seed(Connection c,String table,String keys)throws SQLException{
        exec(c,"CREATE TABLE "+table+"(tenant_id integer NOT NULL,seq integer NOT NULL,payload text NOT NULL) WITH(autovacuum_enabled=false)");
        exec(c,"INSERT INTO "+table+" SELECT i%100,i,repeat(md5(i::text),4) FROM generate_series(1,100000) i");
        exec(c,"CREATE INDEX "+table+"_idx ON "+table+"("+keys+")");
        exec(c,"ALTER TABLE "+table+" ALTER COLUMN tenant_id SET STATISTICS 1000");
        exec(c,"ALTER TABLE "+table+" ALTER COLUMN seq SET STATISTICS 1000");
        exec(c,"VACUUM (ANALYZE) "+table);
    }
    static String select(String table,String suffix){return "SELECT tenant_id,seq FROM "+table+" "+suffix;}
    static void run(Connection c)throws SQLException{
        exec(c,"SET max_parallel_workers_per_gather=0");exec(c,"SET jit=off");exec(c,"SET statement_timeout='30s'");
        for(String key:List.of("shared_buffers","seq_page_cost","random_page_cost","max_parallel_workers_per_gather","jit"))System.out.println("SETTING "+key+"="+scalar(c,"SHOW "+key));
        seed(c,"tenant_first","tenant_id,seq");seed(c,"seq_first","seq,tenant_id");seed(c,"mixed_order","tenant_id ASC,seq DESC");
        for(String t:List.of("tenant_first","seq_first","mixed_order")) {
            // Verify the setup instead of assuming a single VACUUM marks every page all-visible.
            boolean visible=false;
            for(int pass=1;pass<=3;pass++) {
                exec(c,"VACUUM (FREEZE, ANALYZE, DISABLE_PAGE_SKIPPING TRUE) "+t);
                visible="t".equals(scalar(c,"SELECT relallvisible=relpages FROM pg_class WHERE oid='"+t+"'::regclass"));
                if(visible)break;
            }
            check(visible,"Setup requires all-visible pages for index-only comparison");
            System.out.println("VISIBILITY "+t+" "+scalar(c,"SELECT json_build_object('pages',relpages,'allVisible',relallvisible)::text FROM pg_class WHERE oid='"+t+"'::regclass"));
        }
        String range="WHERE tenant_id=42 AND seq BETWEEN 20000 AND 80000";
        var a=explain(c,"range-tenant-first",select("tenant_first",range));
        var b=explain(c,"range-seq-first",select("seq_first",range));
        check(a.root().actual()==600 && b.root().actual()==600,"Same 600 matching rows");
        check(sorted(seqs(c,select("tenant_first",range))).equals(sorted(seqs(c,select("seq_first",range)))),"Exact result equality for two index orders");
        check(a.scan().type().equals("Index Only Scan") && b.scan().type().equals("Index Only Scan") && a.scan().heap()==0 && b.scan().heap()==0,"Index-only comparisons avoid heap fetching");
        check(a.root().buffers()<b.root().buffers(),"Equality-leading key narrows buffer work in this data set");
        System.out.println("PASS key-order-same-result-different-buffer-work");
        var reordered=explain(c,"predicate-text-reordered",select("tenant_first","WHERE seq BETWEEN 20000 AND 80000 AND tenant_id=42"));
        check(reordered.scan().index().equals(a.scan().index()) && reordered.root().actual()==600 && !reordered.sorted(),"WHERE text order does not change selected index here");
        System.out.println("PASS predicate-text-order-is-not-index-key-order");
        String latest="WHERE tenant_id=42 ORDER BY seq DESC LIMIT 20";
        var ta=explain(c,"tenant-latest-tenant-first",select("tenant_first",latest));
        var tb=explain(c,"tenant-latest-seq-first",select("seq_first",latest));
        var expectedTenant=new ArrayList<Integer>();for(int v=99942;expectedTenant.size()<20;v-=100)expectedTenant.add(v);
        check(seqs(c,select("tenant_first",latest)).equals(expectedTenant) && seqs(c,select("seq_first",latest)).equals(expectedTenant),"Exact per-tenant top 20");
        check(!ta.sorted() && !tb.sorted() && "Backward".equals(ta.scan().direction()) && "Backward".equals(tb.scan().direction()),"Both can deliver order backward");
        System.out.println("PASS fixed-tenant-backward-order-without-sort");
        String global="ORDER BY seq DESC LIMIT 20";
        var ga=explain(c,"global-latest-tenant-first",select("tenant_first",global));
        var gb=explain(c,"global-latest-seq-first",select("seq_first",global));
        var expectedGlobal=new ArrayList<Integer>();for(int v=100000;expectedGlobal.size()<20;v--)expectedGlobal.add(v);
        check(seqs(c,select("tenant_first",global)).equals(expectedGlobal) && seqs(c,select("seq_first",global)).equals(expectedGlobal),"Exact global top 20");
        check(ga.sorted() && !gb.sorted() && ga.scan().actual()==100000 && gb.scan().actual()==20,"Global seq order changes sorting and input work");
        System.out.println("PASS global-order-favors-seq-leading-index");
        String missing="WHERE seq BETWEEN 99990 AND 100000";
        var skip=explain(c,"missing-leading-key",select("tenant_first",missing));
        check(skip.root().actual()==11 && "tenant_first_idx".equals(skip.scan().index()) && skip.scan().searches()>1,"PG18 uses multiple index searches without tenant equality here");
        System.out.println("PASS missing-leading-key-still-uses-index-with-multiple-searches");
        String mixed="ORDER BY tenant_id ASC,seq DESC LIMIT 20";
        var ma=explain(c,"mixed-default-index",select("tenant_first",mixed));
        var mb=explain(c,"mixed-matching-index",select("mixed_order",mixed));
        var expectedMixed=new ArrayList<Integer>();for(int v=100000;expectedMixed.size()<20;v-=100)expectedMixed.add(v);
        check(seqs(c,select("tenant_first",mixed)).equals(expectedMixed) && seqs(c,select("mixed_order",mixed)).equals(expectedMixed),"Exact mixed-order top 20");
        check(ma.sorted() && !mb.sorted() && mb.scan().actual()==20,"Mixed direction needs matching order to avoid sorting here");
        System.out.println("PASS mixed-direction-matching-index-removes-sort");
        System.out.println("ALL 6 CHECKPOINTS PASSED. Buffer observations are not latency guarantees; no scan type was forced.");
    }
    public static void main(String[] args)throws Exception{
        String mode=args.length==0?"guided":args[0];if(!Set.of("guided","verify").contains(mode))throw new IllegalArgumentException("Use guided or verify");
        if(mode.equals("guided")){
            System.out.println("예상: (tenant_id,seq)와 (seq,tenant_id)는 같은 조건에서 같은 양을 읽을까요?");
            System.out.println("회원 한 명의 최신20개와 전체 최신20개에 같은 인덱스가 유리할까요? 선두 조건이 없으면 인덱스를 전혀 못 쓸까요?");
            System.out.println("Enter: 임시 PostgreSQL 여섯 체크포인트 / q: 종료");
            var input=new Scanner(System.in);if(!input.hasNextLine()||input.nextLine().strip().equalsIgnoreCase("q"))return;
        }
        try(var pg=EmbeddedPostgres.builder().setServerConfig("listen_addresses","127.0.0.1").setServerConfig("shared_buffers","32MB").setPort(0).start();var c=pg.getPostgresDatabase().getConnection()){
            System.out.printf("Java=%s PostgreSQL=%s JDBC=%s blockSize=%s%n",System.getProperty("java.version"),c.getMetaData().getDatabaseProductVersion(),c.getMetaData().getDriverVersion(),scalar(c,"SHOW block_size"));run(c);
        }
    }
}
