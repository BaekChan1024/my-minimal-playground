package playground.joins;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.sql.*;
import java.util.*;
public final class JoinPlansLab {

    static final List<String> FIELDS=List.of("Node Type","Relation Name","Index Name","Parent Relationship","Actual Rows","Actual Loops","Plan Rows","Shared Hit Blocks","Shared Read Blocks","Temp Read Blocks","Temp Written Blocks","Heap Fetches","Sort Method","Sort Space Type","Sort Space Used","Hash Batches","Peak Memory Usage","Strategy");
    record Node(Map<String,String> values) {
        String s(String key){return values.getOrDefault(key,"");}
        long n(String key){String v=s(key);return v.isEmpty()?0:(long)Double.parseDouble(v);}
        long buffers(){return n("Shared Hit Blocks")+n("Shared Read Blocks");}
    }
    record Plan(List<Node> nodes) {
        Node root(){return nodes.get(0);}
        Node type(String type){return nodes.stream().filter(n->n.s("Node Type").equals(type)).findFirst().orElseThrow();}
        boolean has(String type){return nodes.stream().anyMatch(n->n.s("Node Type").equals(type));}
        Node relation(String relation){return nodes.stream().filter(n->n.s("Relation Name").equals(relation)).findFirst().orElseThrow();}
    }
    static void check(boolean ok,String text){if(!ok)throw new IllegalStateException(text);}
    static void exec(Connection c,String sql)throws SQLException{try(var s=c.createStatement()){s.execute(sql);}}
    static String scalar(Connection c,String sql)throws SQLException{try(var s=c.createStatement();var r=s.executeQuery(sql)){r.next();return r.getString(1);}}
    static long number(Connection c,String sql)throws SQLException{return Long.parseLong(scalar(c,sql));}
    static Plan explain(Connection c,String label,String sql)throws SQLException{
        String json=scalar(c,"EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) "+sql);
        String fields=String.join(",",FIELDS.stream().map(k->"p->>'"+k+"'").toList());
        var nodes=new ArrayList<Node>();
        try(var s=c.prepareStatement("""
            WITH RECURSIVE nodes(p,path) AS (
              SELECT ?::jsonb->0->'Plan',ARRAY[]::bigint[]
              UNION ALL SELECT child,path||ord FROM nodes,
              LATERAL jsonb_array_elements(COALESCE(p->'Plans','[]'::jsonb)) WITH ORDINALITY x(child,ord)
            ) SELECT %s FROM nodes ORDER BY path
            """.formatted(fields))){
            s.setString(1,json);
            try(var r=s.executeQuery()){while(r.next()){
                var values=new LinkedHashMap<String,String>();
                for(int i=0;i<FIELDS.size();i++){String v=r.getString(i+1);if(v!=null)values.put(FIELDS.get(i),v);}
                nodes.add(new Node(Collections.unmodifiableMap(values)));
            }}
        }
        var p=new Plan(List.copyOf(nodes));
        System.out.println("RESULT "+label+" root="+p.root().s("Node Type")+" rows="+p.root().n("Actual Rows")+" buffers="+p.root().buffers());
        for(var n:nodes)System.out.println("NODE "+label+" "+n.values());
        System.out.println("PLAN_JSON "+label+"\n"+json);
        return p;
    }
    static List<Integer> ids(Connection c,String sql)throws SQLException{
        var result=new ArrayList<Integer>();
        try(var s=c.createStatement();var r=s.executeQuery(sql)){while(r.next())result.add(r.getInt(1));}
        return result;
    }
    static List<Integer> sorted(List<Integer> rows){var copy=new ArrayList<>(rows);Collections.sort(copy);return copy;}
    static void visible(Connection c,String table)throws SQLException{
        boolean ok=false;
        for(int pass=0;pass<3;pass++){
            exec(c,"VACUUM (FREEZE, ANALYZE, DISABLE_PAGE_SKIPPING TRUE) "+table);
            ok="t".equals(scalar(c,"SELECT relallvisible=relpages FROM pg_class WHERE oid='"+table+"'::regclass"));
            if(ok)break;
        }
        check(ok,"All-visible setup failed");
        System.out.println("VISIBILITY "+table+" "+scalar(c,"SELECT json_build_object('pages',relpages,'allVisible',relallvisible)::text FROM pg_class WHERE oid='"+table+"'::regclass"));
    }
    static void settings(Connection c)throws SQLException{
        exec(c,"SET jit=off");exec(c,"SET max_parallel_workers_per_gather=0");exec(c,"SET statement_timeout='30s'");exec(c,"SET work_mem='64MB'");
        for(String k:List.of("shared_buffers","work_mem","seq_page_cost","random_page_cost","jit","max_parallel_workers_per_gather"))System.out.println("SETTING "+k+"="+scalar(c,"SHOW "+k));
    }
    static void run(Connection c,EmbeddedPostgres pg)throws SQLException{
        exec(c,"CREATE TABLE customers(id integer PRIMARY KEY, name text NOT NULL) WITH(autovacuum_enabled=false)");
        exec(c,"INSERT INTO customers SELECT i,'customer-'||i FROM generate_series(1,1000) i");
        exec(c,"CREATE TABLE orders(id integer NOT NULL, customer_id integer NOT NULL, amount integer NOT NULL, payload text NOT NULL) WITH(autovacuum_enabled=false)");
        exec(c,"INSERT INTO orders SELECT i,(i-1)%1000+1,i%10000,repeat(md5(i::text),4) FROM generate_series(1,100000) i");
        exec(c,"CREATE INDEX orders_customer_idx ON orders(customer_id,id) INCLUDE(amount)");
        visible(c,"customers");visible(c,"orders");
        String base="SELECT o.id,c.id AS customer_id,o.amount FROM customers c JOIN orders o ON o.customer_id=c.id";
        String narrow=base+" WHERE c.id BETWEEN 1 AND 3";
        var a=explain(c,"narrow",narrow);
        check(a.has("Nested Loop") && a.root().n("Actual Rows")==300,"Narrow nested loop emits 300 rows");
        var inner=a.relation("orders");
        check(inner.n("Actual Rows")==100 && inner.n("Actual Loops")==3,"100 rows per loop times 3 probes");
        var expected=new ArrayList<Integer>();for(int i=1;i<=100000;i++)if((i-1)%1000+1<=3)expected.add(i);
        check(sorted(ids(c,narrow)).equals(expected),"Exact narrow order IDs");
        System.out.println("PASS narrow-index-probes-and-rows-times-loops");
        var b=explain(c,"broad",base);
        check(b.has("Hash Join") && b.root().n("Actual Rows")==100000,"Broad hash join");
        var expectedAll=new ArrayList<Integer>();for(int i=1;i<=100000;i++)expectedAll.add(i);
        check(sorted(ids(c,base)).equals(expectedAll),"Every order appears exactly once");
        System.out.println("PASS broad-hash-join-exact-results");
        String ordered=base+" ORDER BY c.id";
        var m=explain(c,"ordered",ordered);
        check(m.has("Merge Join") && !m.has("Sort") && m.root().n("Actual Rows")==100000,"Ordered merge join uses existing index order");
        check(sorted(ids(c,ordered)).equals(expectedAll),"Same order ID multiset for merge");
        int prior=0;try(var s=c.createStatement();var r=s.executeQuery(ordered)){while(r.next()){int id=r.getInt(2);check(id>=prior,"Nondecreasing customer order");prior=id;}}
        System.out.println("PASS ordered-merge-without-extra-sort");
        exec(c,"DROP INDEX orders_customer_idx");
        var noIndex=explain(c,"narrow-without-index",narrow);
        check(sorted(ids(c,narrow)).equals(expected),"Index removal preserves exact result");
        check(noIndex.relation("orders").s("Node Type").equals("Seq Scan") && noIndex.relation("orders").n("Actual Rows")==100000,"No probe index scans full orders");
        check(noIndex.root().buffers()>a.root().buffers(),"More buffer work without the probe index here");
        System.out.println("PASS same-narrow-result-with-more-input-work-without-index");
        System.out.println("ALL 4 CHECKPOINTS PASSED. No join method was forced; buffer counts are not latency ratios.");
    }

    public static void main(String[] args)throws Exception{
        String mode=args.length==0?"guided":args[0];
        if(!Set.of("guided","verify").contains(mode))throw new IllegalArgumentException("Use guided or verify");
        if(mode.equals("guided")){
            System.out.println("예상: 고객 3명과 전체 고객의 주문 연결은 같은 방법일까요? 고객 순 정렬을 요구하면 어떻게 달라질까요?");
            System.out.println("Enter: 임시 PostgreSQL 실습 / q: 종료");
            var input=new Scanner(System.in);if(!input.hasNextLine()||input.nextLine().strip().equalsIgnoreCase("q"))return;
        }
        try(var pg=EmbeddedPostgres.builder().setServerConfig("listen_addresses","127.0.0.1").setServerConfig("shared_buffers","32MB").setPort(0).start();var c=pg.getPostgresDatabase().getConnection()){
            System.out.printf("Java=%s PostgreSQL=%s JDBC=%s blockSize=%s%n",System.getProperty("java.version"),c.getMetaData().getDatabaseProductVersion(),c.getMetaData().getDriverVersion(),scalar(c,"SHOW block_size"));
            settings(c);run(c,pg);
        }
    }
}
