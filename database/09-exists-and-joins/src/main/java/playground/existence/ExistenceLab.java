package playground.existence;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.sql.*;
import java.util.*;
public final class ExistenceLab {

    static final List<String> FIELDS=List.of("Node Type","Relation Name","Index Name","Parent Relationship","Actual Rows","Actual Loops","Plan Rows","Shared Hit Blocks","Shared Read Blocks","Temp Read Blocks","Temp Written Blocks","Heap Fetches","Sort Method","Sort Space Type","Sort Space Used","Hash Batches","Peak Memory Usage","Strategy","Join Type","Rows Removed by Filter","Rows Removed by Join Filter","Subplan Name");
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
    static void run(Connection c,EmbeddedPostgres pg)throws Exception{
        exec(c,"CREATE TABLE customers(id integer PRIMARY KEY) WITH (autovacuum_enabled=false)");
        exec(c,"INSERT INTO customers SELECT generate_series(1,1010)");
        exec(c,"CREATE TABLE orders(id integer NOT NULL,customer_id integer NOT NULL,payload text NOT NULL) WITH (autovacuum_enabled=false)");
        exec(c,"INSERT INTO orders SELECT i,1+(i-1)%1000,repeat(md5(i::text),4) FROM generate_series(1,100000) i");
        exec(c,"CREATE INDEX orders_customer_idx ON orders(customer_id)");
        visible(c,"customers");visible(c,"orders");
        check(number(c,"SELECT count(*) FROM orders")==100000,"Order fixture");
        String manyCount="SELECT count(*)>0 FROM orders WHERE customer_id<=500";
        String manyExists="SELECT EXISTS(SELECT 1 FROM orders WHERE customer_id<=500)";
        check("t".equals(scalar(c,manyCount))&&"t".equals(scalar(c,manyExists)),"Many match boolean equivalence");
        var count=explain(c,"many_count",manyCount);var exists=explain(c,"many_exists",manyExists);
        check(count.relation("orders").n("Actual Rows")==50000,"Count consumes all qualifying rows");
        check(exists.relation("orders").n("Actual Rows")==1,"Observed scalar EXISTS early stop");
        System.out.println("VALUE many count=true exists=true qualifying=50000");
        System.out.println("PASS 1 many matches: count input vs early stop");

        String absentCount="SELECT count(*)>0 FROM orders WHERE customer_id=2000";
        String absentExists="SELECT EXISTS(SELECT 1 FROM orders WHERE customer_id=2000)";
        check("f".equals(scalar(c,absentCount))&&"f".equals(scalar(c,absentExists)),"Absent boolean equivalence");
        var ac=explain(c,"absent_index_count",absentCount);var ae=explain(c,"absent_index_exists",absentExists);
        check(ac.has("Index Only Scan")&&ae.has("Index Only Scan"),"Observed empty index probes");
        check(ac.relation("orders").n("Actual Rows")==0&&ae.relation("orders").n("Actual Rows")==0,"No matching row");
        System.out.println("VALUE absent count=false exists=false");
        System.out.println("PASS 2 absent with index: both empty probes");

        exec(c,"DROP INDEX orders_customer_idx");
        var nc=explain(c,"absent_scan_count",absentCount);var ne=explain(c,"absent_scan_exists",absentExists);
        check(nc.has("Seq Scan")&&ne.has("Seq Scan"),"Observed absent full scans");
        check(nc.relation("orders").n("Rows Removed by Filter")==100000&&ne.relation("orders").n("Rows Removed by Filter")==100000,"Both inspect full table for absence");
        System.out.println("PASS 3 absent without index: both reject 100000 rows");

        exec(c,"CREATE INDEX orders_customer_idx ON orders(customer_id)");exec(c,"ANALYZE orders");
        String join="SELECT c.id FROM customers c JOIN orders o ON o.customer_id=c.id";
        String distinct="SELECT DISTINCT c.id FROM customers c JOIN orders o ON o.customer_id=c.id";
        String semi="SELECT c.id FROM customers c WHERE EXISTS(SELECT 1 FROM orders o WHERE o.customer_id=c.id)";
        var joined=ids(c,join);var unique=ids(c,distinct);var customers=ids(c,semi);
        var expected=new ArrayList<Integer>();for(int i=1;i<=1000;i++)expected.add(i);
        check(joined.size()==100000&&new HashSet<>(joined).size()==1000,"Join multiplicity");
        check(Collections.frequency(joined,42)==100,"Customer 42 repeats once per order");
        check(sorted(unique).equals(expected)&&sorted(customers).equals(expected),"Exact customer identities match without duplicate rows");
        var jp=explain(c,"join",join);var dp=explain(c,"distinct_join",distinct);var sp=explain(c,"exists_join",semi);
        check(jp.root().n("Actual Rows")==100000&&dp.root().n("Actual Rows")==1000&&sp.root().n("Actual Rows")==1000,"Plan output cardinalities");
        System.out.println("VALUE customer_results join=100000 distinct=1000 exists=1000 customer42_join_rows=100 missing_customers=10");
        System.out.println("PASS 4 exact customer identities and join multiplicity");

        exec(c,"DROP INDEX orders_customer_idx");
        var noIndex=explain(c,"exists_join_no_index",semi);
        check(sorted(ids(c,semi)).equals(expected),"Existence semantics independent of index");
        check(noIndex.relation("orders").n("Actual Rows")==100000,"Observed full order scan for customer existence set");
        System.out.println("PASS 5 correlated EXISTS can scan all orders in another plan");

        String trap="SELECT EXISTS(SELECT count(*) FROM orders WHERE customer_id=2000)";
        check(number(c,"SELECT count(*) FROM orders WHERE customer_id=2000")==0,"Empty aggregate value zero");
        check("t".equals(scalar(c,trap))&&"f".equals(scalar(c,absentExists)),"EXISTS tests aggregate output row, not count value");
        explain(c,"aggregate_trap",trap);
        System.out.println("VALUE aggregate_trap count=0 exists_count=true exists_row=false");
        System.out.println("PASS 6 aggregate output row is not source-row existence");
        System.out.println("ALL 6 CHECKPOINTS PASSED");
    }

    public static void main(String[] args)throws Exception{
        String mode=args.length==0?"guided":args[0];
        if(!Set.of("guided","verify").contains(mode))throw new IllegalArgumentException("Use guided or verify");
        if(mode.equals("guided")){
            System.out.println("예상: 존재 확인은 언제 일찍 끝날까? 주문 100개인 고객은 JOIN 결과에 몇 번 나올까?");
            System.out.println("Enter: 임시 PostgreSQL 실습 / q: 종료");
            var input=new Scanner(System.in);if(!input.hasNextLine()||input.nextLine().strip().equalsIgnoreCase("q"))return;
        }
        try(var pg=EmbeddedPostgres.builder().setServerConfig("listen_addresses","127.0.0.1").setServerConfig("shared_buffers","32MB").setPort(0).start();var c=pg.getPostgresDatabase().getConnection()){
            System.out.printf("Java=%s PostgreSQL=%s JDBC=%s blockSize=%s%n",System.getProperty("java.version"),c.getMetaData().getDatabaseProductVersion(),c.getMetaData().getDriverVersion(),scalar(c,"SHOW block_size"));
            settings(c);run(c,pg);
        }
    }
}
