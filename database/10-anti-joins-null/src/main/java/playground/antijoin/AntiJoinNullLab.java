package playground.antijoin;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.sql.*;
import java.util.*;
public final class AntiJoinNullLab {

    static final List<String> FIELDS=List.of("Node Type","Relation Name","Index Name","Parent Relationship","Actual Rows","Actual Loops","Plan Rows","Shared Hit Blocks","Shared Read Blocks","Temp Read Blocks","Temp Written Blocks","Heap Fetches","Sort Method","Sort Space Type","Sort Space Used","Hash Batches","Peak Memory Usage","Strategy","Join Type","Rows Removed by Filter","Subplan Name","Filter");
    record Node(Map<String,String> values) {
        String s(String key){return values.getOrDefault(key,"");}
        long n(String key){String v=s(key);return v.isEmpty()?0:new java.math.BigDecimal(v).longValueExact();}
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
    static void expectIds(Connection c,String label,String sql,List<Integer> expected)throws Exception{
        var actual=sorted(ids(c,sql));check(actual.equals(expected),label+" exact IDs");
        System.out.println("VALUE "+label+"="+actual);
    }
    static String notIn(String c,String o){return "SELECT c.id FROM "+c+" c WHERE c.id NOT IN(SELECT o.customer_id FROM "+o+" o)";}
    static String notExists(String c,String o){return "SELECT c.id FROM "+c+" c WHERE NOT EXISTS(SELECT 1 FROM "+o+" o WHERE o.customer_id=c.id)";}
    static String leftJoin(String c,String o){return "SELECT c.id FROM "+c+" c LEFT JOIN "+o+" o ON o.customer_id=c.id WHERE o.customer_id IS NULL";}
    static String filtered(String c,String o){return "SELECT c.id FROM "+c+" c WHERE c.id NOT IN(SELECT o.customer_id FROM "+o+" o WHERE o.customer_id IS NOT NULL)";}
    static void run(Connection c,EmbeddedPostgres pg)throws Exception{
        exec(c,"CREATE TABLE tiny_customers(id integer PRIMARY KEY)");
        exec(c,"CREATE TABLE tiny_orders(id integer PRIMARY KEY,customer_id integer,note text)");
        exec(c,"INSERT INTO tiny_customers VALUES(1),(2),(3)");
        exec(c,"INSERT INTO tiny_orders VALUES(10,1,NULL),(11,1,'ok'),(12,2,'ok')");
        for(String q:List.of(notIn("tiny_customers","tiny_orders"),notExists("tiny_customers","tiny_orders"),leftJoin("tiny_customers","tiny_orders")))expectIds(c,"before_null",q,List.of(3));
        System.out.println("PASS 1 no-null inputs yield customer 3 in all three forms");

        exec(c,"INSERT INTO tiny_orders VALUES(13,NULL,'unknown customer')");
        expectIds(c,"null_not_in",notIn("tiny_customers","tiny_orders"),List.of());
        expectIds(c,"null_not_exists",notExists("tiny_customers","tiny_orders"),List.of(3));
        expectIds(c,"null_left_join",leftJoin("tiny_customers","tiny_orders"),List.of(3));
        expectIds(c,"null_filtered_not_in",filtered("tiny_customers","tiny_orders"),List.of(3));
        System.out.println("PASS 2 inner NULL changes NOT IN result; explicit filtering restores this fixture");

        String[][] truth={
            {"matched","SELECT 1 NOT IN(1,2,NULL)","f"},
            {"unmatched","SELECT 3 NOT IN(1,2,NULL)",null},
            {"outer_null_nonempty","SELECT NULL::integer NOT IN(SELECT customer_id FROM tiny_orders WHERE customer_id IS NOT NULL)",null},
            {"outer_null_not_exists","SELECT NOT EXISTS(SELECT 1 FROM tiny_orders WHERE customer_id=NULL::integer)","t"},
            {"outer_null_empty","SELECT NULL::integer NOT IN(SELECT customer_id FROM tiny_orders WHERE false)","t"}
        };
        for(var row:truth){String value=scalar(c,row[1]);check(Objects.equals(value,row[2]),"Truth table "+row[0]);System.out.println("VALUE truth_"+row[0]+"="+(value==null?"NULL":value));}
        System.out.println("PASS 3 outer NULL and empty subquery semantics");

        expectIds(c,"wrong_nullable_marker","SELECT c.id FROM tiny_customers c LEFT JOIN tiny_orders o ON o.customer_id=c.id WHERE o.note IS NULL",List.of(1,3));
        expectIds(c,"safe_primary_key_marker","SELECT c.id FROM tiny_customers c LEFT JOIN tiny_orders o ON o.customer_id=c.id WHERE o.id IS NULL",List.of(3));
        System.out.println("PASS 4 nullable note cannot distinguish unmatched rows");

        exec(c,"CREATE TABLE customers(id integer PRIMARY KEY) WITH (autovacuum_enabled=false)");
        exec(c,"INSERT INTO customers SELECT generate_series(1,1010)");
        exec(c,"CREATE TABLE orders(id integer NOT NULL,customer_id integer,payload text NOT NULL) WITH (autovacuum_enabled=false)");
        exec(c,"INSERT INTO orders SELECT i,1+(i-1)%1000,repeat(md5(i::text),4) FROM generate_series(1,100000) i");
        exec(c,"INSERT INTO orders VALUES(100001,NULL,repeat('x',128))");
        visible(c,"customers");visible(c,"orders");
        var expected=new ArrayList<Integer>();for(int i=1001;i<=1010;i++)expected.add(i);
        String ni=notIn("customers","orders"),ne=notExists("customers","orders"),lj=leftJoin("customers","orders"),fi=filtered("customers","orders");
        expectIds(c,"large_not_in",ni,List.of());
        for(String q:List.of(ne,lj,fi))expectIds(c,"large_absent",q,expected);
        var before=explain(c,"no_index_not_exists",ne);explain(c,"no_index_left_join",lj);explain(c,"no_index_not_in",ni);explain(c,"no_index_filtered",fi);
        check(before.relation("orders").n("Actual Rows")==100001,"Observed no-index full order scan");
        System.out.println("PASS 5 large fixture: compare semantics before comparing plans");

        exec(c,"CREATE INDEX orders_customer_idx ON orders(customer_id)");exec(c,"ANALYZE orders");
        for(String q:List.of(ne,lj,fi))expectIds(c,"indexed_absent",q,expected);
        var indexed=explain(c,"index_not_exists",ne);explain(c,"index_left_join",lj);explain(c,"index_not_in",ni);explain(c,"index_filtered",fi);
        check(indexed.root().n("Actual Rows")==10,"Indexed anti join output");
        System.out.println("PASS 6 index preserves answers while access paths may change");

        // Rebuild the isolated fixture so this comparison is independent of dead-tuple cleanup.
        exec(c,"TRUNCATE orders");
        exec(c,"INSERT INTO orders SELECT i,1+(i-1)%1000,repeat(md5(i::text),4) FROM generate_series(1,100000) i");
        exec(c,"ALTER TABLE orders ALTER COLUMN customer_id SET NOT NULL");visible(c,"orders");
        check("t".equals(scalar(c,"SELECT attnotnull FROM pg_attribute WHERE attrelid='orders'::regclass AND attname='customer_id'")),"NOT NULL constraint installed");
        for(String q:List.of(ni,ne,lj))expectIds(c,"not_null_absent",q,expected);
        explain(c,"not_null_not_in",ni);explain(c,"not_null_not_exists",ne);explain(c,"not_null_left_join",lj);
        System.out.println("PASS 7 non-null keys restore result equivalence; observe rather than assume plan rewrite");
        System.out.println("ALL 7 CHECKPOINTS PASSED");
    }

    public static void main(String[] args)throws Exception{
        String mode=args.length==0?"guided":args[0];
        if(!Set.of("guided","verify").contains(mode))throw new IllegalArgumentException("Use guided or verify");
        if(mode.equals("guided")){
            System.out.println("예상: 제외 목록에 NULL이 하나 섞이면 주문 없는 고객은 어떻게 조회될까?");
            System.out.println("Enter: 임시 PostgreSQL 실습 / q: 종료");
            var input=new Scanner(System.in);if(!input.hasNextLine()||input.nextLine().strip().equalsIgnoreCase("q"))return;
        }
        try(var pg=EmbeddedPostgres.builder().setServerConfig("listen_addresses","127.0.0.1").setServerConfig("shared_buffers","32MB").setPort(0).start();var c=pg.getPostgresDatabase().getConnection()){
            System.out.printf("Java=%s PostgreSQL=%s JDBC=%s blockSize=%s%n",System.getProperty("java.version"),c.getMetaData().getDatabaseProductVersion(),c.getMetaData().getDriverVersion(),scalar(c,"SHOW block_size"));
            settings(c);run(c,pg);
        }
    }
}
