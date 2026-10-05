package playground.sorting;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.sql.*;
import java.util.*;
public final class SortMemoryLab {

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
        exec(c,"CREATE TABLE sort_data(id integer NOT NULL,sort_key text COLLATE \"C\" NOT NULL,payload text NOT NULL) WITH(autovacuum_enabled=false)");
        exec(c,"INSERT INTO sort_data SELECT i,md5(i::text),repeat(md5(i::text),4) FROM generate_series(1,100000) i");
        exec(c,"ANALYZE sort_data");
        String full="SELECT id,sort_key,payload FROM sort_data ORDER BY sort_key,id";
        exec(c,"SET work_mem='64kB'");
        var low=explain(c,"full-64kB",full);
        var ls=low.type("Sort");
        check(ls.s("Sort Space Type").equals("Disk") && low.root().n("Temp Written Blocks")>0 && ls.n("Actual Rows")==100000,"Small work_mem spills full sort");
        var lowIds=ids(c,full);
        var expected=new ArrayList<Integer>();for(int i=1;i<=100000;i++)expected.add(i);
        check(sorted(lowIds).equals(expected),"All IDs appear exactly once");
        System.out.println("PASS full-sort-spills-with-small-work-mem");
        exec(c,"SET work_mem='64MB'");
        var high=explain(c,"full-64MB",full);
        check(high.type("Sort").s("Sort Space Type").equals("Memory") && high.root().n("Temp Written Blocks")==0,"Larger work_mem holds this sort in memory");
        check(ids(c,full).equals(lowIds),"Exact ordered IDs equal at both memory budgets");
        System.out.println("PASS larger-memory-same-result-without-temp-write");
        exec(c,"SET work_mem='64kB'");
        String top=full+" LIMIT 20";
        var limited=explain(c,"top20-64kB",top);
        check(limited.type("Sort").s("Sort Method").equals("top-N heapsort") && limited.root().n("Temp Written Blocks")==0,"Bounded top-N sort fits this small budget");
        check(limited.root().n("Actual Rows")==20 && limited.relation("sort_data").n("Actual Rows")==100000,"LIMIT output does not mean only 20 input rows");
        check(ids(c,top).equals(lowIds.subList(0,20)),"Exact top20 equality");
        System.out.println("PASS top-n-keeps-small-state-but-reads-all-input");
        exec(c,"CREATE INDEX sort_data_order_idx ON sort_data(sort_key,id)");
        exec(c,"ANALYZE sort_data");
        var indexed=explain(c,"top20-index",top);
        check(!indexed.has("Sort") && indexed.relation("sort_data").s("Node Type").equals("Index Scan"),"Matching index removes sort");
        check(indexed.relation("sort_data").n("Actual Rows")==20 && indexed.root().buffers()<limited.root().buffers(),"Matching index stops after 20 output rows here");
        check(ids(c,top).equals(lowIds.subList(0,20)),"Same ordered top20 with index");
        System.out.println("PASS matching-order-index-avoids-full-input-sort");
        exec(c,"RESET work_mem");
        System.out.println("ALL 4 CHECKPOINTS PASSED. work_mem is per operation, not a total server cap; temp blocks are not physical disk throughput.");
    }

    public static void main(String[] args)throws Exception{
        String mode=args.length==0?"guided":args[0];
        if(!Set.of("guided","verify").contains(mode))throw new IllegalArgumentException("Use guided or verify");
        if(mode.equals("guided")){
            System.out.println("예상: work_mem을 늘리는 것과 LIMIT 20을 붙이는 것은 같은 효과일까요? 정렬 인덱스가 있으면 몇 행을 읽을까요?");
            System.out.println("Enter: 임시 PostgreSQL 실습 / q: 종료");
            var input=new Scanner(System.in);if(!input.hasNextLine()||input.nextLine().strip().equalsIgnoreCase("q"))return;
        }
        try(var pg=EmbeddedPostgres.builder().setServerConfig("listen_addresses","127.0.0.1").setServerConfig("shared_buffers","32MB").setPort(0).start();var c=pg.getPostgresDatabase().getConnection()){
            System.out.printf("Java=%s PostgreSQL=%s JDBC=%s blockSize=%s%n",System.getProperty("java.version"),c.getMetaData().getDatabaseProductVersion(),c.getMetaData().getDriverVersion(),scalar(c,"SHOW block_size"));
            settings(c);run(c,pg);
        }
    }
}
