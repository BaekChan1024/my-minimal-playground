package playground.aggregation;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.sql.*;
import java.util.*;
public final class CountAggregationLab {

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
        exec(c,"CREATE TABLE metrics(id integer NOT NULL,bucket integer NOT NULL,amount integer,payload text NOT NULL) WITH(autovacuum_enabled=false)");
        exec(c,"INSERT INTO metrics SELECT i,i%100,CASE WHEN i%10=0 THEN NULL ELSE i END,repeat(md5(i::text),4) FROM generate_series(1,100000) i");
        exec(c,"ANALYZE metrics");
        String countAll="SELECT count(*) FROM metrics";
        var seq=explain(c,"count-without-index",countAll);
        check(seq.root().n("Actual Rows")==1 && seq.relation("metrics").n("Actual Rows")==100000 && seq.relation("metrics").s("Node Type").equals("Seq Scan"),"One result reads all input rows");
        try(var s=c.createStatement();var r=s.executeQuery("SELECT count(*),count(amount),sum(amount) FROM metrics")){
            r.next();check(r.getLong(1)==100000 && r.getLong(2)==90000 && r.getLong(3)==4500000000L,"Count and nullable amount semantics");
            System.out.println("VALUE counts all="+r.getLong(1)+" nonnull="+r.getLong(2)+" sum="+r.getLong(3));
        }
        System.out.println("PASS one-output-row-is-not-one-input-row-and-null-counts-differ");
        exec(c,"CREATE INDEX metrics_bucket_idx ON metrics(bucket)");
        visible(c,"metrics");
        var idx=explain(c,"count-with-index",countAll);
        check(idx.relation("metrics").s("Node Type").equals("Index Only Scan") && idx.relation("metrics").n("Heap Fetches")==0,"Prepared all-visible index-only count");
        check(idx.relation("metrics").n("Actual Rows")==100000 && number(c,countAll)==100000,"Index-only still visits all matching entries");
        check(idx.root().buffers()<seq.root().buffers(),"Smaller index reduces buffer work here");
        System.out.println("PASS index-only-reduces-buffer-work-not-counted-cardinality");
        String filtered="SELECT count(*) FROM metrics WHERE bucket=42";
        var small=explain(c,"count-bucket42",filtered);
        check(number(c,filtered)==1000 && small.relation("metrics").n("Actual Rows")==1000,"Filtered count input and value");
        check(small.root().buffers()<idx.root().buffers(),"Narrow filter reads less here");
        System.out.println("PASS filtered-count-narrows-input");
        String grouped="SELECT bucket,count(*),sum(amount) FROM metrics GROUP BY bucket";
        var group=explain(c,"grouped",grouped);
        check(group.root().n("Actual Rows")==100 && group.relation("metrics").n("Actual Rows")==100000,"100 output groups still read 100000 rows");
        Set<Integer> buckets=new HashSet<>();
        try(var s=c.createStatement();var r=s.executeQuery(grouped)){while(r.next()){
            int b=r.getInt(1);check(buckets.add(b) && b>=0 && b<100 && r.getLong(2)==1000,"Every group exactly once with 1000 rows");
            if(b%10==0)check(r.getObject(3)==null,"All-null group sum is null");
            else check(r.getLong(3)==49950000L+b*1000L,"Exact non-null group sum");
        }}
        check(buckets.size()==100,"All groups present");
        System.out.println("PASS group-output-cardinality-and-null-sums");
        try(var s=c.createStatement();var r=s.executeQuery("SELECT count(*),sum(amount) FROM metrics WHERE bucket=-1")){
            r.next();check(r.getLong(1)==0 && r.getObject(2)==null,"Empty aggregate count zero sum null");
            System.out.println("VALUE empty count="+r.getLong(1)+" sum="+r.getObject(2));
        }
        System.out.println("PASS empty-count-zero-sum-null");
        long estimateBefore=number(c,"SELECT reltuples::bigint FROM pg_class WHERE oid='metrics'::regclass");
        try(var reader=pg.getPostgresDatabase().getConnection()){
            reader.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);reader.setAutoCommit(false);
            long oldCount=number(reader,countAll);
            exec(c,"INSERT INTO metrics VALUES(100001,1,100001,'new-row')");
            long newCount=number(c,countAll);long heldCount=number(reader,countAll);
            check(oldCount==100000 && heldCount==100000 && newCount==100001,"Different visible snapshots have different exact counts");
            reader.commit();long refreshed=number(reader,countAll);check(refreshed==100001,"Next transaction sees committed row");reader.commit();
            long estimateAfterInsert=number(c,"SELECT reltuples::bigint FROM pg_class WHERE oid='metrics'::regclass");
            check(estimateBefore==100000 && estimateAfterInsert==100000,"Catalog estimate is not live exact count");
            exec(c,"ANALYZE metrics");long estimateAfterAnalyze=number(c,"SELECT reltuples::bigint FROM pg_class WHERE oid='metrics'::regclass");
            System.out.println("VALUE snapshots held="+heldCount+" fresh="+newCount+" nextTransaction="+refreshed);
            System.out.println("VALUE estimates before="+estimateBefore+" afterInsert="+estimateAfterInsert+" afterAnalyze="+estimateAfterAnalyze);
        }
        System.out.println("PASS exact-count-is-snapshot-dependent-catalog-is-estimate");
        System.out.println("ALL 6 CHECKPOINTS PASSED. Exact counts, estimates and cached business counters are different contracts.");
    }

    public static void main(String[] args)throws Exception{
        String mode=args.length==0?"guided":args[0];
        if(!Set.of("guided","verify").contains(mode))throw new IllegalArgumentException("Use guided or verify");
        if(mode.equals("guided")){
            System.out.println("예상: COUNT 결과가 한 행이면 입력도 적게 읽을까요? 인덱스만 읽으면 전체 개수를 바로 알까요? NULL과 스냅샷은 어떤 영향을 줄까요?");
            System.out.println("Enter: 임시 PostgreSQL 실습 / q: 종료");
            var input=new Scanner(System.in);if(!input.hasNextLine()||input.nextLine().strip().equalsIgnoreCase("q"))return;
        }
        try(var pg=EmbeddedPostgres.builder().setServerConfig("listen_addresses","127.0.0.1").setServerConfig("shared_buffers","32MB").setPort(0).start();var c=pg.getPostgresDatabase().getConnection()){
            System.out.printf("Java=%s PostgreSQL=%s JDBC=%s blockSize=%s%n",System.getProperty("java.version"),c.getMetaData().getDatabaseProductVersion(),c.getMetaData().getDriverVersion(),scalar(c,"SHOW block_size"));
            settings(c);run(c,pg);
        }
    }
}
