package playground.search;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.sql.*;
import java.util.*;
import java.nio.file.*;
import java.io.*;

public class SearchLab {
    static final String WHERE=" FROM content_read c WHERE c.channel='BLOG' AND EXISTS (SELECT 1 FROM content v WHERE v.id=c.id AND v.channel=c.channel AND v.status='PUBLISHED') AND (c.title ILIKE ? OR c.excerpt ILIKE ? OR c.body ILIKE ?)";
    static int checks;
    static void check(String name,boolean ok){if(!ok)throw new AssertionError(name);checks++;System.out.println("PASS "+name);}
    static void exec(Connection c,String sql)throws SQLException{try(var s=c.createStatement()){s.execute(sql);}}
    static String scalar(Connection c,String sql)throws SQLException{try(var s=c.createStatement();var r=s.executeQuery(sql)){r.next();return r.getString(1);}}
    static String pattern(String q){return "%"+q.replace("\\","\\\\").replace("%","\\%").replace("_","\\_")+"%";}
    static List<Long> query(Connection c,String tableSql,String q)throws SQLException{
        try(var s=c.prepareStatement(tableSql)){for(int i=1;i<=3;i++)s.setString(i,pattern(q));try(var r=s.executeQuery()){List<Long> out=new ArrayList<>();while(r.next())out.add(r.getLong(1));return out;}}
    }
    static String explain(Connection c,String sql,String q)throws SQLException{
        try(var s=c.prepareStatement("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) "+sql)){for(int i=1;i<=3;i++)s.setString(i,pattern(q));try(var r=s.executeQuery()){r.next();return r.getString(1);}}
    }
    static void setup(Connection c,int n)throws SQLException{
        exec(c,"DROP TABLE IF EXISTS content_read,content");
        exec(c,"CREATE TABLE content(id bigint PRIMARY KEY,channel text NOT NULL,status text NOT NULL)");
        exec(c,"CREATE TABLE content_read(id bigint PRIMARY KEY,channel text NOT NULL,title text NOT NULL,excerpt text NOT NULL,body text NOT NULL,published_at date NOT NULL)");
        exec(c,"INSERT INTO content SELECT i,'BLOG',CASE WHEN i%17=0 THEN 'DRAFT' ELSE 'PUBLISHED' END FROM generate_series(1,"+n+")i");
        exec(c,"INSERT INTO content_read SELECT i,'BLOG','note '||i||CASE WHEN i%5000=1 THEN ' 희귀표식' ELSE '' END,CASE WHEN i%2=0 THEN '검색 안내' ELSE '일반 안내' END,repeat(md5(i::text)||' storage message cache ',12)||CASE WHEN i%5000=2 THEN ' needlemarker' ELSE '' END,DATE '2026-01-01'+(i%365)::int FROM generate_series(1,"+n+")i");
        // Keep baseline index shape used by the source: no extra id ordering key.
        exec(c,"CREATE INDEX channel_pub ON content_read(channel,published_at DESC)");
        exec(c,"CREATE INDEX channel_status_pub ON content(channel,status)");
        exec(c,"VACUUM (ANALYZE) content");exec(c,"VACUUM (ANALYZE) content_read");
    }
    static void semanticChecks(Connection c)throws SQLException{
        exec(c,"CREATE TEMP TABLE literal_cases(id bigint,title text,excerpt text,body text)");
        exec(c,"INSERT INTO literal_cases VALUES (1,'100% done','',''),(2,'100X done','',''),(3,'a_b','',''),(4,'axb','',''),(5,'red','blue',''),(6,'red blue','','')");
        String w="SELECT id FROM literal_cases WHERE title ILIKE ? OR excerpt ILIKE ? OR body ILIKE ? ORDER BY id";
        check("literal-percent",query(c,w,"100%").equals(List.of(1L)));
        check("literal-underscore",query(c,w,"a_b").equals(List.of(3L)));
        check("field-boundary-preserved",query(c,w,"red blue").equals(List.of(6L)));
        check("case-insensitive-ascii",query(c,w,"DONE").equals(List.of(1L,2L)));
    }
    public static void main(String[] args)throws Exception{
        if(args.length==0||!args[0].equals("verify")){
            System.out.println("예상: 희귀와 희귀표식은 같은 결과일 때 같은 인덱스를 쓸까요? LIMIT 20이 count 비용도 줄일까요? Enter 실행, q 종료.");
            if("q".equalsIgnoreCase(new BufferedReader(new InputStreamReader(System.in)).readLine()))return;
        }
        Path out=Path.of("build/search-evidence");Files.createDirectories(out);
        try(var pg=EmbeddedPostgres.builder().setPort(0).start();var c=pg.getPostgresDatabase().getConnection();var log=Files.newBufferedWriter(out.resolve("plans.jsonl"))){
            exec(c,"SET max_parallel_workers_per_gather=0");exec(c,"SET jit=off");exec(c,"SET statement_timeout='30s'");
            exec(c,"CREATE EXTENSION pg_trgm");
            System.out.println("PostgreSQL="+scalar(c,"SHOW server_version")+" pg_trgm="+scalar(c,"SELECT extversion FROM pg_extension WHERE extname='pg_trgm'")+" JDBC="+c.getMetaData().getDriverVersion());
            System.out.println("lc_ctype="+scalar(c,"SELECT datctype FROM pg_database WHERE datname=current_database()")+" collation="+scalar(c,"SELECT datcollate FROM pg_database WHERE datname=current_database()")+" encoding="+scalar(c,"SHOW server_encoding"));
            semanticChecks(c);
            for(int n:new int[]{5000,50000}){
                setup(c,n);
                Map<String,List<Long>> baseline=new HashMap<>();
                for(String phase:List.of("btree","trigram","trigram_ordered")){
                    if(phase.equals("trigram")){
                        for(String col:List.of("title","excerpt","body"))exec(c,"CREATE INDEX trgm_"+col+" ON content_read USING gin ("+col+" gin_trgm_ops)");
                        exec(c,"ANALYZE content_read");
                        System.out.println("SIZE rows="+n+" tableBytes="+scalar(c,"SELECT pg_table_size('content_read')")+" trigramBytes="+scalar(c,"SELECT sum(pg_relation_size(indexrelid)) FROM pg_index WHERE indrelid='content_read'::regclass AND indexrelid::regclass::text LIKE 'trgm_%'"));
                    }
                    if(phase.equals("trigram_ordered")){
                        exec(c,"CREATE INDEX search_order ON content_read(channel,published_at DESC NULLS LAST,id DESC)");
                        exec(c,"ANALYZE content_read");
                        System.out.println("ORDER_SIZE rows="+n+" bytes="+scalar(c,"SELECT pg_relation_size('search_order')"));
                    }
                    for(String term:List.of("희","희귀","희귀표식","검색","검색 안내","needlemarker","NOTFOUND")){
                        String all="SELECT c.id"+WHERE+" ORDER BY c.published_at DESC NULLS LAST,c.id DESC";
                        List<Long> ids=query(c,all,term);
                        if(phase.equals("btree"))baseline.put(term,ids);
                        else check("same-results rows="+n+" term="+term,ids.equals(baseline.get(term)));
                        check("count-list-contract rows="+n+" phase="+phase+" term="+term,query(c,"SELECT count(*)"+WHERE,term).getFirst()==ids.size());
                        System.out.println("RESULT rows="+n+" phase="+phase+" term="+term+" count="+ids.size());
                        for(String kind:List.of("list","count")){
                            String sql=kind.equals("list")?all+" LIMIT 20 OFFSET 0":"SELECT count(*)"+WHERE;
                            explain(c,sql,term); // one unrecorded warm-up per query
                            for(int run=1;run<=3;run++){
                                String plan=explain(c,sql,term);
                                log.write("{\"size\":"+n+",\"phase\":\""+phase+"\",\"term\":\""+term+"\",\"kind\":\""+kind+"\",\"run\":"+run+",\"plan\":"+plan.replace("\n","")+"}\n");
                            }
                        }
                    }
                }
            }
            System.out.println("ALL "+checks+" CHECKS PASSED; full plans: "+out.toAbsolutePath());
        }
    }
}
