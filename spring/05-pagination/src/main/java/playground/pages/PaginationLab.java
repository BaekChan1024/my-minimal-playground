package playground.pages;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.springframework.core.SpringVersion;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;

public class PaginationLab {
    record Row(long id, LocalDate date) {}
    record Boundary(LocalDate date,long id) {
        String encode() { return Base64.getUrlEncoder().withoutPadding().encodeToString(
            ("v1|"+(date==null?"~":date)+"|"+id).getBytes(StandardCharsets.UTF_8)); }
        static Boundary decode(String value) {
            if(value.length()>120)throw new IllegalArgumentException("Cursor too long");
            String[] fields=new String(Base64.getUrlDecoder().decode(value),StandardCharsets.UTF_8).split("\\|",-1);
            if(fields.length!=3||!fields[0].equals("v1"))throw new IllegalArgumentException("Invalid cursor");
            long id=Long.parseLong(fields[2]);if(id<1)throw new IllegalArgumentException("Invalid id");
            return new Boundary(fields[1].equals("~")?null:LocalDate.parse(fields[1]),id);
        }
    }
    static final String ORDER=" ORDER BY published_at DESC NULLS LAST,id DESC";
    final JdbcTemplate jdbc;
    final TransactionTemplate read;
    PaginationLab(DataSource ds) {
        jdbc=new JdbcTemplate(ds);read=new TransactionTemplate(new DataSourceTransactionManager(ds));
        read.setReadOnly(true);read.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);read.setTimeout(10);
    }
    static void check(boolean ok,String reason) { if(!ok)throw new IllegalStateException(reason); }
    static List<Long> ids(List<Row> rows) { return rows.stream().map(Row::id).toList(); }
    static void expect(List<Row> rows,Long... expected) { check(ids(rows).equals(List.of(expected)),"Expected "+List.of(expected)+", got "+ids(rows)); }
    void reset() {
        jdbc.execute("TRUNCATE page_post");
        for(long id=1;id<=6;id++)jdbc.update("INSERT INTO page_post VALUES (?,?)",id,LocalDate.of(2026,1,(int)id));
    }
    List<Row> query(String where,int size,Object... args) {
        var params=new ArrayList<Object>(Arrays.asList(args));params.add(size);
        return read.execute(status->jdbc.query("SELECT id,published_at FROM page_post"+where+ORDER+" LIMIT ?",
            (rs,n)->new Row(rs.getLong(1),rs.getObject(2,LocalDate.class)),params.toArray()));
    }
    List<Row> offset(int skip,int size) {
        return read.execute(status->jdbc.query("SELECT id,published_at FROM page_post"+ORDER+" LIMIT ? OFFSET ?",
            (rs,n)->new Row(rs.getLong(1),rs.getObject(2,LocalDate.class)),size,skip));
    }
    String cursor(List<Row> rows) {
        Row last=rows.getLast();var b=new Boundary(last.date(),last.id());String encoded=b.encode();
        check(b.equals(Boundary.decode(encoded)),"Cursor round trip");return encoded;
    }
    List<Row> next(String token,int size) {
        var b=Boundary.decode(token);
        return b.date()==null
            ?query(" WHERE published_at IS NULL AND id < ?",size,b.id())
            :query(" WHERE (published_at,id) < (?,?) OR published_at IS NULL",size,b.date(),b.id());
    }
    void run() {
        jdbc.execute("CREATE TABLE page_post(id bigint PRIMARY KEY,published_at date)");
        reset();var first=offset(0,3);expect(first,6L,5L,4L);
        String encodedOffset=Base64.getUrlEncoder().withoutPadding().encodeToString("offset:3".getBytes(StandardCharsets.UTF_8));
        check(new String(Base64.getUrlDecoder().decode(encodedOffset),StandardCharsets.UTF_8).equals("offset:3"),"Offset encoding");
        jdbc.update("INSERT INTO page_post VALUES (7,DATE '2026-01-07')");var second=offset(3,3);expect(second,4L,3L,2L);
        System.out.println("PASS offset-insert first="+ids(first)+" next="+ids(second)+" repeated=4 cursor="+encodedOffset);

        reset();first=offset(0,3);expect(first,6L,5L,4L);jdbc.update("DELETE FROM page_post WHERE id=6");second=offset(3,3);expect(second,2L,1L);
        check(jdbc.queryForObject("SELECT count(*) FROM page_post WHERE id=3",Long.class)==1,"Skipped row still exists");
        System.out.println("PASS offset-delete first="+ids(first)+" next="+ids(second)+" skippedExisting=3");

        reset();first=offset(0,3);String token=cursor(first);jdbc.update("INSERT INTO page_post VALUES (7,DATE '2026-01-07')");second=next(token,3);expect(second,3L,2L,1L);expect(offset(0,3),7L,6L,5L);
        System.out.println("PASS keyset-insert first="+ids(first)+" next="+ids(second)+" freshFirst=[7, 6, 5]");

        reset();first=offset(0,3);token=cursor(first);jdbc.update("DELETE FROM page_post WHERE id=4");second=next(token,3);expect(second,3L,2L,1L);
        check(jdbc.queryForObject("SELECT count(*) FROM page_post WHERE id=4",Long.class)==0,"Anchor really deleted");
        System.out.println("PASS keyset-deleted-anchor next="+ids(second)+" anchorExists=false");

        reset();jdbc.update("UPDATE page_post SET published_at=DATE '2026-01-06' WHERE id IN (4,5)");first=offset(0,2);expect(first,6L,5L);token=cursor(first);
        var dateOnly=query(" WHERE published_at < ?",2,first.getLast().date());expect(dateOnly,3L,2L);second=next(token,2);expect(second,4L,3L);
        System.out.println("PASS tied-date first="+ids(first)+" dateOnly="+ids(dateOnly)+" composite="+ids(second));

        reset();jdbc.update("UPDATE page_post SET published_at=NULL WHERE id IN (1,2)");
        first=offset(0,2);expect(first,6L,5L);second=next(cursor(first),2);expect(second,4L,3L);var third=next(cursor(second),2);expect(third,2L,1L);check(next(cursor(third),2).isEmpty(),"End after NULL group");
        var naive=query(" WHERE (published_at,id) < (?,?)",3,null,2L);check(naive.isEmpty(),"NULL row comparison not true");
        var nullTail=next(new Boundary(null,2).encode(),3);expect(nullTail,1L);
        System.out.println("PASS null-boundary pages="+List.of(ids(first),ids(second),ids(third))+" naive="+ids(naive)+" nullTail="+ids(nullTail));

        reset();first=offset(0,3);token=cursor(first);jdbc.update("UPDATE page_post SET published_at=DATE '2026-01-09' WHERE id=3");second=next(token,3);expect(second,2L,1L);expect(offset(0,3),3L,6L,5L);
        System.out.println("PASS mutable-sort-key next="+ids(second)+" movedBeforeBoundary=3 freshFirst=[3, 6, 5]");
        System.out.println("ALL 7 SCENARIOS PASSED. Compare EXERCISES.md and ANSWERS.md.");
    }
    public static void main(String[] args) throws Exception {
        String mode=args.length==0?"guided":args[0];
        if(!Set.of("guided","verify").contains(mode))throw new IllegalArgumentException("Use guided or verify");
        if(mode.equals("guided")) {
            System.out.println("예상: 첫 페이지 [6,5,4] 뒤 삽입/삭제가 있으면 다음 페이지는? 같은 날짜와 NULL은 어떻게 넘길까요?");
            System.out.println("Enter: 임시 PostgreSQL의 일곱 실험 / q: 종료");
            if(new Scanner(System.in).nextLine().strip().equalsIgnoreCase("q"))return;
        }
        try(var pg=EmbeddedPostgres.builder().setServerConfig("listen_addresses","127.0.0.1").setServerConfig("shared_buffers","32MB").setPort(0).start()) {
            var ds=pg.getPostgresDatabase();try(var c=ds.getConnection()) {System.out.printf("Spring=%s PostgreSQL=%s JDBC=%s%n",SpringVersion.getVersion(),c.getMetaData().getDatabaseProductVersion(),c.getMetaData().getDriverVersion());}
            new PaginationLab(ds).run();
        }
    }
}
