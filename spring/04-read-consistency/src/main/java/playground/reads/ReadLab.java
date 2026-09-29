package playground.reads;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import jakarta.persistence.*;
import org.hibernate.Version;
import org.hibernate.resource.jdbc.spi.StatementInspector;
import org.springframework.core.SpringVersion;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.*;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class ReadLab {
    static void check(boolean condition,String message) { if(!condition) throw new IllegalStateException(message); }
    public static class SelectCounter implements StatementInspector {
        final AtomicInteger selects=new AtomicInteger();
        @Override public String inspect(String sql) { if(sql.startsWith("select ")) selects.incrementAndGet(); return sql; }
    }
    final DataSource ds;
    final JdbcTemplate jdbc;
    final JpaTransactionManager manager;
    final EntityManager em;
    final SelectCounter counter;
    int readerPid;
    ReadLab(DataSource ds,EntityManagerFactory factory,SelectCounter counter) {
        this.ds=ds;this.jdbc=new JdbcTemplate(ds);this.manager=new JpaTransactionManager(factory);
        this.em=SharedEntityManagerCreator.createSharedEntityManager(factory);this.counter=counter;
    }
    void init() { jdbc.execute("CREATE TABLE post_record (id bigint PRIMARY KEY,title text NOT NULL,version bigint NOT NULL)"); }
    void reset() { jdbc.execute("TRUNCATE post_record");jdbc.update("INSERT INTO post_record VALUES (1,'original',0)");counter.selects.set(0); }
    String value() { return jdbc.queryForObject("SELECT title FROM post_record WHERE id=1",String.class); }
    void readTx(boolean repeatable,boolean readOnly,Consumer<EntityManager> work) {
        var tx=new TransactionTemplate(manager);
        tx.setIsolationLevel(repeatable?TransactionDefinition.ISOLATION_REPEATABLE_READ:TransactionDefinition.ISOLATION_READ_COMMITTED);
        tx.setReadOnly(readOnly);tx.setTimeout(15);
        tx.executeWithoutResult(status->{
            jdbc.execute("SET LOCAL statement_timeout='8s'");
            String isolation=jdbc.queryForObject("SHOW transaction_isolation",String.class);
            String ro=jdbc.queryForObject("SHOW transaction_read_only",String.class);
            check(isolation.equals(repeatable?"repeatable read":"read committed"),"Unexpected isolation: "+isolation);
            check(ro.equals(readOnly?"on":"off"),"Unexpected readOnly: "+ro);
            readerPid=jdbc.queryForObject("SELECT pg_backend_pid()",Integer.class);
            work.accept(em);
        });
    }
    void independentWrite(boolean insert) {
        // Direct connection, deliberately not a Spring transaction-aware wrapper.
        // This writer commits while the reader transaction remains open on its own connection.
        try(var c=ds.getConnection()) {
            c.setAutoCommit(false);
            try(var statement=c.createStatement()) {
                statement.setQueryTimeout(5);
                try(var result=statement.executeQuery("SELECT pg_backend_pid()")) {
                    check(result.next()&&result.getInt(1)!=readerPid,"Writer must use a separate connection");
                }
                int changed=statement.executeUpdate(insert?"INSERT INTO post_record VALUES (2,'second',0)":"UPDATE post_record SET title='updated',version=version+1 WHERE id=1");
                check(changed==1,"Writer changed rows");c.commit();
            } catch(SQLException|RuntimeException e) { c.rollback();throw e; }
        } catch(SQLException e) { throw new IllegalStateException(e); }
    }
    void jdbcReads(boolean repeatable) {
        reset();String expected=repeatable?"original":"updated";
        readTx(repeatable,false,e->{
            String first=value();check(first.equals("original"),"Initial value");
            independentWrite(false);String second=value();check(second.equals(expected),"Second JDBC read");
            System.out.printf("OBS jdbc-%s first=%s second=%s separateWriter=true%n",repeatable?"rr":"rc",first,second);
        });
        check(value().equals("updated"),"Writer commit must be visible after reader ends");
        System.out.printf("PASS jdbc-%s afterReader=updated%n",repeatable?"rr":"rc");
    }
    void jpaReads(boolean repeatable) {
        reset();String expected=repeatable?"original":"updated";
        readTx(repeatable,false,e->{
            Post first=e.find(Post.class,1L);check(first.title().equals("original")&&first.version()==0,"Initial entity");
            int before=counter.selects.get();check(before==1,"Initial find must SELECT");
            independentWrite(false);Post second=e.find(Post.class,1L);
            check(first==second&&second.title().equals("original")&&counter.selects.get()==before,"Second find must reuse managed entity");
            String raw=value();String scalar=e.createQuery("select p.title from Post p where p.id=1",String.class).getSingleResult();
            check(raw.equals(expected)&&scalar.equals(expected),"DB snapshot differs from expected");
            e.refresh(first);
            check(first.title().equals(expected)&&first.version()==(repeatable?0:1),"Refresh result");
            e.clear();Post afterClear=e.find(Post.class,1L);
            check(first!=afterClear&&afterClear.title().equals(expected),"Clear and reload");
            check(counter.selects.get()==4,"Expected find, scalar query, refresh, find after clear");
            System.out.printf("OBS jpa-%s secondFind=original sameObject=true secondFindSql=0 jdbc=%s scalar=%s refresh=%s clearFind=%s hibernateSelects=4%n",repeatable?"rr":"rc",raw,scalar,first.title(),afterClear.title());
        });
        check(value().equals("updated"),"Final committed row");
        System.out.printf("PASS jpa-%s afterReader=updated%n",repeatable?"rr":"rc");
    }
    void countAndList(boolean repeatable) {
        reset();
        readTx(repeatable,true,e->{
            long total=jdbc.queryForObject("SELECT count(*) FROM post_record",Long.class);
            check(total==1,"Initial total");independentWrite(true);
            List<Long> ids=jdbc.queryForList("SELECT id FROM post_record ORDER BY id",Long.class);
            long later=jdbc.queryForObject("SELECT count(*) FROM post_record",Long.class);
            check(ids.equals(repeatable?List.of(1L):List.of(1L,2L))&&later==(repeatable?1:2),"Count/list snapshot");
            System.out.printf("OBS readonly-count-list-%s readOnly=on totalBefore=%d ids=%s totalAfter=%d%n",repeatable?"rr":"rc",total,ids,later);
        });
        check(jdbc.queryForObject("SELECT count(*) FROM post_record",Long.class)==2,"Writer insert must persist");
        System.out.printf("PASS readonly-count-list-%s afterReaderCount=2%n",repeatable?"rr":"rc");
    }
    public static void main(String[] args) throws Exception {
        String mode=args.length==0?"guided":args[0];
        if(!Set.of("guided","verify").contains(mode)) throw new IllegalArgumentException("Use guided or verify");
        if(mode.equals("guided")) {
            System.out.println("예상: 같은 트랜잭션의 두 SELECT는 같을까요? readOnly면 스냅샷이 고정될까요? find가 같으면 DB도 같을까요?");
            System.out.println("Enter: 임시 PostgreSQL에서 여섯 실험 실행 / q: 종료");
            if(new Scanner(System.in).nextLine().strip().equalsIgnoreCase("q"))return;
        }
        try(var postgres=EmbeddedPostgres.builder().setServerConfig("listen_addresses","127.0.0.1")
                .setServerConfig("shared_buffers","32MB").setPort(0).start()) {
            var ds=postgres.getPostgresDatabase();var counter=new SelectCounter();
            var factory=new LocalContainerEntityManagerFactoryBean();factory.setDataSource(ds);
            factory.setPackagesToScan("playground.reads");factory.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
            factory.setJpaPropertyMap(Map.of("hibernate.hbm2ddl.auto","none","hibernate.session_factory.statement_inspector",counter));
            factory.afterPropertiesSet();
            try {
                var lab=new ReadLab(ds,Objects.requireNonNull(factory.getObject()),counter);lab.init();
                try(var c=ds.getConnection()) { System.out.printf("Spring=%s Hibernate=%s PostgreSQL=%s JDBC=%s%n",SpringVersion.getVersion(),Version.getVersionString(),c.getMetaData().getDatabaseProductVersion(),c.getMetaData().getDriverVersion()); }
                lab.jdbcReads(false);lab.jdbcReads(true);lab.jpaReads(false);lab.jpaReads(true);lab.countAndList(false);lab.countAndList(true);
                System.out.println("ALL 6 SCENARIOS PASSED. 비교 해설: spring/04-read-consistency/ANSWERS.md");
            } finally {factory.destroy();}
        }
    }
}
