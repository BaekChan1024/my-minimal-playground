package playground.outbox;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.test.*;
import org.apache.kafka.server.common.MetadataVersion;
import javax.sql.DataSource;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** A small JDBC lab; not a copy of a production service or a Spring listener. */
public class OutboxLab {
    static DataSource ds;
    static String bootstrap;
    static void check(boolean ok, String reason) { if (!ok) throw new IllegalStateException(reason); }
    static class Injected extends RuntimeException { }
    interface Work<T> { T run(Connection c) throws Exception; }
    static <T> T tx(Work<T> work) throws Exception {
        try (var c = ds.getConnection()) {
            c.setAutoCommit(false);
            try { T result = work.run(c); c.commit(); return result; }
            catch (Exception e) { c.rollback(); throw e; }
        }
    }
    static int sql(Connection c, String sql, Object... args) throws SQLException {
        try (var p = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) p.setObject(i + 1, args[i]);
            return p.executeUpdate();
        }
    }
    static long count(String table) throws SQLException {
        try (var c = ds.getConnection(); var s = c.createStatement(); var r = s.executeQuery("SELECT count(*) FROM " + table)) {
            r.next(); return r.getLong(1);
        }
    }
    static void reset() throws Exception {
        tx(c -> { sql(c, "TRUNCATE posts,outbox,inbox,projection"); return null; });
    }
    static void save(boolean fail) throws Exception {
        tx(c -> {
            sql(c, "INSERT INTO posts VALUES (1,'ramen')");
            // A real NOT NULL violation, after the original row has been inserted.
            sql(c, "INSERT INTO outbox VALUES ('E101',1,?,false)", fail ? null : "ramen");
            return null;
        });
    }
    static KafkaProducer<String,String> producer() {
        return new KafkaProducer<>(Map.of("bootstrap.servers",bootstrap,"acks","all","enable.idempotence","true"),
                new StringSerializer(),new StringSerializer());
    }
    static void relay(String topic, boolean failAfterAck) throws Exception {
        try (var producer = producer()) {
            tx(c -> {
                try (var s = c.createStatement(); var r = s.executeQuery("SELECT event_id,post_id,title FROM outbox WHERE NOT sent ORDER BY event_id FOR UPDATE")) {
                    while (r.next()) {
                        String event = r.getString(1), key = r.getString(2), title = r.getString(3);
                        producer.send(new ProducerRecord<>(topic,0,key,event + "|" + key + "|" + title)).get(15,TimeUnit.SECONDS);
                        if (failAfterAck) throw new Injected();
                        sql(c,"UPDATE outbox SET sent=true WHERE event_id=?",event);
                    }
                }
                return null;
            });
        }
    }
    static KafkaConsumer<String,String> consumer(String group) {
        return new KafkaConsumer<>(Map.of("bootstrap.servers",bootstrap,"group.id",group,
                "enable.auto.commit","false","auto.offset.reset","earliest","max.poll.records","1"),
                new StringDeserializer(),new StringDeserializer());
    }
    static int apply(ConsumerRecord<String,String> record, boolean fail) throws Exception {
        String[] event = record.value().split("\\|",3);
        check(record.key().equals(event[1]),"Wrong Kafka key");
        return tx(c -> {
            int inserted = sql(c,"INSERT INTO inbox VALUES ('public-view',?) ON CONFLICT DO NOTHING",event[0]);
            if (inserted == 0) return 0;
            sql(c,"INSERT INTO projection VALUES (?,?)",Long.parseLong(event[1]),fail ? null : event[2]);
            return 1;
        });
    }
    static ConsumerRecord<String,String> next(KafkaConsumer<String,String> c) {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
        while(System.nanoTime()<deadline) {
            var batch=c.poll(Duration.ofMillis(100));
            if(!batch.isEmpty()) { check(batch.count()==1,"One record per poll"); return batch.iterator().next(); }
        }
        throw new IllegalStateException("No record before deadline");
    }
    static long committed(KafkaConsumer<String,String> c, TopicPartition tp) {
        var v=c.committed(Set.of(tp)).get(tp); return v==null ? -1 : v.offset();
    }
    static void commit(KafkaConsumer<String,String> c, ConsumerRecord<String,String> r) {
        c.commitSync(Map.of(new TopicPartition(r.topic(),r.partition()),new OffsetAndMetadata(r.offset()+1)));
    }
    static long end(String topic) {
        try(var c=consumer("observer-"+topic)) { var tp=new TopicPartition(topic,0);return c.endOffsets(List.of(tp)).get(tp); }
    }
    static void state(long kafka, long inbox, long projection, String topic) throws Exception {
        check(count("posts")==1 && count("outbox")==1,"Missing source records");
        check(end(topic)==kafka && count("inbox")==inbox && count("projection")==projection,"Wrong state");
        if(projection==1) try(var c=ds.getConnection();var s=c.createStatement();var r=s.executeQuery("SELECT title FROM projection WHERE id=1")) {
            check(r.next()&&r.getString(1).equals("ramen"),"Wrong projection content");
        }
    }
    static boolean sent() throws Exception {
        try(var c=ds.getConnection();var s=c.createStatement();var r=s.executeQuery("SELECT sent FROM outbox")) {check(r.next(),"Missing outbox");return r.getBoolean(1);}
    }
    static void create(Admin admin,String topic) throws Exception {admin.createTopics(List.of(new NewTopic(topic,1,(short)1))).all().get(15,TimeUnit.SECONDS);}
    static void lesson(Admin admin) throws Exception {
        reset(); boolean failed=false;
        try{save(true);}catch(SQLException e){check("23502".equals(e.getSQLState()),"Wrong save failure");failed=true;}
        check(failed&&count("posts")==0&&count("outbox")==0,"Atomic save failed");
        System.out.println("PASS save-rollback posts=0 outbox=0 sqlState=23502");

        reset();String topic="normal";create(admin,topic);save(false);state(0,0,0,topic);check(!sent(),"Premature sent");
        relay(topic,false);check(sent(),"Unmarked send");
        try(var c=consumer(topic)){var tp=new TopicPartition(topic,0);c.assign(List.of(tp));var r=next(c);check(apply(r,false)==1,"Not applied");commit(c,r);check(committed(c,tp)==1,"Offset not committed");}
        state(1,1,1,topic);System.out.println("PASS normal posts=1 outbox=1 sent=true kafka=1 inbox=1 projection=1 committed=1");

        reset();topic="ack-gap";create(admin,topic);save(false);failed=false;
        try{relay(topic,true);}catch(Injected e){failed=true;}
        check(failed&&!sent(),"Completion did not roll back");state(1,0,0,topic);
        relay(topic,false);check(sent(),"Retry not marked");
        try(var c=consumer(topic)){var tp=new TopicPartition(topic,0);c.assign(List.of(tp));int applied=0;
            for(int i=0;i<2;i++){var r=next(c);applied+=apply(r,false);commit(c,r);}
            check(applied==1&&committed(c,tp)==2,"Deduplication failed");}
        state(2,1,1,topic);System.out.println("PASS ack-gap kafka=2 sameEvent=E101 applied=1 inbox=1 projection=1 committed=2");

        reset();topic="consumer-db-failure";create(admin,topic);save(false);relay(topic,false);failed=false;
        var tp=new TopicPartition(topic,0);
        try(var c=consumer(topic)){c.assign(List.of(tp));c.commitSync(Map.of(tp,new OffsetAndMetadata(0)));var r=next(c);
            try{apply(r,true);}catch(SQLException e){check("23502".equals(e.getSQLState()),"Wrong projection failure");failed=true;}
            check(failed&&committed(c,tp)==0,"Failure advanced offset");state(1,0,0,topic);}
        try(var c=consumer(topic)){c.assign(List.of(tp));var r=next(c);check(r.offset()==0&&apply(r,false)==1,"Replay failed");commit(c,r);check(committed(c,tp)==1,"Recovery offset");}
        state(1,1,1,topic);System.out.println("PASS consumer-db-failure beforeRetry=inbox0,projection0,committed0 afterRetry=inbox1,projection1,committed1");

        reset();topic="consumer-commit-gap";create(admin,topic);save(false);relay(topic,false);
        try(var c=consumer(topic)){tp=new TopicPartition(topic,0);c.assign(List.of(tp));c.commitSync(Map.of(tp,new OffsetAndMetadata(0)));check(apply(next(c),false)==1,"Apply failed");check(committed(c,tp)==0,"Premature offset");}
        try(var c=consumer(topic)){c.assign(List.of(tp));var r=next(c);check(r.offset()==0&&apply(r,false)==0,"Duplicate after reopen");commit(c,r);check(committed(c,tp)==1,"Final commit");}
        state(1,1,1,topic);System.out.println("PASS consumer-commit-gap rereadOffset=0 duplicate=true kafka=1 inbox=1 projection=1 committed=1");
    }
    public static void main(String[] args) throws Exception {
        check(args.length<=1&&(args.length==0||Set.of("guided","verify").contains(args[0])),"guided|verify");
        if(args.length==0||args[0].equals("guided")){
            System.out.println("Outbox 04: 먼저 예상하세요. ACK 후 실패하면 Kafka와 Inbox는 각각 몇 건일까요?\nEXERCISES.md의 다섯 질문을 적고 Enter. q는 실행 전 종료입니다.");
            var scanner=new Scanner(System.in);if(!scanner.hasNextLine()||scanner.nextLine().equalsIgnoreCase("q"))return;
        }
        try(var pg=EmbeddedPostgres.builder().setPort(0).start()) {
            ds=pg.getPostgresDatabase();
            tx(c->{sql(c,"CREATE TABLE posts(id bigint PRIMARY KEY,title text NOT NULL)");
                sql(c,"CREATE TABLE outbox(event_id text PRIMARY KEY,post_id bigint NOT NULL REFERENCES posts(id),title text NOT NULL,sent boolean NOT NULL)");
                sql(c,"CREATE TABLE inbox(consumer_name text,event_id text,PRIMARY KEY(consumer_name,event_id))");
                sql(c,"CREATE TABLE projection(id bigint PRIMARY KEY,title text NOT NULL)");return null;});
            var nodes=new TestKitNodes.Builder().setCombined(false).setNumBrokerNodes(1).setNumControllerNodes(1)
                .setBootstrapMetadataVersion(MetadataVersion.fromVersionString("4.1-IV1")).build();
            try(var cluster=new KafkaClusterTestKit.Builder(nodes).setConfigProp("offsets.topic.replication.factor","1")
                .setConfigProp("offsets.topic.num.partitions","1").build()) {
                cluster.format();cluster.startup();cluster.waitForReadyBrokers();bootstrap=cluster.bootstrapServers();
                try(var c=ds.getConnection();var s=c.createStatement();var r=s.executeQuery("SHOW server_version")){r.next();System.out.println("PostgreSQL="+r.getString(1)+" Kafka="+org.apache.kafka.common.utils.AppInfoParser.getVersion());}
                try(var admin=Admin.create(Map.of("bootstrap.servers",bootstrap))){lesson(admin);}
            }
        }
        System.out.println("ALL 5 SCENARIOS PASSED. Compare with ANSWERS.md; this does not assess your understanding.");
    }
}
