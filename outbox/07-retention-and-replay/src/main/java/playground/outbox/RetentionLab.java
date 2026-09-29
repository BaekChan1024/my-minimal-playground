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

public class RetentionLab {
    static DataSource ds;
    static String bootstrap;
    static final String TOPIC="retention-lab";
    static final TopicPartition TP=new TopicPartition(TOPIC,0);
    interface Work<T>{T run(Connection c)throws Exception;}
    static void check(boolean ok,String reason){if(!ok)throw new IllegalStateException(reason);}
    static <T>T tx(Work<T>w)throws Exception{try(var c=ds.getConnection()){c.setAutoCommit(false);try{T v=w.run(c);c.commit();return v;}catch(Exception e){c.rollback();throw e;}}}
    static int sql(Connection c,String text,Object...args)throws Exception{try(var p=c.prepareStatement(text)){for(int i=0;i<args.length;i++)p.setObject(i+1,args[i]);return p.executeUpdate();}}
    static long scalar(String text)throws Exception{try(var c=ds.getConnection();var s=c.createStatement();var r=s.executeQuery(text)){check(r.next(),"Missing value");return r.getLong(1);}}
    static KafkaConsumer<String,String> consumer(){return new KafkaConsumer<>(Map.of("bootstrap.servers",bootstrap,"group.id","audit-"+UUID.randomUUID(),"enable.auto.commit","false","auto.offset.reset","none"),new StringDeserializer(),new StringDeserializer());}
    static ConsumerRecord<String,String> replay(){
        try(var c=consumer()){
            c.assign(List.of(TP));c.seek(TP,0);long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
            while(System.nanoTime()<until){var batch=c.poll(Duration.ofMillis(100));if(!batch.isEmpty()){check(batch.count()==1,"Unexpected records");var r=batch.iterator().next();check(r.offset()==0&&r.key().equals("counter-1")&&r.value().equals("E1|5"),"Wrong replay");return r;}}
            throw new IllegalStateException("Replay timed out");
        }
    }
    static String apply(ConsumerRecord<String,String> r,String namespace,String target)throws Exception{
        check(Set.of("counter","rebuild_counter").contains(target),"Unexpected target");
        String[] event=r.value().split("\\|");
        return tx(c->{
            int inserted=sql(c,"INSERT INTO inbox VALUES (?,?,'2026-01-01T00:00:00Z') ON CONFLICT DO NOTHING",namespace,event[0]);
            if(inserted==0)return "DUPLICATE";
            sql(c,"UPDATE "+target+" SET total=total+? WHERE id=1",Integer.parseInt(event[1]));return "APPLIED";
        });
    }
    static void lesson(Admin admin)throws Exception{
        int removed=tx(c->sql(c,"DELETE FROM outbox WHERE sent_at IS NOT NULL AND sent_at < TIMESTAMPTZ '2026-01-10T00:00:00Z'"));
        check(removed==1&&scalar("SELECT count(*) FROM outbox")==2&&scalar("SELECT count(*) FROM outbox WHERE id=3 AND sent_at IS NULL")==1&&scalar("SELECT count(*) FROM outbox WHERE id=2")==1,"Unsafe cleanup");
        System.out.println("PASS outbox-cleanup removedOldSent=1 retainedRecentSent=1 retainedOldPending=1");

        admin.createTopics(List.of(new NewTopic(TOPIC,1,(short)1))).all().get(15,TimeUnit.SECONDS);
        try(var p=new KafkaProducer<String,String>(Map.of("bootstrap.servers",bootstrap,"acks","all"),new StringSerializer(),new StringSerializer())){p.send(new ProducerRecord<>(TOPIC,0,"counter-1","E1|5")).get(15,TimeUnit.SECONDS);}
        check(apply(replay(),"view","counter").equals("APPLIED"),"First application");
        check(apply(replay(),"view","counter").equals("DUPLICATE")&&scalar("SELECT total FROM counter WHERE id=1")==5,"Retained Inbox failed");
        System.out.println("PASS retained-inbox kafkaOffset=0 replayResult=DUPLICATE total=5 inbox=1");

        int inboxDeleted=tx(c->sql(c,"DELETE FROM inbox WHERE handled_at < TIMESTAMPTZ '2026-01-10T00:00:00Z'"));
        check(inboxDeleted==1&&apply(replay(),"view","counter").equals("APPLIED")&&scalar("SELECT total FROM counter WHERE id=1")==10,"Expiry counterexample");
        System.out.println("PASS expired-inbox sameKafkaOffset=0 replayResult=APPLIED total=10 duplicateEffectObserved=true");

        check(apply(replay(),"view","rebuild_counter").equals("DUPLICATE")&&scalar("SELECT total FROM rebuild_counter WHERE id=1")==0,"Expected skipped rebuild");
        System.out.println("PASS reused-namespace rebuildTotal=0 result=DUPLICATE missingRebuildObserved=true");

        check(apply(replay(),"rebuild-v2","rebuild_counter").equals("APPLIED")&&scalar("SELECT total FROM rebuild_counter WHERE id=1")==5&&scalar("SELECT total FROM counter WHERE id=1")==10&&scalar("SELECT count(*) FROM inbox")==2,"Isolated rebuild failed");
        System.out.println("PASS isolated-rebuild namespace=rebuild-v2 rebuildTotal=5 originalTotal=10 inbox=2");

        // Explicit administrative deletion on a newly created, local topic only.
        long low=admin.deleteRecords(Map.of(TP,RecordsToDelete.beforeOffset(1))).lowWatermarks().get(TP).get(15,TimeUnit.SECONDS).lowWatermark();
        check(low==1,"Low watermark");boolean rejected=false;
        try(var c=consumer()){
            check(c.beginningOffsets(List.of(TP)).get(TP)==1&&c.endOffsets(List.of(TP)).get(TP)==1,"Log boundaries");
            c.assign(List.of(TP));c.seek(TP,0);long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
            try{while(System.nanoTime()<until)c.poll(Duration.ofMillis(100));}catch(OffsetOutOfRangeException e){rejected=true;}
        }
        check(rejected&&scalar("SELECT count(*) FROM inbox")==2,"Deleted replay not rejected");
        System.out.println("PASS removed-kafka-log logStart=1 logEnd=1 seek0=OffsetOutOfRange inboxStill=2");
    }
    public static void main(String[]args)throws Exception{
        check(args.length<=1&&(args.length==0||Set.of("guided","verify").contains(args[0])),"guided|verify");
        if(args.length==0||args[0].equals("guided")){
            System.out.println("Outbox 07: 처리한 Inbox를 지운 뒤 같은 +5를 읽으면? 과거 Kafka 로그가 없으면? 예상 후 Enter/q.");
            var in=new Scanner(System.in);if(!in.hasNextLine()||in.nextLine().equalsIgnoreCase("q"))return;
        }
        try(var pg=EmbeddedPostgres.builder().setPort(0).start()){
            ds=pg.getPostgresDatabase();tx(c->{
                sql(c,"CREATE TABLE outbox(id integer PRIMARY KEY,created_at timestamptz NOT NULL,sent_at timestamptz)");
                sql(c,"INSERT INTO outbox VALUES (1,'2026-01-01Z','2026-01-02Z'),(2,'2026-01-01Z','2026-01-11Z'),(3,'2026-01-01Z',NULL)");
                sql(c,"CREATE TABLE inbox(consumer_name text,event_id text,handled_at timestamptz NOT NULL,PRIMARY KEY(consumer_name,event_id))");
                sql(c,"CREATE TABLE counter(id integer PRIMARY KEY,total integer NOT NULL)");sql(c,"INSERT INTO counter VALUES(1,0)");
                sql(c,"CREATE TABLE rebuild_counter(id integer PRIMARY KEY,total integer NOT NULL)");sql(c,"INSERT INTO rebuild_counter VALUES(1,0)");return null;});
            var nodes=new TestKitNodes.Builder().setCombined(false).setNumBrokerNodes(1).setNumControllerNodes(1)
                .setBootstrapMetadataVersion(MetadataVersion.fromVersionString("4.1-IV1")).build();
            try(var cluster=new KafkaClusterTestKit.Builder(nodes).setConfigProp("offsets.topic.replication.factor","1").setConfigProp("offsets.topic.num.partitions","1").build()){
                cluster.format();cluster.startup();cluster.waitForReadyBrokers();bootstrap=cluster.bootstrapServers();
                try(var c=ds.getConnection();var s=c.createStatement();var v=s.executeQuery("SHOW server_version")){v.next();System.out.println("PostgreSQL="+v.getString(1)+" Kafka="+org.apache.kafka.common.utils.AppInfoParser.getVersion()+" cutoff=fixed-fixture logRemoval=Admin.deleteRecords");}
                try(var admin=Admin.create(Map.of("bootstrap.servers",bootstrap))){lesson(admin);}
            }
        }
        System.out.println("ALL 6 SCENARIOS PASSED. Fixed dates and explicit log deletion are not timed-retention measurements.");
    }
}
