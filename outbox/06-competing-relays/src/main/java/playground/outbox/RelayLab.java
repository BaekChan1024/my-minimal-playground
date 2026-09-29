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

public class RelayLab {
    static DataSource ds;
    static String bootstrap;
    static void check(boolean ok,String reason){if(!ok)throw new IllegalStateException(reason);}
    static Connection connection()throws Exception{
        var c=ds.getConnection();c.setAutoCommit(false);
        try(var s=c.createStatement()){s.execute("SET statement_timeout='5s'");}
        return c;
    }
    static void sql(Connection c,String text)throws Exception{try(var s=c.createStatement()){s.executeUpdate(text);}}
    static int first(Connection c,String sql)throws Exception{try(var s=c.createStatement();var r=s.executeQuery(sql)){return r.next()?r.getInt(1):0;}}
    static int take(Connection c,boolean lock)throws Exception{
        return first(c,"SELECT id FROM outbox WHERE NOT sent ORDER BY id LIMIT 1"+(lock?" FOR UPDATE SKIP LOCKED":""));
    }
    static int guard(Connection c)throws Exception{return first(c,"SELECT id FROM relay_guard WHERE id=1 FOR UPDATE SKIP LOCKED");}
    static void done(Connection c,int id)throws Exception{sql(c,"UPDATE outbox SET sent=true WHERE id="+id);}
    static void reset(int n)throws Exception{try(var c=connection()){sql(c,"TRUNCATE outbox");for(int i=1;i<=n;i++)sql(c,"INSERT INTO outbox VALUES ("+i+",1,false)");c.commit();}}
    static void send(KafkaProducer<String,String> p,String topic,int id)throws Exception{
        check(id>0,"Cannot send missing work");p.send(new ProducerRecord<>(topic,0,"post-1","E"+id)).get(15,TimeUnit.SECONDS);
    }
    static void topic(Admin a,String name)throws Exception{a.createTopics(List.of(new NewTopic(name,1,(short)1))).all().get(15,TimeUnit.SECONDS);}
    static List<String> read(String topic,int expected){
        try(var c=new KafkaConsumer<String,String>(Map.of("bootstrap.servers",bootstrap,"group.id","audit-"+topic,"enable.auto.commit","false","auto.offset.reset","earliest"),new StringDeserializer(),new StringDeserializer())){
            var tp=new TopicPartition(topic,0);c.assign(List.of(tp));c.seek(tp,0);
            check(c.endOffsets(List.of(tp)).get(tp)==expected,"Wrong Kafka count "+topic);
            var result=new ArrayList<String>();long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
            while(result.size()<expected&&System.nanoTime()<until){for(var r:c.poll(Duration.ofMillis(100))){check(r.partition()==0&&r.key().equals("post-1")&&r.offset()==result.size(),"Record mismatch");result.add(r.value());}}
            check(result.size()==expected,"Missing records");return result;
        }
    }
    static void sentCount(int expected)throws Exception{try(var c=connection()){check(first(c,"SELECT count(*) FROM outbox WHERE sent")==expected,"Wrong sent count");c.rollback();}}
    static void scenarios(Admin admin,KafkaProducer<String,String> p)throws Exception{
        reset(1);topic(admin,"no-lock");
        try(var a=connection();var b=connection()){
            int x=take(a,false),y=take(b,false);check(x==1&&y==1,"Expected competing readers");
            send(p,"no-lock",x);send(p,"no-lock",y);done(a,x);a.commit();done(b,y);b.commit();
        }
        check(read("no-lock",2).equals(List.of("E1","E1")),"Duplicate not observed");sentCount(1);
        System.out.println("PASS no-lock selectedA=1 selectedB=1 kafka=E1,E1 sentRows=1");

        reset(2);topic(admin,"global-guard");
        try(var a=connection();var b=connection()){
            check(guard(a)==1&&guard(b)==0,"Guard did not exclude B");b.rollback();
            int x=take(a,false);check(x==1,"Expected first work");send(p,"global-guard",x);done(a,x);a.commit();
            check(guard(b)==1,"Guard not released");int y=take(b,false);check(y==2,"Expected next work");send(p,"global-guard",y);done(b,y);b.commit();
        }
        check(read("global-guard",2).equals(List.of("E1","E2")),"Guard order");sentCount(2);
        System.out.println("PASS global-guard BWhileALocked=empty BAfterACommit=acquired kafka=E1,E2 sentRows=2");

        reset(2);topic(admin,"row-skip");
        try(var a=connection();var b=connection()){
            int x=take(a,true),y=take(b,true);check(x==1&&y==2,"Row work not split");
            // B sends first deliberately while A still holds the earlier row.
            send(p,"row-skip",y);done(b,y);b.commit();send(p,"row-skip",x);done(a,x);a.commit();
        }
        check(read("row-skip",2).equals(List.of("E2","E1")),"Expected send-order inversion");sentCount(2);
        System.out.println("PASS row-skip selectedA=1 selectedB=2 sameKey=post-1 kafka=E2,E1 sentRows=2");

        reset(1);topic(admin,"rollback-release");
        try(var a=connection();var b=connection()){
            check(take(a,true)==1&&take(b,true)==0,"Expected row lock");b.rollback();a.rollback();
            check(take(b,true)==1,"Rolled-back row not retried");send(p,"rollback-release",1);done(b,1);b.commit();
        }
        check(read("rollback-release",1).equals(List.of("E1")),"Rollback retry");sentCount(1);
        System.out.println("PASS rollback-release beforeRollback=empty afterRollback=1 kafka=E1 sentRows=1");

        reset(1);topic(admin,"early-commit");
        try(var a=connection();var b=connection()){
            int x=take(a,true);a.commit(); // No durable claim was written.
            int y=take(b,true);check(x==1&&y==1,"Expected lost ownership");
            send(p,"early-commit",x);send(p,"early-commit",y);done(b,y);b.commit();done(a,x);a.commit();
        }
        check(read("early-commit",2).equals(List.of("E1","E1")),"Early commit duplicate");sentCount(1);
        System.out.println("PASS early-commit selectedA=1 selectedB=1 durableClaim=false kafka=E1,E1 sentRows=1");
    }
    public static void main(String[]args)throws Exception{
        check(args.length<=1&&(args.length==0||Set.of("guided","verify").contains(args[0])),"guided|verify");
        if(args.length==0||args[0].equals("guided")){
            System.out.println("Outbox 06: 두 릴레이가 같은 행을 보면? SKIP LOCKED면 순서도 유지될까요? 예상 후 Enter/q.");
            var in=new Scanner(System.in);if(!in.hasNextLine()||in.nextLine().equalsIgnoreCase("q"))return;
        }
        try(var pg=EmbeddedPostgres.builder().setPort(0).start()){
            ds=pg.getPostgresDatabase();try(var c=connection()){
                sql(c,"CREATE TABLE outbox(id integer PRIMARY KEY,post_id integer NOT NULL,sent boolean NOT NULL)");
                sql(c,"CREATE TABLE relay_guard(id integer PRIMARY KEY)");sql(c,"INSERT INTO relay_guard VALUES (1)");c.commit();}
            var nodes=new TestKitNodes.Builder().setCombined(false).setNumBrokerNodes(1).setNumControllerNodes(1)
                .setBootstrapMetadataVersion(MetadataVersion.fromVersionString("4.1-IV1")).build();
            try(var cluster=new KafkaClusterTestKit.Builder(nodes).setConfigProp("offsets.topic.replication.factor","1").setConfigProp("offsets.topic.num.partitions","1").build()){
                cluster.format();cluster.startup();cluster.waitForReadyBrokers();bootstrap=cluster.bootstrapServers();
                try(var c=ds.getConnection();var s=c.createStatement();var v=s.executeQuery("SHOW server_version")){v.next();System.out.println("PostgreSQL="+v.getString(1)+" Kafka="+org.apache.kafka.common.utils.AppInfoParser.getVersion()+" overlappingDbConnections=2 scheduling=deterministic");}
                try(var admin=Admin.create(Map.of("bootstrap.servers",bootstrap));var p=new KafkaProducer<String,String>(Map.of("bootstrap.servers",bootstrap,"acks","all","enable.idempotence","true"),new StringSerializer(),new StringSerializer())){scenarios(admin,p);}
            }
        }
        System.out.println("ALL 5 SCENARIOS PASSED. No throughput or production scaling claim.");
    }
}
