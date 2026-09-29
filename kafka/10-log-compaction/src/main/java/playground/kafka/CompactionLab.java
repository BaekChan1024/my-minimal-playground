package playground.kafka;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.test.*;
import org.apache.kafka.server.common.MetadataVersion;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
public final class CompactionLab {
 static final String TOPIC="compacted-state";static final TopicPartition TP=new TopicPartition(TOPIC,0);
 record Row(long offset,String key,String value){}
 static void check(boolean v,String m){if(!v)throw new IllegalStateException(m);}
 public static void main(String[]args)throws Exception{
  boolean guided=args.length==0||args[0].equals("guided");if(args.length>1||(args.length==1&&!Set.of("guided","verify").contains(args[0])))throw new IllegalArgumentException("guided|verify");
  try(var in=new Scanner(System.in)){
   if(guided){System.out.println("같은 key를 쓰면 이전 값은 즉시 사라질까요? null과 빈 문자열은 같은가요? [Enter/q]");if(!in.hasNextLine()||in.nextLine().equalsIgnoreCase("q"))return;}
   var nodes=new TestKitNodes.Builder().setCombined(false).setNumBrokerNodes(1).setNumControllerNodes(1).setBootstrapMetadataVersion(MetadataVersion.latestProduction()).build();
   try(var cluster=new KafkaClusterTestKit.Builder(nodes).setConfigProp("log.cleaner.enable","true").setConfigProp("log.cleaner.backoff.ms","100").build()){
    cluster.format();cluster.startup();cluster.waitForReadyBrokers();String bs=cluster.bootstrapServers();
    try(var admin=Admin.create(Map.of("bootstrap.servers",bs))){
     admin.createTopics(List.of(new NewTopic(TOPIC,1,(short)1).configs(Map.of("cleanup.policy","compact","segment.bytes","1048576","min.compaction.lag.ms","3600000","delete.retention.ms","3600000","min.cleanable.dirty.ratio","0.01")))).all().get(15,TimeUnit.SECONDS);
     try(var p=new KafkaProducer<String,String>(Map.of("bootstrap.servers",bs,"acks","all","batch.size","0"),new StringSerializer(),new StringSerializer())){
      send(p,"A","old");send(p,"B","present");send(p,"A","new");send(p,"B",null);send(p,"roll","marker-1");send(p,"roll","marker-2");
     }
     var before=business(read(bs));check(before.equals(List.of(new Row(0,"A","old"),new Row(1,"B","present"),new Row(2,"A","new"),new Row(3,"B",null))),"Initial history");
     var expectedState=Map.of("A","new");check(state(before).equals(expectedState),"Initial replay");
     System.out.println("PASS before businessRecords=4 offsets=0,1,2,3 state=A:new tombstone=true");
     admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.TOPIC,TOPIC),List.of(new AlterConfigOp(new ConfigEntry("min.compaction.lag.ms","0"),AlterConfigOp.OpType.SET)))).all().get(10,TimeUnit.SECONDS);
     var expected=List.of(new Row(2,"A","new"),new Row(3,"B",null));long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(60);List<Row> after;
     do{after=business(read(bs));if(after.equals(expected))break;Thread.sleep(200);}while(System.nanoTime()<until);
     check(after.equals(expected),"Compaction did not remove obsolete values");check(state(after).equals(expectedState),"State changed after cleaning");
     System.out.println("PASS after businessRecords=2 offsets=2,3 state=A:new tombstoneRetained=true");
     try(var p=new KafkaProducer<String,String>(Map.of("bootstrap.servers",bs,"acks","all"),new StringSerializer(),new StringSerializer())){p.send(new ProducerRecord<>(TOPIC,0,"B","")).get(10,TimeUnit.SECONDS);}
     var finalState=state(business(read(bs)));check(finalState.containsKey("B")&&finalState.get("B").isEmpty(),"Empty string must be a value");
     System.out.println("PASS emptyString BExists=true BValueLength=0");
    }
   }
  }
 }
 static void send(KafkaProducer<String,String> p,String k,String v)throws Exception{p.send(new ProducerRecord<>(TOPIC,0,k,v==null?null:v+"|"+"X".repeat(600000))).get(10,TimeUnit.SECONDS);}
 static List<Row> business(List<Row> rows){return rows.stream().filter(r->!r.key.equals("roll")).toList();}
 static Map<String,String> state(List<Row> rows){var m=new HashMap<String,String>();for(var r:rows){if(r.value==null)m.remove(r.key);else m.put(r.key,r.value);}return m;}
 static List<Row> read(String bs){try(var c=new KafkaConsumer<String,String>(Map.of("bootstrap.servers",bs,"enable.auto.commit","false","auto.offset.reset","earliest","default.api.timeout.ms","10000"),new StringDeserializer(),new StringDeserializer())){
  c.assign(List.of(TP));c.seek(TP,0);long end=c.endOffsets(List.of(TP)).get(TP),until=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);var rows=new ArrayList<Row>();
  do{for(var r:c.poll(Duration.ofMillis(100))){String v=r.value();rows.add(new Row(r.offset(),r.key(),v==null?null:v.contains("|")?v.substring(0,v.indexOf('|')):v));}if(c.position(TP)>=end)return rows;}while(System.nanoTime()<until);throw new IllegalStateException("Read timeout");
 }}
}
