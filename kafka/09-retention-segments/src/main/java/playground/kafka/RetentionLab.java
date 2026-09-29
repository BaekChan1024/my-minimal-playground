package playground.kafka;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.test.*;
import org.apache.kafka.server.common.MetadataVersion;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
public final class RetentionLab {
 static final String TOPIC="retention-events";static final TopicPartition TP=new TopicPartition(TOPIC,0);
 static void check(boolean v,String m){if(!v)throw new IllegalStateException(m);}
 public static void main(String[]args)throws Exception{
  boolean guided=args.length==0||args[0].equals("guided");if(args.length>1||(args.length==1&&!Set.of("guided","verify").contains(args[0])))throw new IllegalArgumentException("guided|verify");
  try(var in=new Scanner(System.in)){
   if(guided){System.out.println("커밋하지 않은 Consumer가 있으면 retention 삭제가 멈출까요? [Enter/q]");if(!in.hasNextLine()||in.nextLine().equalsIgnoreCase("q"))return;}
   Path base=Files.createTempDirectory("kafka-retention-lab-");
   var nodes=new TestKitNodes.Builder().setBaseDirectory(base).setCombined(false).setNumBrokerNodes(1).setNumControllerNodes(1).setBootstrapMetadataVersion(MetadataVersion.latestProduction()).build();
   try(var cluster=new KafkaClusterTestKit.Builder(nodes).setConfigProp("log.retention.check.interval.ms","200").setConfigProp("offsets.topic.replication.factor","1").setConfigProp("offsets.topic.num.partitions","1").build()){
    cluster.format();cluster.startup();cluster.waitForReadyBrokers();String bs=cluster.bootstrapServers();
    try(var admin=Admin.create(Map.of("bootstrap.servers",bs))){
     admin.createTopics(List.of(new NewTopic(TOPIC,1,(short)1).configs(Map.of("cleanup.policy","delete","retention.ms","-1","segment.bytes","1048576","segment.ms","1000","file.delete.delay.ms","100")))).all().get(15,TimeUnit.SECONDS);
     try(var producer=new KafkaProducer<String,String>(Map.of("bootstrap.servers",bs,"acks","all","batch.size","0"),new StringSerializer(),new StringSerializer())){
      for(int i=0;i<8;i++)producer.send(new ProducerRecord<>(TOPIC,0,System.currentTimeMillis()-3600000,"old-"+i,"X".repeat(600000))).get(10,TimeUnit.SECONDS);
      producer.send(new ProducerRecord<>(TOPIC,0,System.currentTimeMillis(),"fresh","Y".repeat(600000))).get(10,TimeUnit.SECONDS);
     }
     var before=read(bs,"earliest",null);check(before.equals(List.of("old-0","old-1","old-2","old-3","old-4","old-5","old-6","old-7","fresh")),"Initial records");
     int segments=segments(base);check(segments>=2,"No segment roll observed");
     admin.alterConsumerGroupOffsets("slow-reader",Map.of(TP,new OffsetAndMetadata(0))).all().get(10,TimeUnit.SECONDS);
     System.out.println("PASS before start=0 end=9 records=9 multipleSegments=true committed=0");
     admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.TOPIC,TOPIC),List.of(new AlterConfigOp(new ConfigEntry("retention.ms","600000"),AlterConfigOp.OpType.SET)))).all().get(10,TimeUnit.SECONDS);
     long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(60);long start;
     do{start=admin.listOffsets(Map.of(TP,OffsetSpec.earliest())).all().get(10,TimeUnit.SECONDS).get(TP).offset();if(start==8)break;Thread.sleep(100);}while(System.nanoTime()<until);
     check(start==8,"Expected old segments removed before fresh expires");
     long committed=admin.listConsumerGroupOffsets("slow-reader").partitionsToOffsetAndMetadata().get(10,TimeUnit.SECONDS).get(TP).offset();check(committed==0,"Retention changed group commit");
     boolean outOfRange=false;try{read(bs,"none",0L);}catch(OffsetOutOfRangeException e){outOfRange=true;}check(outOfRange,"Old offset should be unavailable");
     check(read(bs,"earliest",0L).equals(List.of("fresh")),"Earliest should skip deleted records");
     until=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);while(segments(base)>=segments&&System.nanoTime()<until)Thread.sleep(100);check(segments(base)<segments,"Old log files not removed");
     System.out.println("PASS after start=8 end=9 committed=0 oldSegmentFilesRemoved=true");
     System.out.println("PASS reset=none offset=0 error=OffsetOutOfRangeException");
     System.out.println("PASS reset=earliest offset=0 records=fresh");
    }
   }
  }
 }
 static int segments(Path base)throws Exception{try(var files=Files.walk(base)){return (int)files.filter(p->p.getParent()!=null&&p.getParent().getFileName().toString().equals(TOPIC+"-0")&&p.getFileName().toString().contains(".log")).count();}}
 static List<String> read(String bs,String reset,Long offset){
  try(var c=new KafkaConsumer<String,String>(Map.of("bootstrap.servers",bs,"enable.auto.commit","false","auto.offset.reset",reset,"default.api.timeout.ms","10000"),new StringDeserializer(),new StringDeserializer())){
   c.assign(List.of(TP));if(offset==null)c.seekToBeginning(List.of(TP));else c.seek(TP,offset);
   var keys=new ArrayList<String>();long end=c.endOffsets(List.of(TP)).get(TP);long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
   do{for(var r:c.poll(Duration.ofMillis(100)))keys.add(r.key());if(c.position(TP)>=end)return keys;}while(System.nanoTime()<until);
   throw new IllegalStateException("Read timeout");
  }
 }
}
