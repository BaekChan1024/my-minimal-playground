package playground.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.test.*;
import org.apache.kafka.server.common.MetadataVersion;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public final class BatchingLab {
 static final int WARMUP=40,COUNT=600,ROUNDS=3;
 static final String PAD="event-body-".repeat(100);
 record Case(String name,boolean synchronous,int linger,String compression){}
 static final List<Case> CASES=List.of(new Case("sync-0-none",true,0,"none"),new Case("sync-20-none",true,20,"none"),new Case("async-0-none",false,0,"none"),new Case("async-20-none",false,20,"none"),new Case("async-20-gzip",false,20,"gzip"));
 static void check(boolean v,String m){if(!v)throw new IllegalStateException(m);}
 static String value(int i){return "id:"+i+"|"+PAD;}
 public static void main(String[]args)throws Exception{
  boolean guided=args.length==0||args[0].equals("guided");if(args.length>1||(args.length==1&&!Set.of("guided","verify").contains(args[0])))throw new IllegalArgumentException("guided|verify");
  try(var in=new Scanner(System.in)){
   if(guided){System.out.println("매 send 뒤 get을 기다리면 batch가 채워질까요? gzip이 항상 지연도 줄일까요? [Enter/q]");if(!in.hasNextLine()||in.nextLine().equalsIgnoreCase("q"))return;}
   var nodes=new TestKitNodes.Builder().setCombined(false).setNumBrokerNodes(1).setNumControllerNodes(1).setBootstrapMetadataVersion(MetadataVersion.latestProduction()).build();
   try(var cluster=new KafkaClusterTestKit.Builder(nodes).build()){
    cluster.format();cluster.startup();cluster.waitForReadyBrokers();String bs=cluster.bootstrapServers();
    try(var admin=Admin.create(Map.of("bootstrap.servers",bs))){
     System.out.println("case,round,elapsed_ms,records_s,ack_p50_ms,ack_p99_ms,batch_bytes_avg,compression_ratio");
     for(int round=1;round<=ROUNDS;round++)for(int j=0;j<CASES.size();j++){
      var config=CASES.get((j+round-1)%CASES.size());String topic=config.name+"-"+round;
      admin.createTopics(List.of(new NewTopic(topic,1,(short)1))).all().get(15,TimeUnit.SECONDS);
      run(bs,topic,config,round);verifyRecords(bs,topic);
     }
     System.out.println("PASS cases=5 rounds=3 eachRecords=640 exactKeyValueOrder=true timingAssertions=false");
    }
   }
  }
 }
 static void run(String bs,String topic,Case config,int round)throws Exception{
  var props=new HashMap<String,Object>();props.put("bootstrap.servers",bs);props.put("acks","all");props.put("enable.idempotence",true);props.put("batch.size",131072);props.put("linger.ms",config.linger);props.put("compression.type",config.compression);
  try(var p=new KafkaProducer<String,String>(props,new StringSerializer(),new StringSerializer())){
   p.partitionsFor(topic);var warm=new ArrayList<Future<RecordMetadata>>();
   for(int i=0;i<WARMUP;i++){var f=p.send(new ProducerRecord<>(topic,0,""+i,value(i)));warm.add(f);if(config.synchronous)f.get(15,TimeUnit.SECONDS);}
   for(var f:warm)f.get(15,TimeUnit.SECONDS);p.flush();
   long[]latencies=new long[COUNT];var completed=new CountDownLatch(COUNT);var futures=new ArrayList<Future<RecordMetadata>>();long started=System.nanoTime();
   for(int i=0;i<COUNT;i++){
    int index=i,id=i+WARMUP;long sent=System.nanoTime();
    var f=p.send(new ProducerRecord<>(topic,0,""+id,value(id)),(meta,error)->{latencies[index]=System.nanoTime()-sent;completed.countDown();});futures.add(f);
    if(config.synchronous)f.get(15,TimeUnit.SECONDS);
   }
   for(var f:futures)f.get(15,TimeUnit.SECONDS);check(completed.await(15,TimeUnit.SECONDS),"Callback timeout");double ms=(System.nanoTime()-started)/1e6;
   Arrays.sort(latencies);double p50=latencies[(int)Math.ceil(COUNT*.5)-1]/1e6,p99=latencies[(int)Math.ceil(COUNT*.99)-1]/1e6;
   double ratio=metric(p,"compression-rate-avg"),batch=metric(p,"batch-size-avg");check(Double.isFinite(ratio)&&Double.isFinite(batch),"Missing metrics");
   System.out.printf(Locale.ROOT,"%s,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.4f%n",config.name,round,ms,COUNT*1000/ms,p50,p99,batch,ratio);
  }
 }
 static double metric(KafkaProducer<String,String> p,String name){return p.metrics().entrySet().stream().filter(e->e.getKey().group().equals("producer-metrics")&&e.getKey().name().equals(name)).mapToDouble(e->((Number)e.getValue().metricValue()).doubleValue()).findFirst().orElseThrow();}
 static void verifyRecords(String bs,String topic){var tp=new TopicPartition(topic,0);
  try(var c=new KafkaConsumer<String,String>(Map.of("bootstrap.servers",bs,"enable.auto.commit","false","auto.offset.reset","earliest"),new StringDeserializer(),new StringDeserializer())){
   c.assign(List.of(tp));c.seek(tp,0);int count=0;long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
   while(count<WARMUP+COUNT&&System.nanoTime()<until){for(var r:c.poll(Duration.ofMillis(100))){check(r.offset()==count&&r.key().equals(""+count)&&r.value().equals(value(count)),"Record mismatch "+count);count++;}}
   check(count==WARMUP+COUNT&&c.endOffsets(List.of(tp)).get(tp)==count,"Count mismatch");
  }
 }
}
