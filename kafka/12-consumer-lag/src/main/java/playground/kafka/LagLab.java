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
import java.util.concurrent.atomic.*;

public final class LagLab {
 static final int TOTAL=160;
 record Case(String name,int workers,boolean skew,int paceMs,int workMs){}
 static void check(boolean v,String m){if(!v)throw new IllegalStateException(m);}
 static KafkaProducer<String,String> producer(String bs){return new KafkaProducer<>(Map.of("bootstrap.servers",bs,"acks","all","linger.ms","0"),new StringSerializer(),new StringSerializer());}
 static KafkaConsumer<String,String> consumer(String bs,String group){return new KafkaConsumer<>(Map.of("bootstrap.servers",bs,"group.id",group,"enable.auto.commit","false","auto.offset.reset","earliest","max.poll.records","10","group.protocol","classic","partition.assignment.strategy",RangeAssignor.class.getName(),"session.timeout.ms","6000","heartbeat.interval.ms","1000"),new StringDeserializer(),new StringDeserializer());}
 public static void main(String[]args)throws Exception{
  boolean guided=args.length==0||args[0].equals("guided");if(args.length>1||(args.length==1&&!Set.of("guided","verify").contains(args[0])))throw new IllegalArgumentException("guided|verify");
  try(var in=new Scanner(System.in)){
   if(guided){System.out.println("poll 후의 lag와 commit 기준 lag는 같을까요? 파티션2개에 Consumer3개를 두면 더 빨라질까요? [Enter/q]");if(!in.hasNextLine()||in.nextLine().equalsIgnoreCase("q"))return;}
   var nodes=new TestKitNodes.Builder().setCombined(false).setNumBrokerNodes(1).setNumControllerNodes(1).setBootstrapMetadataVersion(MetadataVersion.latestProduction()).build();
   try(var cluster=new KafkaClusterTestKit.Builder(nodes).setConfigProp("offsets.topic.replication.factor","1").setConfigProp("offsets.topic.num.partitions","1").setConfigProp("group.initial.rebalance.delay.ms","0").build()){
    cluster.format();cluster.startup();cluster.waitForReadyBrokers();String bs=cluster.bootstrapServers();
    try(var admin=Admin.create(Map.of("bootstrap.servers",bs))){
     boundaries(bs,admin);
     System.out.println("case,workers,assigned_workers,p0_records,p1_records,pace_ms,handler_ms,ingress_ms,ingress_records_s,completion_ms,completion_records_s,sample_ms,lag_p0,lag_p1,final_lag");
     for(var config:List.of(new Case("balanced-1",1,false,0,10),new Case("balanced-2",2,false,0,10),new Case("balanced-3",3,false,0,10),new Case("skewed-2",2,true,0,10),new Case("paced-1",1,false,12,20),new Case("paced-2",2,false,12,20)))run(bs,admin,config);
     System.out.println("PASS scenarios=6 eachRecords=160 exactKeysValuesPartitions=true finalCommittedLag=0 timingAssertions=false");
    }
   }
  }
 }
 static void boundaries(String bs,Admin admin)throws Exception{
  String topic="lag-boundary",group="lag-boundary-group";var tp=new TopicPartition(topic,0);
  admin.createTopics(List.of(new NewTopic(topic,1,(short)1))).all().get(15,TimeUnit.SECONDS);
  try(var p=producer(bs)){for(int i=0;i<100;i++)p.send(new ProducerRecord<>(topic,0,""+i,"v"+i));p.flush();}
  initializeOffsets(admin,group,Map.of(tp,new OffsetAndMetadata(0)));
  try(var c=consumer(bs,group)){
   c.assign(List.of(tp));c.seek(tp,0);ConsumerRecords<String,String> batch;long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
   do{batch=c.poll(Duration.ofMillis(100));}while(batch.isEmpty()&&System.nanoTime()<until);
   check(batch.count()==10,"Expected ten fetched records");long end=c.endOffsets(List.of(tp)).get(tp),position=c.position(tp),committed=c.committed(Set.of(tp)).get(tp).offset();
   check(end==100&&position==10&&committed==0,"Wrong boundary");System.out.println("PASS afterPoll end=100 position=10 committed=0 positionLag=90 committedLag=100 processed=0");
   int processed=0;for(var r:batch){check(r.key().equals(""+processed)&&r.value().equals("v"+processed),"Boundary data mismatch");Thread.sleep(10);processed++;}
   c.commitSync(Map.of(tp,new OffsetAndMetadata(position)));check(c.committed(Set.of(tp)).get(tp).offset()==10&&processed==10,"Commit mismatch");
   System.out.println("PASS afterWork processed=10 committed=10 committedLag=90");
  }
 }
 static void initializeOffsets(Admin admin,String group,Map<TopicPartition,OffsetAndMetadata> offsets)throws Exception{
  long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
  while(true){try{admin.alterConsumerGroupOffsets(group,offsets).all().get(10,TimeUnit.SECONDS);return;}catch(ExecutionException e){
   if(!(e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException)||System.nanoTime()>=until)throw e;
   Thread.sleep(100); // Topic creation acknowledgement can precede coordinator metadata propagation.
  }}
 }
 static int partition(Case config,int i){return config.skew?(i<150?0:1):i%2;}
 static long[] lag(Admin admin,String group,List<TopicPartition> tps)throws Exception{
  var req=new HashMap<TopicPartition,OffsetSpec>();for(var tp:tps)req.put(tp,OffsetSpec.latest());
  var end=admin.listOffsets(req).all().get(10,TimeUnit.SECONDS);var committed=admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10,TimeUnit.SECONDS);long[] result=new long[2];
  for(int i=0;i<2;i++){var tp=tps.get(i);check(committed.containsKey(tp),"Missing stored offset");result[i]=end.get(tp).offset()-committed.get(tp).offset();}return result;
 }
 static void run(String bs,Admin admin,Case config)throws Exception{
  String topic=config.name,group=topic+"-group";var tps=List.of(new TopicPartition(topic,0),new TopicPartition(topic,1));
  admin.createTopics(List.of(new NewTopic(topic,2,(short)1))).all().get(15,TimeUnit.SECONDS);
  initializeOffsets(admin,group,Map.of(tps.get(0),new OffsetAndMetadata(0),tps.get(1),new OffsetAndMetadata(0)));
  var start=new AtomicBoolean(false);var stop=new AtomicBoolean(false);var done=new AtomicInteger();var seen=ConcurrentHashMap.<String>newKeySet();var errors=new ConcurrentLinkedQueue<Throwable>();var assignments=new AtomicIntegerArray(config.workers);for(int i=0;i<config.workers;i++)assignments.set(i,-1);
  var pool=Executors.newFixedThreadPool(config.workers);var tasks=new ArrayList<Future<?>>();
  try{
   for(int id=0;id<config.workers;id++){final int worker=id;tasks.add(pool.submit(()->{
    try(var c=consumer(bs,group)){
     c.subscribe(List.of(topic));
     while(!stop.get()){
      if(!start.get()){
       c.pause(c.assignment());var records=c.poll(Duration.ofMillis(100));check(records.isEmpty(),"Consumed before start barrier");c.pause(c.assignment());assignments.set(worker,c.assignment().size());continue;
      }
      c.resume(c.assignment());var records=c.poll(Duration.ofMillis(100));var commits=new HashMap<TopicPartition,OffsetAndMetadata>();int count=0;
      for(var r:records){int i=Integer.parseInt(r.key());check(i>=0&&i<TOTAL&&r.value().equals("value-"+i)&&r.partition()==partition(config,i),"Invalid record");Thread.sleep(config.workMs);check(seen.add(r.key()),"Duplicate processing");commits.put(new TopicPartition(topic,r.partition()),new OffsetAndMetadata(r.offset()+1));count++;}
      if(!commits.isEmpty()){c.commitSync(commits);done.addAndGet(count);}
     }
    }catch(Throwable e){errors.add(e);}
   }));}
   long readyUntil=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);int assignedWorkers=0;boolean ready=false;
   while(System.nanoTime()<readyUntil){
    check(errors.isEmpty(),"Worker failed: "+errors.peek());
    var description=admin.describeConsumerGroups(List.of(group)).all().get(10,TimeUnit.SECONDS).get(group);
    int total=0,active=0;boolean reported=true;for(int i=0;i<config.workers;i++){int n=assignments.get(i);if(n<0)reported=false;else{total+=n;if(n>0)active++;}}
    if(description.members().size()==config.workers&&description.state().toString().equalsIgnoreCase("stable")&&reported&&total==2&&active==Math.min(2,config.workers)){assignedWorkers=active;ready=true;break;}Thread.sleep(50);
   }
   check(ready&&assignedWorkers==Math.min(2,config.workers),"Group assignment not ready");
   long began=0,ingressStarted,ingressEnded;long[] sampledLag;double sampleMs;
   try(var p=producer(bs)){
    p.partitionsFor(topic);if(config.paceMs>0){began=System.nanoTime();start.set(true);}ingressStarted=System.nanoTime();var futures=new ArrayList<Future<RecordMetadata>>();
    for(int i=0;i<TOTAL;i++){futures.add(p.send(new ProducerRecord<>(topic,partition(config,i),""+i,"value-"+i)));if(config.paceMs>0&&i<TOTAL-1)Thread.sleep(config.paceMs);}
    for(var f:futures)f.get(15,TimeUnit.SECONDS);ingressEnded=System.nanoTime();
    if(config.paceMs==0){long[] initial=lag(admin,group,tps);check(initial[0]+initial[1]==TOTAL,"Initial backlog mismatch");began=System.nanoTime();start.set(true);}
    if(config.paceMs==0)while(System.nanoTime()-began<TimeUnit.MILLISECONDS.toNanos(250))Thread.sleep(10);
    sampledLag=lag(admin,group,tps);sampleMs=(System.nanoTime()-began)/1e6;
    long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(20);while(done.get()<TOTAL&&System.nanoTime()<deadline){check(errors.isEmpty(),"Worker failed: "+errors.peek());Thread.sleep(10);}
    double completeMs=(System.nanoTime()-began)/1e6,ingressMs=(ingressEnded-ingressStarted)/1e6;check(done.get()==TOTAL&&seen.size()==TOTAL,"Incomplete processing");long[] finalLag=lag(admin,group,tps);check(finalLag[0]==0&&finalLag[1]==0,"Final lag remains");
    System.out.printf(Locale.ROOT,"%s,%d,%d,%d,%d,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%d,%d,0%n",config.name,config.workers,assignedWorkers,config.skew?150:80,config.skew?10:80,config.paceMs,config.workMs,ingressMs,TOTAL*1000/ingressMs,completeMs,TOTAL*1000/completeMs,sampleMs,sampledLag[0],sampledLag[1]);
   }
  }finally{stop.set(true);pool.shutdown();for(var task:tasks)task.get(15,TimeUnit.SECONDS);check(errors.isEmpty(),"Worker failed: "+errors.peek());}
 }
}
