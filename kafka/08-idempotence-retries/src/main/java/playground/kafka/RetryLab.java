package playground.kafka;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.server.common.MetadataVersion;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public final class RetryLab {
    static final String TOPIC="replicated-orders";
    static void check(boolean b,String m){if(!b)throw new IllegalStateException(m);}
    public static void main(String[] args)throws Exception{
        boolean guided=args.length==0||args[0].equals("guided");
        if(args.length>1||(args.length==1&&!Set.of("guided","verify").contains(args[0])))throw new IllegalArgumentException("guided|verify");
        try(var in=new Scanner(System.in)){
            if(guided){System.out.println("내부 재시도와 같은 업무의 새 send는 같은가요? [Enter/q]");if(!in.hasNextLine()||in.nextLine().equalsIgnoreCase("q"))return;}
            var nodes=new TestKitNodes.Builder().setCombined(false).setNumControllerNodes(1).setNumBrokerNodes(2).setBootstrapMetadataVersion(MetadataVersion.latestProduction()).build();
            try(var b=new KafkaClusterTestKit.Builder(nodes).setConfigProp("replica.lag.time.max.ms","3000").build()){
                b.format();b.startup();b.waitForReadyBrokers();String bootstrap=b.bootstrapServers();
                try(var admin=Admin.create(Map.of("bootstrap.servers",bootstrap,"request.timeout.ms","5000","default.api.timeout.ms","15000"))){
                    admin.createTopics(List.of(new NewTopic(TOPIC,1,(short)2).configs(Map.of("min.insync.replicas","2")))).all().get(20,TimeUnit.SECONDS);
                    var initial=waitIsr(admin,2);int follower=initial.replicas().stream().map(Node::id).filter(id->id!=initial.leader().id()).findFirst().orElseThrow();
                    stop(b,follower);waitIsr(admin,1);
                    var props=new Properties();props.put("bootstrap.servers",bootstrap);props.put("acks","all");props.put("enable.idempotence","true");props.put("max.in.flight.requests.per.connection","5");props.put("request.timeout.ms","2000");props.put("delivery.timeout.ms","60000");props.put("retry.backoff.ms","200");
                    try(var producer=new KafkaProducer<String,String>(props,new StringSerializer(),new StringSerializer())){
                        var a=producer.send(new ProducerRecord<>(TOPIC,0,"same-business-id","A"));
                        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(20);
                        while(retries(producer)<1&&System.nanoTime()<deadline)Thread.sleep(100);
                        check(retries(producer)>=1&&!a.isDone(),"Real retry not observed while ISR is insufficient");
                        System.out.println("PASS pending retryObserved=true futureDone=false ISR=1");
                        b.brokers().get(follower).startup();waitIsr(admin,2);a.get(45,TimeUnit.SECONDS);
                        producer.send(new ProducerRecord<>(TOPIC,0,"same-business-id","B")).get(15,TimeUnit.SECONDS);
                        check(read(bootstrap).equals(List.of("A","B")),"Retry output order");
                        System.out.println("PASS recovered internalRetry=true records=A,B");
                        producer.send(new ProducerRecord<>(TOPIC,0,"same-business-id","A")).get(15,TimeUnit.SECONDS);
                        check(read(bootstrap).equals(List.of("A","B","A")),"Explicit new send should be a new record");
                        System.out.println("PASS explicitResend sameKey=true idempotence=true records=A,B,A");
                    }
                    props.put("max.in.flight.requests.per.connection","6");boolean invalid=false;
                    try(var invalidProducer=new KafkaProducer<String,String>(props,new StringSerializer(),new StringSerializer())){}catch(org.apache.kafka.common.config.ConfigException e){invalid=true;}
                    check(invalid,"Incompatible idempotence config accepted");System.out.println("PASS incompatibleConfig maxInFlight=6 rejected=ConfigException");
                }
            }
        }
    }
    static double retries(KafkaProducer<String,String> p){return p.metrics().entrySet().stream().filter(e->e.getKey().name().equals("record-retry-total")&&e.getKey().group().equals("producer-metrics")).mapToDouble(e->((Number)e.getValue().metricValue()).doubleValue()).findFirst().orElse(0);}
    static void stop(KafkaClusterTestKit b,int id){var server=b.brokers().get(id);server.shutdown();server.awaitShutdown();}
    static org.apache.kafka.common.TopicPartitionInfo waitIsr(Admin a,int n)throws Exception{
        long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(40);
        do{var p=a.describeTopics(List.of(TOPIC)).allTopicNames().get(15,TimeUnit.SECONDS).get(TOPIC).partitions().get(0);if(p.isr().size()==n&&p.leader()!=null&&p.leader().id()>=0)return p;Thread.sleep(150);}while(System.nanoTime()<until);
        throw new IllegalStateException("ISR did not reach "+n);
    }
    static void send(String bootstrap,String acks,String value)throws Exception{
        var p=new Properties();p.put("bootstrap.servers",bootstrap);p.put("acks",acks);p.put("enable.idempotence","false");p.put("retries","0");p.put("request.timeout.ms","5000");p.put("delivery.timeout.ms","10000");p.put("max.block.ms","10000");
        try(var producer=new KafkaProducer<String,String>(p,new StringSerializer(),new StringSerializer())){producer.send(new ProducerRecord<>(TOPIC,0,"order",value)).get(15,TimeUnit.SECONDS);}
    }
    static List<String> read(String bootstrap){
        var p=new Properties();p.put("bootstrap.servers",bootstrap);p.put("enable.auto.commit","false");p.put("default.api.timeout.ms","10000");
        try(var c=new KafkaConsumer<String,String>(p,new StringDeserializer(),new StringDeserializer())){
            var tp=new TopicPartition(TOPIC,0);c.assign(List.of(tp));c.seek(tp,0);long end=c.endOffsets(List.of(tp)).get(tp);var values=new ArrayList<String>();long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
            while(c.position(tp)<end&&System.nanoTime()<until)for(var r:c.poll(Duration.ofMillis(100)))values.add(r.value());
            check(c.position(tp)>=end,"Read timeout");return values;
        }
    }
}
