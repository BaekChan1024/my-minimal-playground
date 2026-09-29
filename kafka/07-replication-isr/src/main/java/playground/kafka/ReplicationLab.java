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

public final class ReplicationLab {
    static final String TOPIC="replicated-orders";
    static void check(boolean b,String m){if(!b)throw new IllegalStateException(m);}
    public static void main(String[] args)throws Exception{
        System.out.println("Kafka "+org.apache.kafka.common.utils.AppInfoParser.getVersion()+" | three in-process brokers | controlled shutdown");
        boolean guided=args.length==0||args[0].equals("guided");
        if(args.length>1||(args.length==1&&!Set.of("guided","verify").contains(args[0])))throw new IllegalArgumentException("guided|verify");
        try(var in=new Scanner(System.in)){
            if(guided){System.out.println("RF=3, minISR=2. 복제본 한 대와 두 대가 빠졌을 때 acks=all은? [Enter/q]");if(!in.hasNextLine()||in.nextLine().equalsIgnoreCase("q"))return;}
            var nodes=new TestKitNodes.Builder().setCombined(false).setNumControllerNodes(1).setNumBrokerNodes(3)
                    .setBootstrapMetadataVersion(MetadataVersion.latestProduction()).build();
            try(var b=new KafkaClusterTestKit.Builder(nodes).setConfigProp("offsets.topic.replication.factor","3")
                    .setConfigProp("offsets.topic.num.partitions","1").setConfigProp("replica.lag.time.max.ms","3000").build()){
                b.format();b.startup();b.waitForReadyBrokers();String bootstrap=b.bootstrapServers();
                try(var admin=Admin.create(Map.of("bootstrap.servers",bootstrap,"default.api.timeout.ms","15000","request.timeout.ms","5000"))){
                    admin.createTopics(List.of(new NewTopic(TOPIC,1,(short)3).configs(Map.of("min.insync.replicas","2","unclean.leader.election.enable","false")))).all().get(20,TimeUnit.SECONDS);
                    var initial=waitIsr(admin,3);int leader=initial.leader().id();
                    var followers=initial.replicas().stream().map(Node::id).filter(id->id!=leader).toList();
                    send(bootstrap,"all","A");
                    stop(b,followers.get(0));waitIsr(admin,2);send(bootstrap,"all","B");
                    System.out.println("PASS RF=3 ISR=2 minISR=2 acks=all accepted=B");
                    stop(b,followers.get(1));waitIsr(admin,1);
                    boolean refused=false;
                    try{send(bootstrap,"all","REJECTED");}catch(ExecutionException e){if(e.getCause() instanceof NotEnoughReplicasException)refused=true;else throw e;}
                    check(refused,"Expected NotEnoughReplicasException");
                    System.out.println("PASS RF=3 ISR=1 minISR=2 acks=all rejected=NotEnoughReplicasException");
                    send(bootstrap,"1","C");
                    System.out.println("PASS RF=3 ISR=1 minISR=2 acks=1 accepted=C");
                    for(int id:followers)b.brokers().get(id).startup();
                    waitIsr(admin,3);check(read(bootstrap).equals(List.of("A","B","C")),"Recovery contents");
                    stop(b,leader);var replacement=waitIsr(admin,2);check(replacement.leader().id()!=leader,"No leader change");
                    check(read(bootstrap).equals(List.of("A","B","C")),"Leader replacement contents");
                    System.out.println("PASS recoveredISR=3 leaderChanged=true records=A,B,C rejectedAbsent=true");
                }
            }
        }
    }
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
