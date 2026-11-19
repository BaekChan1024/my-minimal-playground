package playground.sourcefailure;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.server.common.MetadataVersion;

/** 실제 임시 Kafka(브로커 1·컨트롤러 1). */
final class KafkaRig implements AutoCloseable {
    private final KafkaClusterTestKit cluster;
    private final Admin admin;
    private final KafkaProducer<String, String> producer;
    final String bootstrap;

    KafkaRig() throws Exception {
        var nodes = new TestKitNodes.Builder().setCombined(false).setNumBrokerNodes(1).setNumControllerNodes(1)
                .setBootstrapMetadataVersion(MetadataVersion.latestProduction()).build();
        cluster = new KafkaClusterTestKit.Builder(nodes)
                .setConfigProp("offsets.topic.replication.factor", "1")
                .setConfigProp("offsets.topic.num.partitions", "1")
                .setConfigProp("group.initial.rebalance.delay.ms", "0")
                .build();
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
        bootstrap = cluster.bootstrapServers();
        admin = Admin.create(Map.of("bootstrap.servers", bootstrap));
        producer = new KafkaProducer<>(Map.of("bootstrap.servers", bootstrap, "acks", "all", "linger.ms", "0"),
                new StringSerializer(), new StringSerializer());
    }

    void topic(String name, int partitions) throws Exception {
        admin.createTopics(List.of(new NewTopic(name, partitions, (short) 1))).all().get(15, TimeUnit.SECONDS);
        producer.partitionsFor(name);
    }

    /** 파티션을 직접 지정해 보낸다. 실험마다 어느 이벤트가 어느 파티션에 있는지 통제하기 위해서다. */
    void send(String topic, int partition, Event event) throws Exception {
        producer.send(new ProducerRecord<>(topic, partition, Long.toString(event.contentId()), event.json())).get(15, TimeUnit.SECONDS);
    }

    void sendRaw(String topic, String key, String value) throws Exception {
        producer.send(new ProducerRecord<>(topic, 0, key, value)).get(15, TimeUnit.SECONDS);
    }

    /** 토픽 전체를 처음부터 읽는다(그룹 없음). DLT 내용 확인과 재처리에 쓴다. */
    List<Event> readAll(String topic, int partitions) {
        var tps = new ArrayList<TopicPartition>();
        for (int p = 0; p < partitions; p++) tps.add(new TopicPartition(topic, p));
        try (var consumer = consumer(null, 500, Map.of())) {
            consumer.assign(tps);
            consumer.seekToBeginning(tps);
            var end = consumer.endOffsets(tps);
            var events = new ArrayList<Event>();
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (tps.stream().anyMatch(tp -> consumer.position(tp) < end.get(tp)) && System.nanoTime() < until) {
                for (var r : consumer.poll(Duration.ofMillis(100))) events.add(Event.parse(r.value()));
            }
            return events;
        }
    }

    /** 커밋된 오프셋. 커밋이 없으면 0. */
    long committed(String group, TopicPartition tp) throws Exception {
        var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
        var meta = offsets.get(tp);
        return meta == null ? 0 : meta.offset();
    }

    KafkaConsumer<String, String> consumer(String group, int maxPollRecords, Map<String, Object> extra) {
        var config = new HashMap<String, Object>(Map.of(
                "bootstrap.servers", bootstrap,
                "enable.auto.commit", "false",
                "auto.offset.reset", "earliest",
                "max.poll.records", Integer.toString(maxPollRecords),
                "group.protocol", "classic",
                "partition.assignment.strategy", RangeAssignor.class.getName()));
        if (group != null) config.put("group.id", group);
        config.putAll(extra);
        return new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer());
    }

    @Override
    public void close() throws Exception {
        producer.close();
        admin.close();
        cluster.close();
    }
}
