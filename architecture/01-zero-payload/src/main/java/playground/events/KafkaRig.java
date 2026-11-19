package playground.events;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.server.common.MetadataVersion;
import playground.events.Events.Event;

/** 실제 임시 Kafka(브로커 1·컨트롤러 1). 토픽은 파티션 1개이고 key는 글 ID다. */
final class KafkaRig implements AutoCloseable {
    /** 배치 처리 중 index번째 레코드에서 다시 시도해야 한다. */
    static final class RetryAt extends Exception {
        final int index;
        RetryAt(int index, Throwable cause) {
            super(cause.getMessage(), cause);
            this.index = index;
        }
    }

    interface BatchHandler {
        void handle(List<Event> batch) throws RetryAt;
    }

    /** drain 한 번의 결과. committed는 이 그룹의 커밋 오프셋, end는 토픽 끝 오프셋이다. */
    record Drain(int handled, int retries, int batches, long committed, long end, String lastRetry) {}

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
        // 기본 max.request.size(1048576)는 그대로 둔다. 큰 본문 실험이 이 기본값을 관측한다.
        producer = new KafkaProducer<>(Map.of("bootstrap.servers", bootstrap, "acks", "all", "linger.ms", "0"),
                new StringSerializer(), new StringSerializer());
    }

    void topic(String name) throws Exception {
        admin.createTopics(List.of(new NewTopic(name, 1, (short) 1))).all().get(15, TimeUnit.SECONDS);
        producer.partitionsFor(name);
    }

    /** 전송 성공(브로커 확인)까지 기다린다. 실패하면 원인 예외를 그대로 던진다. */
    RecordMetadata send(String topic, long key, String value) throws Exception {
        try {
            return producer.send(new ProducerRecord<>(topic, Long.toString(key), value)).get(15, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception cause) throw cause;
            throw e;
        }
    }

    static int bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    /** 토픽에 남아 있는 모든 value를 처음부터 읽는다. 그룹 오프셋은 건드리지 않는다. */
    List<String> readAll(String topic) {
        var tp = new TopicPartition(topic, 0);
        try (var consumer = consumer(null, 500)) {
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            long end = consumer.endOffsets(List.of(tp)).get(tp);
            var values = new ArrayList<String>();
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (consumer.position(tp) < end && System.nanoTime() < until) {
                for (var r : consumer.poll(Duration.ofMillis(100))) values.add(r.value());
            }
            if (consumer.position(tp) < end) throw new IllegalStateException("Could not read whole topic " + topic);
            return values;
        }
    }

    static BatchHandler each(ReadModel.Handler handler) {
        return batch -> {
            for (int i = 0; i < batch.size(); i++) {
                try {
                    handler.handle(batch.get(i));
                } catch (ReadModel.RetryLater e) {
                    throw new RetryAt(i, e);
                }
            }
        };
    }

    /**
     * 그룹의 커밋 위치부터 읽어 끝 오프셋까지 처리하거나 timeout까지 반복한다.
     * 처리에 성공한 레코드까지만 커밋한다. 다시 시도해야 하면 실패한 레코드로 seek하고
     * 100ms 쉰 뒤 다시 poll한다(무한 재시도. DLT 이동은 이 실습 범위가 아니다).
     */
    Drain drain(String topic, String group, BatchHandler handler, int maxPollRecords, Duration timeout) {
        var tp = new TopicPartition(topic, 0);
        int handled = 0, retries = 0, batches = 0;
        String lastRetry = null;
        try (var consumer = consumer(group, maxPollRecords)) {
            consumer.assign(List.of(tp));
            var stored = consumer.committed(Set.of(tp)).get(tp);
            consumer.seek(tp, stored == null ? 0 : stored.offset());
            long end = consumer.endOffsets(List.of(tp)).get(tp);
            long committed = stored == null ? 0 : stored.offset();
            long until = System.nanoTime() + timeout.toNanos();
            while (committed < end && System.nanoTime() < until) {
                var records = consumer.poll(Duration.ofMillis(100)).records(tp);
                if (records.isEmpty()) continue;
                batches++;
                var events = new ArrayList<Event>();
                for (ConsumerRecord<String, String> r : records) events.add(Events.decode(r.value()));
                long next;
                try {
                    handler.handle(events);
                    handled += events.size();
                    next = records.getLast().offset() + 1;
                } catch (RetryAt e) {
                    handled += e.index;
                    retries++;
                    lastRetry = e.getMessage();
                    next = records.get(e.index).offset();
                    consumer.seek(tp, next);
                    sleep(100);
                }
                if (next > committed) {
                    consumer.commitSync(Map.of(tp, new OffsetAndMetadata(next)));
                    committed = next;
                }
            }
            return new Drain(handled, retries, batches, committed, end, lastRetry);
        }
    }

    private KafkaConsumer<String, String> consumer(String group, int maxPollRecords) {
        var config = new java.util.HashMap<String, Object>(Map.of(
                "bootstrap.servers", bootstrap,
                "enable.auto.commit", "false",
                "auto.offset.reset", "earliest",
                "max.poll.records", Integer.toString(maxPollRecords)));
        if (group != null) config.put("group.id", group);
        return new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer());
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() throws Exception {
        producer.close();
        admin.close();
        cluster.close();
    }
}
