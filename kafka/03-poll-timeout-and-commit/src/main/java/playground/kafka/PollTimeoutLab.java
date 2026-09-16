package playground.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Actual local Kafka membership expiry; the effect counter is only an in-memory model. */
public final class PollTimeoutLab {
    private static final String TOPIC = "poll-boundary";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);
    private static final Duration API_TIMEOUT = Duration.ofSeconds(10);

    public static void main(String[] args) throws Exception {
        if (args.length > 1 || (args.length == 1 && !Set.of("guided", "verify").contains(args[0])))
            throw new IllegalArgumentException("Usage: PollTimeoutLab [guided|verify]");
        boolean guided = args.length == 0 || args[0].equals("guided");
        try (var input = new Scanner(System.in)) {
            System.out.println("Kafka " + AppInfoParser.getVersion() + " | classic, RangeAssignor, dynamic membership | max.poll.interval.ms=2000");
            System.out.println("Local broker only. Effects are an in-memory counter, not DB writes.");
            if (guided && !next(input, "A가 효과를 반영한 뒤 커밋하지 못하면 B가 같은 레코드를 읽을까요?")) return;
            var broker = new EmbeddedKafkaKraftBroker(1, 1, TOPIC);
            broker.brokerProperties(Map.of("offsets.topic.replication.factor", "1",
                    "offsets.topic.num.partitions", "1", "group.initial.rebalance.delay.ms", "0"));
            broker.afterPropertiesSet();
            try (var admin = Admin.create(Map.of("bootstrap.servers", broker.getBrokersAsString()));
                 var producer = new KafkaProducer<String, String>(Map.of("bootstrap.servers", broker.getBrokersAsString(),
                         "acks", "all", "enable.idempotence", "true"), new StringSerializer(), new StringSerializer())) {
                var metadata = producer.send(new ProducerRecord<>(TOPIC, "event-1", "effect-1")).get(15, TimeUnit.SECONDS);
                require(metadata.partition() == 0 && metadata.offset() == 0, "Unexpected seed identity");
                runCase(broker.getBrokersAsString(), admin, false);
                if (guided && !next(input, "A가 효과 반영과 커밋을 모두 마친 뒤 멈추면 B의 결과는 어떻게 달라질까요?")) return;
                runCase(broker.getBrokersAsString(), admin, true);
                System.out.println("DONE: 2 cases verified; no crash injection, DB transaction, or performance benchmark.");
            } finally {
                broker.destroy();
            }
        }
    }

    private static boolean next(Scanner input, String question) {
        System.out.println(question + " [Enter: 진행 / q: 종료]");
        return input.hasNextLine() && !input.nextLine().trim().equalsIgnoreCase("q");
    }

    private static KafkaConsumer<String, String> consumer(String bootstrap, String group, String name) {
        var props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", group);
        props.put("client.id", name);
        props.put("group.protocol", "classic");
        props.put("partition.assignment.strategy", RangeAssignor.class.getName());
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", "1");
        props.put("max.poll.interval.ms", "2000");
        props.put("session.timeout.ms", "6000");
        props.put("heartbeat.interval.ms", "1000");
        props.put("default.api.timeout.ms", "10000");
        return new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    }

    private static void runCase(String bootstrap, Admin admin, boolean commitBeforePause) throws Exception {
        String group = commitBeforePause ? "committed-first" : "effect-only";
        var ready = new CountDownLatch(1);
        var releaseOld = new CountDownLatch(1);
        var stopNew = new AtomicBoolean();
        var effects = new AtomicInteger();
        var newReads = new AtomicInteger();
        var assignedNew = new AtomicBoolean();
        var observedNewPosition = new AtomicLong(-1);
        var lateCommitRejected = new AtomicBoolean();
        var pool = Executors.newFixedThreadPool(2);
        Future<?> newer = null;
        Future<?> older = pool.submit(() -> {
            try (var c = consumer(bootstrap, group, "old")) {
                c.subscribe(List.of(TOPIC));
                ConsumerRecords<String, String> records;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                do {
                    records = c.poll(Duration.ofMillis(100));
                    require(System.nanoTime() < deadline, "Old consumer read timeout");
                } while (records.isEmpty());
                require(records.count() == 1, "Old read count");
                checkIdentity(records.iterator().next());
                effects.incrementAndGet(); // Model a completed external effect; not a real database.
                if (commitBeforePause) c.commitSync(records.nextOffsets(), API_TIMEOUT);
                System.out.printf("%s old: offset=0 position=%d committed=%s effects=%d%n", group,
                        c.position(TP), Optional.ofNullable(c.committed(Set.of(TP)).get(TP)).map(OffsetAndMetadata::offset).orElse(null), effects.get());
                ready.countDown();
                // No poll while blocked. Main thread releases only after the new member has taken over.
                require(releaseOld.await(40, TimeUnit.SECONDS), "Release old timeout");
                try {
                    c.commitSync(records.nextOffsets(), API_TIMEOUT);
                    throw new IllegalStateException("Expected late commit to fail after membership expiry");
                } catch (CommitFailedException expected) {
                    lateCommitRejected.set(true);
                    System.out.println(group + " old: late commit rejected (CommitFailedException)");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });
        try {
            require(ready.await(35, TimeUnit.SECONDS), "Old consumer not ready");
            newer = pool.submit(() -> {
                try (var c = consumer(bootstrap, group, "new")) {
                    c.subscribe(List.of(TOPIC), new ConsumerRebalanceListener() {
                        public void onPartitionsRevoked(Collection<TopicPartition> partitions) { assignedNew.set(false); }
                        public void onPartitionsAssigned(Collection<TopicPartition> partitions) { assignedNew.set(partitions.contains(TP)); }
                    });
                    while (!stopNew.get()) {
                        var records = c.poll(Duration.ofMillis(100));
                        for (var r : records) {
                            checkIdentity(r);
                            effects.incrementAndGet();
                            newReads.incrementAndGet();
                        }
                        if (!records.isEmpty()) c.commitSync(records.nextOffsets(), API_TIMEOUT);
                        if (c.assignment().contains(TP)) observedNewPosition.set(c.position(TP));
                    }
                }
            });
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (true) {
                if (newer.isDone()) newer.get();
                var description = admin.describeConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS).get(group);
                boolean onlyNew = description.members().size() == 1
                        && description.members().iterator().next().clientId().equals("new")
                        && description.members().iterator().next().assignment().topicPartitions().equals(Set.of(TP));
                var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
                var committed = offsets.get(TP);
                if (onlyNew && assignedNew.get() && observedNewPosition.get() == 1
                        && committed != null && committed.offset() == 1) break;
                require(System.nanoTime() < deadline, "New consumer takeover timeout");
                Thread.sleep(100);
            }
            System.out.println(group + " group: only new owns P0; committed=1");
            releaseOld.countDown();
            older.get(15, TimeUnit.SECONDS);
            stopNew.set(true);
            newer.get(15, TimeUnit.SECONDS);
            int expectedReads = commitBeforePause ? 0 : 1;
            require(newReads.get() == expectedReads, "Unexpected replay count");
            require(effects.get() == 1 + expectedReads, "Unexpected effect count");
            require(lateCommitRejected.get(), "Missing failed old commit");
            System.out.printf("PASS %s: newReads=%d effects=%d newPosition=1 committed=1 oldLateCommitFailed=true%n",
                    group, newReads.get(), effects.get());
        } finally {
            stopNew.set(true);
            releaseOld.countDown();
            pool.shutdownNow();
            require(pool.awaitTermination(15, TimeUnit.SECONDS), "Consumer threads failed to close");
        }
    }

    private static void checkIdentity(ConsumerRecord<String, String> r) {
        require(r.topic().equals(TOPIC) && r.partition() == 0 && r.offset() == 0
                && r.key().equals("event-1") && r.value().equals("effect-1"), "Unexpected record identity");
    }

    private static void require(boolean value, String message) {
        if (!value) throw new IllegalStateException(message);
    }
}
