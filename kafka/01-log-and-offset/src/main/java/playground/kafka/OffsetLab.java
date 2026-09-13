package playground.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** An actual Kafka broker and ordinary Java clients; no Spring application context. */
public final class OffsetLab implements AutoCloseable {
    private static final String TOPIC = "playground-offsets";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private final EmbeddedKafkaKraftBroker broker = new EmbeddedKafkaKraftBroker(1, 1, TOPIC);
    private Admin admin;

    public static void main(String[] args) throws Exception {
        if (args.length > 1 || (args.length == 1 && !args[0].equals("verify"))) {
            throw new IllegalArgumentException("Usage: OffsetLab [verify]");
        }
        try (var lab = new OffsetLab()) {
            lab.start();
            if (args.length == 1) lab.verify();
            else lab.shell();
        }
    }

    private void start() {
        broker.brokerProperties(Map.of(
                "offsets.topic.replication.factor", "1",
                "offsets.topic.num.partitions", "1",
                "group.initial.rebalance.delay.ms", "0",
                "auto.create.topics.enable", "false",
                "log.retention.hours", "24"));
        broker.afterPropertiesSet();
        admin = Admin.create(Map.of("bootstrap.servers", broker.getBrokersAsString()));
        System.out.printf("Kafka %s | 1 broker | 1 partition | temporary data%n", AppInfoParser.getVersion());
    }

    private void seed() throws Exception {
        if (endOffset() != 0) throw new IllegalStateException("Already seeded. Quit and restart for a fresh lab.");
        var props = new Properties();
        props.put("bootstrap.servers", broker.getBrokersAsString());
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        try (var producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
            for (var value : List.of("A", "B", "C")) {
                var metadata = producer.send(new ProducerRecord<>(TOPIC, 0, "post-42", value))
                        .get(15, TimeUnit.SECONDS);
                System.out.printf("stored value=%s partition=%d offset=%d%n", value, metadata.partition(), metadata.offset());
            }
        }
    }

    /** Each invocation creates a NEW consumer so group restart behavior is visible. */
    private Observation read(String group, int count, boolean commit, Long seekOffset) throws Exception {
        if (count < 1 || count > 100) throw new IllegalArgumentException("count must be 1..100");
        var props = new Properties();
        props.put("bootstrap.servers", broker.getBrokersAsString());
        props.put("group.id", group);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", "1");
        props.put("group.protocol", "classic");
        var offsets = new ArrayList<Long>();
        try (var consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            // Manual assignment intentionally isolates offsets from group rebalancing in lesson 1.
            consumer.assign(List.of(PARTITION));
            long endAtStart = endOffset();
            if (seekOffset != null) {
                if (seekOffset < 0 || seekOffset > endAtStart) {
                    throw new IllegalArgumentException("seek offset must be between 0 and " + endAtStart);
                }
                consumer.seek(PARTITION, seekOffset);
            }
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (offsets.size() < count && consumer.position(PARTITION) < endAtStart) {
                if (System.nanoTime() > deadline) throw new IllegalStateException("Timed out waiting for existing records");
                for (var record : consumer.poll(Duration.ofMillis(250))) {
                    // "Processing" in this lab is printing, not a database business transaction.
                    System.out.printf("read group=%s value=%s offset=%d%n", group, record.value(), record.offset());
                    offsets.add(record.offset());
                    if (commit) {
                        consumer.commitSync(Map.of(PARTITION, new OffsetAndMetadata(record.offset() + 1)));
                    }
                }
            }
            var metadata = consumer.committed(Set.of(PARTITION)).get(PARTITION);
            var observation = new Observation(List.copyOf(offsets), consumer.position(PARTITION),
                    metadata == null ? null : metadata.offset());
            System.out.printf("result group=%s read=%s position=%d committed=%s%n", group,
                    observation.offsets(), observation.position(), observation.committed());
            return observation;
        }
    }

    private long endOffset() throws Exception {
        return admin.listOffsets(Map.of(PARTITION, OffsetSpec.latest()))
                .all().get(15, TimeUnit.SECONDS).get(PARTITION).offset();
    }

    private void status(String group) throws Exception {
        var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                .get(15, TimeUnit.SECONDS);
        var committed = offsets.get(PARTITION);
        long start = admin.listOffsets(Map.of(PARTITION, OffsetSpec.earliest()))
                .all().get(15, TimeUnit.SECONDS).get(PARTITION).offset();
        long end = endOffset();
        System.out.printf("status group=%s start=%d end=%d committed=%s lag=%s%n", group, start, end,
                committed == null ? "none" : committed.offset(),
                committed == null ? "unknown" : end - committed.offset());
    }

    private void verify() throws Exception {
        seed();
        expect("read without commit", read("study", 2, false, null), List.of(0L, 1L), 2, null);
        expect("reopen and commit", read("study", 2, true, null), List.of(0L, 1L), 2, 2L);
        expect("resume even with earliest", read("study", 1, true, null), List.of(2L), 3, 3L);
        expect("caught up", read("study", 1, false, null), List.of(), 3, 3L);
        expect("independent group", read("audit", 3, true, null), List.of(0L, 1L, 2L), 3, 3L);
        status("study");
        expect("seek without changing commit", read("study", 3, false, 0L), List.of(0L, 1L, 2L), 3, 3L);
        expect("seek was only local", read("study", 1, false, null), List.of(), 3, 3L);
        long start = admin.listOffsets(Map.of(PARTITION, OffsetSpec.earliest()))
                .all().get(15, TimeUnit.SECONDS).get(PARTITION).offset();
        if (start != 0 || endOffset() != 3) throw new AssertionError("Unexpected log boundaries");
        System.out.println("PASS: 7 consumer scenarios; log boundaries remain [0, 3).");
    }

    private static void expect(String scenario, Observation actual, List<Long> offsets, long position, Long committed) {
        var expected = new Observation(offsets, position, committed);
        if (!expected.equals(actual)) throw new AssertionError(scenario + ": expected=" + expected + ", actual=" + actual);
        System.out.println("PASS: " + scenario);
    }

    private void shell() throws Exception {
        help();
        try (var input = new Scanner(System.in)) {
            while (true) {
                System.out.print("lab> ");
                if (!input.hasNextLine()) break;
                var words = input.nextLine().trim().split("\\s+");
                try {
                    switch (words[0]) {
                        case "seed" -> { requireArgs(words, 1); seed(); }
                        case "read" -> {
                            requireArgs(words, 4);
                            if (!Set.of("commit", "no-commit").contains(words[3])) {
                                throw new IllegalArgumentException("Use commit or no-commit");
                            }
                            read(words[1], Integer.parseInt(words[2]), words[3].equals("commit"), null);
                        }
                        case "seek" -> {
                            requireArgs(words, 4);
                            read(words[1], Integer.parseInt(words[3]), false, Long.parseLong(words[2]));
                        }
                        case "status" -> { requireArgs(words, 2); status(words[1]); }
                        case "help" -> help();
                        case "quit", "exit" -> { return; }
                        case "" -> { }
                        default -> System.out.println("Unknown command. Type help.");
                    }
                } catch (IllegalArgumentException | IllegalStateException e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    private static void requireArgs(String[] words, int length) {
        if (words.length != length) throw new IllegalArgumentException("Wrong arguments. Type help.");
    }

    private static void help() {
        System.out.println("""
                seed                           Store A, B, C once
                read GROUP COUNT no-commit     Read with a new consumer, do not commit
                read GROUP COUNT commit        Read with a new consumer, commit offset + 1
                status GROUP                   Inspect log boundaries and group commit
                seek GROUP OFFSET COUNT        Replay from OFFSET; NEVER commit
                quit                           Stop broker and discard this session's data
                Every read/seek creates and closes a consumer. Broker lives until quit.
                """);
    }

    @Override public void close() {
        if (admin != null) admin.close(Duration.ofSeconds(5));
        broker.destroy();
    }

    private record Observation(List<Long> offsets, long position, Long committed) { }
}
