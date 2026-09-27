package playground.kafka;

import org.apache.kafka.clients.admin.Admin;
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

/** Controlled commit gap versus abort/retry. No process kill, DB or external API. */
public final class OffsetsInTransactionLab {
    private static final String INPUT = "orders";
    private static final String SEPARATE = "separate-output";
    private static final String ATOMIC = "atomic-output";
    private static final TopicPartition IN = new TopicPartition(INPUT, 0);
    private static final Duration API = Duration.ofSeconds(10);
    private record Row(long offset, String key, String value) {}

    public static void main(String[] args) throws Exception {
        if (args.length > 1 || (args.length == 1 && !Set.of("guided", "verify").contains(args[0])))
            throw new IllegalArgumentException("Usage: OffsetsInTransactionLab [guided|verify]");
        boolean guided = args.length == 0 || args[0].equals("guided");
        try (var input = new Scanner(System.in)) {
            System.out.println("Kafka " + AppInfoParser.getVersion() + " | classic dynamic groups | input P0 offset 0");
            System.out.println("Controlled close/reopen and explicit abort; no crash injection, DB or external API.");
            if (guided && !next(input, "출력 전송 뒤 입력 오프셋을 커밋하지 않고 Consumer를 다시 열면 출력은 몇 건일까요?")) return;
            var broker = new EmbeddedKafkaKraftBroker(1, 1, INPUT, SEPARATE, ATOMIC);
            broker.brokerProperties(Map.of("offsets.topic.replication.factor", "1",
                    "offsets.topic.num.partitions", "1", "group.initial.rebalance.delay.ms", "0",
                    "transaction.state.log.replication.factor", "1", "transaction.state.log.min.isr", "1",
                    "transaction.state.log.num.partitions", "1"));
            try {
                broker.afterPropertiesSet();
                String bootstrap = broker.getBrokersAsString();
                try (var admin = Admin.create(Map.of("bootstrap.servers", bootstrap, "default.api.timeout.ms", "10000", "request.timeout.ms", "5000"));
                     var plain = producer(bootstrap, false)) {
                    Row seed = send(plain, INPUT, "order-1", "created");
                    require(seed.offset == 0, "Unexpected seed offset");
                    separate(bootstrap, admin, plain);
                    if (guided && !next(input, "출력과 입력 오프셋을 같은 트랜잭션으로 보내고 abort한 뒤 재처리하면 무엇이 남을까요?")) return;
                    atomic(bootstrap, admin);
                    System.out.println("DONE: separate commit gap versus Kafka offset/output transaction verified; business code ran twice in both cases.");
                }
            } finally { broker.destroy(); }
        }
    }

    private static void separate(String bootstrap, Admin admin, KafkaProducer<String, String> p) throws Exception {
        String group = "separate-group";
        var expected = new ArrayList<Row>();
        int attempts = 0;
        try (var c = inputConsumer(bootstrap, group)) {
            var batch = readInput(c);
            attempts++;
            expected.add(send(p, SEPARATE, "order-1", "processed"));
            require(committed(admin, group) == null, "Unexpected offset commit before gap");
            require(snapshot(bootstrap, SEPARATE, "read_committed", 1).equals(expected), "First output missing");
            System.out.printf("PASS separate-gap: position=%d inputCommitted=null outputRC=1 attempts=%d%n", c.position(IN), attempts);
            // Close without commit. Auto commit is disabled; restart uses the same group.
        }
        try (var c = inputConsumer(bootstrap, group)) {
            var batch = readInput(c); // Assert topic/partition/offset/key/value, not only count.
            attempts++;
            expected.add(send(p, SEPARATE, "order-1", "processed"));
            c.commitSync(batch.nextOffsets(), API);
        }
        require(Objects.equals(committed(admin, group), 1L), "Separate final offset");
        var output = snapshot(bootstrap, SEPARATE, "read_committed", 2);
        require(output.equals(expected) && attempts == 2, "Separate duplicate identity/order");
        System.out.printf("PASS separate-retry: inputCommitted=1 outputRC=%d attempts=%d%n", output.size(), attempts);
    }

    private static void atomic(String bootstrap, Admin admin) throws Exception {
        String group = "atomic-group";
        int attempts = 0;
        Row aborted;
        Row accepted;
        try (var tx = producer(bootstrap, true)) {
            tx.initTransactions();
            try (var c = inputConsumer(bootstrap, group)) {
                var batch = readInput(c);
                attempts++;
                tx.beginTransaction();
                aborted = send(tx, ATOMIC, "order-1", "processed");
                tx.sendOffsetsToTransaction(batch.nextOffsets(), c.groupMetadata());
                tx.abortTransaction();
                require(Objects.equals(c.position(IN), 1L), "Abort should not rewind consumer position");
            }
            require(committed(admin, group) == null, "Aborted input offset became committed");
            var abortedRu = snapshot(bootstrap, ATOMIC, "read_uncommitted", aborted.offset + 1);
            var abortedRc = snapshot(bootstrap, ATOMIC, "read_committed", aborted.offset + 1);
            require(abortedRu.equals(List.of(aborted)) && abortedRc.isEmpty(), "Abort visibility mismatch");
            System.out.printf("PASS atomic-abort: inputCommitted=null outputRC=0 outputRU=1 attempts=%d%n", attempts);
            try (var c = inputConsumer(bootstrap, group)) {
                var batch = readInput(c);
                attempts++;
                tx.beginTransaction();
                accepted = send(tx, ATOMIC, "order-1", "processed");
                tx.sendOffsetsToTransaction(batch.nextOffsets(), c.groupMetadata());
                tx.commitTransaction();
                // No consumer.commitSync/commitAsync in this transactional path.
            }
        }
        require(Objects.equals(committed(admin, group), 1L), "Transactional final offset");
        var outputRc = snapshot(bootstrap, ATOMIC, "read_committed", accepted.offset + 1);
        var outputRu = snapshot(bootstrap, ATOMIC, "read_uncommitted", accepted.offset + 1);
        require(outputRc.equals(List.of(accepted)), "Exactly one committed output expected in this scenario");
        require(outputRu.equals(List.of(aborted, accepted)) && attempts == 2, "Aborted attempt missing from raw view");
        System.out.printf("PASS atomic-commit: inputCommitted=1 outputRC=%d outputRU=%d attempts=%d%n", outputRc.size(), outputRu.size(), attempts);
        try (var c = inputConsumer(bootstrap, group)) {
            long deadline = deadline();
            do {
                require(c.poll(Duration.ofMillis(200)).isEmpty(), "Committed input unexpectedly replayed");
                if (c.assignment().contains(IN) && c.position(IN, API) == 1) {
                    require(c.endOffsets(Set.of(IN), API).get(IN) == 1, "Unexpected input end");
                    System.out.println("PASS atomic-restart: position=1 inputEnd=1 replayed=0");
                    return;
                }
            } while (System.nanoTime() < deadline);
            throw new IllegalStateException("Restart did not resume from committed offset");
        }
    }

    private static KafkaProducer<String, String> producer(String bootstrap, boolean transactional) {
        var props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        props.put("max.block.ms", "15000");
        props.put("request.timeout.ms", "10000");
        props.put("delivery.timeout.ms", "20000");
        if (transactional) {
            props.put("transactional.id", "offset-output-lab");
            props.put("transaction.timeout.ms", "60000");
        }
        return new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }

    private static KafkaConsumer<String, String> inputConsumer(String bootstrap, String group) {
        var props = consumerProps(bootstrap, "read_committed");
        props.put("group.id", group);
        props.put("group.protocol", "classic");
        props.put("partition.assignment.strategy", RangeAssignor.class.getName());
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", "1");
        var c = new KafkaConsumer<String, String>(props, new StringDeserializer(), new StringDeserializer());
        c.subscribe(List.of(INPUT));
        return c;
    }

    private static Properties consumerProps(String bootstrap, String isolation) {
        var p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("enable.auto.commit", "false");
        p.put("isolation.level", isolation);
        p.put("default.api.timeout.ms", "10000");
        return p;
    }

    private static ConsumerRecords<String, String> readInput(KafkaConsumer<String, String> c) {
        long deadline = deadline();
        do {
            var batch = c.poll(Duration.ofMillis(200));
            if (!batch.isEmpty()) {
                require(batch.count() == 1, "Input count");
                var r = batch.iterator().next();
                require(r.topic().equals(INPUT) && r.partition() == 0 && r.offset() == 0
                        && r.key().equals("order-1") && r.value().equals("created"), "Input identity");
                require(batch.nextOffsets().get(IN).offset() == 1, "Next input offset");
                require(c.groupMetadata().generationId() >= 0, "Real group membership required");
                return batch;
            }
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Input poll timeout");
    }

    private static Long committed(Admin admin, String group) throws Exception {
        var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(15, TimeUnit.SECONDS);
        return offsets.get(IN) == null ? null : offsets.get(IN).offset();
    }

    private static Row send(KafkaProducer<String, String> p, String topic, String key, String value) throws Exception {
        var m = p.send(new ProducerRecord<>(topic, 0, key, value)).get(25, TimeUnit.SECONDS);
        require(m.topic().equals(topic) && m.partition() == 0, "Wrong output destination");
        return new Row(m.offset(), key, value);
    }

    private static List<Row> snapshot(String bootstrap, String topic, String isolation, long minimum) {
        var tp = new TopicPartition(topic, 0);
        try (var c = new KafkaConsumer<String, String>(consumerProps(bootstrap, isolation), new StringDeserializer(), new StringDeserializer())) {
            c.assign(List.of(tp));
            c.seek(tp, 0);
            long deadline = deadline();
            long boundary;
            do {
                boundary = c.endOffsets(Set.of(tp), API).get(tp);
                if (boundary >= minimum) break;
                c.poll(Duration.ofMillis(100));
                require(System.nanoTime() < deadline, "Output boundary timeout");
            } while (true);
            c.seek(tp, 0);
            var rows = new ArrayList<Row>();
            do {
                for (var r : c.poll(Duration.ofMillis(200))) {
                    require(r.topic().equals(topic) && r.partition() == 0, "Wrong output identity");
                    rows.add(new Row(r.offset(), r.key(), r.value()));
                }
                if (c.position(tp, API) >= boundary) return rows;
            } while (System.nanoTime() < deadline);
            throw new IllegalStateException("Output snapshot timeout");
        }
    }

    private static long deadline() { return System.nanoTime() + TimeUnit.SECONDS.toNanos(30); }
    private static boolean next(Scanner input, String question) {
        System.out.println(question + " [Enter: 진행 / q: 종료]");
        return input.hasNextLine() && !input.nextLine().trim().equalsIgnoreCase("q");
    }
    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
