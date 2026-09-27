package playground.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Real Kafka transaction visibility; externalEffects is only an in-memory model. */
public final class TransactionVisibilityLab {
    private static final String TOPIC = "transaction-boundary";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);
    private static final Duration API = Duration.ofSeconds(10);
    private record Row(long offset, String key, String value) {}

    public static void main(String[] args) throws Exception {
        if (args.length > 1 || (args.length == 1 && !Set.of("guided", "verify").contains(args[0])))
            throw new IllegalArgumentException("Usage: TransactionVisibilityLab [guided|verify]");
        boolean guided = args.length == 0 || args[0].equals("guided");
        try (var input = new Scanner(System.in)) {
            System.out.println("Kafka " + AppInfoParser.getVersion() + " | one local partition | manual assignment, no offset commits");
            System.out.println("Effects are an in-memory model. No DB, HTTP call, crash injection or performance benchmark.");
            if (guided && !next(input, "열린 트랜잭션 뒤에 일반 메시지를 쓰면 read_committed에서 먼저 보일까요? abort하면 무엇이 남을까요?")) return;
            var broker = new EmbeddedKafkaKraftBroker(1, 1, TOPIC);
            broker.brokerProperties(Map.of(
                    "offsets.topic.replication.factor", "1", "offsets.topic.num.partitions", "1",
                    "transaction.state.log.replication.factor", "1",
                    "transaction.state.log.min.isr", "1", "transaction.state.log.num.partitions", "1"));
            try {
                broker.afterPropertiesSet();
                run(broker.getBrokersAsString(), guided, input);
            } finally { broker.destroy(); }
        }
    }

    private static void run(String bootstrap, boolean guided, Scanner input) throws Exception {
        try (var tx = producer(bootstrap, true);
             var plain = producer(bootstrap, false);
             var ru = consumer(bootstrap, "read_uncommitted");
             var rc = consumer(bootstrap, "read_committed")) {
            tx.initTransactions();
            int externalEffects = 0;
            tx.beginTransaction();
            Row aborted = send(tx, "event-abort", "tentative");
            externalEffects++; // A Java value, not a database transaction or a payment.
            Row marker = send(plain, "marker", "plain");
            require(aborted.offset == 0 && marker.offset == 1, "Unexpected initial offsets");
            long openRuEnd = awaitEnd(ru, marker.offset + 1);
            long openRcEnd = rc.endOffsets(Set.of(TP), API).get(TP);
            require(openRcEnd == aborted.offset, "Open transaction should hold read_committed boundary");
            var openRu = snapshot(ru, openRuEnd);
            var openRc = snapshot(rc, openRcEnd);
            require(openRu.equals(List.of(aborted, marker)), "Open read_uncommitted identity mismatch");
            require(openRc.isEmpty() && rc.position(TP, API) == 0, "Open transaction leaked to read_committed");
            System.out.printf("PASS open: RU offsets=%s end=%d; RC offsets=%s end=%d; externalEffects=%d%n",
                    offsets(openRu), openRuEnd, offsets(openRc), openRcEnd, externalEffects);

            tx.abortTransaction();
            // Wait for the read boundary to include the later plain record, not a fixed sleep.
            long abortRcEnd = awaitEnd(rc, marker.offset + 1);
            long abortRuEnd = awaitEnd(ru, abortRcEnd);
            var abortRc = snapshot(rc, abortRcEnd);
            var abortRu = snapshot(ru, abortRuEnd);
            require(abortRc.equals(List.of(marker)), "Aborted row must be hidden; plain row must survive");
            require(abortRu.equals(List.of(aborted, marker)), "Abort is not physical removal from log");
            require(externalEffects == 1, "Java side effect unexpectedly changed");
            System.out.printf("PASS abort: RU offsets=%s; RC offsets=%s; externalEffects=%d%n",
                    offsets(abortRu), offsets(abortRc), externalEffects);

            // Prompt only with no open transaction: user thinking time must not expire a transaction.
            if (guided && !next(input, "멱등 Producer에서 같은 key/value로 send를 두 번 호출하면 한 건으로 합쳐질까요?")) return;
            tx.beginTransaction();
            Row first = send(tx, "same-event", "same-value");
            Row second = send(tx, "same-event", "same-value");
            externalEffects++;
            tx.commitTransaction();
            long commitRcEnd = awaitEnd(rc, second.offset + 1);
            var committed = snapshot(rc, commitRcEnd);
            long commitRuEnd = awaitEnd(ru, commitRcEnd);
            var all = snapshot(ru, commitRuEnd);
            require(first.offset < second.offset, "Separate sends must have distinct offsets");
            require(committed.equals(List.of(marker, first, second)), "Committed snapshot identity/order mismatch");
            require(all.equals(List.of(aborted, marker, first, second)), "Uncommitted snapshot mismatch");
            long copies = committed.stream().filter(r -> r.key.equals("same-event") && r.value.equals("same-value")).count();
            require(copies == 2 && externalEffects == 2, "Application-level duplicate comparison failed");
            System.out.printf("PASS commit: RU offsets=%s; RC offsets=%s; sameEventCopies=%d; externalEffects=%d%n",
                    offsets(all), offsets(committed), copies, externalEffects);
            System.out.println("DONE: open/abort/commit visibility and two explicit sends verified. Consumer group offset transactions are not tested.");
        }
    }

    private static KafkaProducer<String, String> producer(String bootstrap, boolean transactional) {
        var props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        props.put("max.block.ms", "10000");
        props.put("request.timeout.ms", "10000");
        props.put("delivery.timeout.ms", "20000");
        if (transactional) {
            props.put("transactional.id", "visibility-lab");
            props.put("transaction.timeout.ms", "60000");
        }
        return new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }

    private static KafkaConsumer<String, String> consumer(String bootstrap, String isolation) {
        var props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("enable.auto.commit", "false");
        props.put("isolation.level", isolation);
        props.put("default.api.timeout.ms", "10000");
        var c = new KafkaConsumer<String, String>(props, new StringDeserializer(), new StringDeserializer());
        c.assign(List.of(TP));
        c.seek(TP, 0);
        return c;
    }

    private static Row send(KafkaProducer<String, String> p, String key, String value) throws Exception {
        var m = p.send(new ProducerRecord<>(TOPIC, 0, key, value)).get(25, TimeUnit.SECONDS);
        require(m.topic().equals(TOPIC) && m.partition() == 0, "Wrong send destination");
        return new Row(m.offset(), key, value);
    }

    private static long awaitEnd(KafkaConsumer<String, String> c, long minimum) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        do {
            long end = c.endOffsets(Set.of(TP), API).get(TP);
            if (end >= minimum) return end;
            // poll is bounded and does not replace the endOffsets assertion.
            c.poll(Duration.ofMillis(100));
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Read boundary did not advance to " + minimum);
    }

    private static List<Row> snapshot(KafkaConsumer<String, String> c, long boundary) {
        c.seek(TP, 0);
        var rows = new ArrayList<Row>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        do {
            for (var r : c.poll(Duration.ofMillis(500))) {
                require(r.topic().equals(TOPIC) && r.partition() == 0, "Wrong read destination");
                rows.add(new Row(r.offset(), r.key(), r.value()));
            }
            if (c.position(TP, API) >= boundary) return rows;
        } while (System.nanoTime() < deadline);
        throw new IllegalStateException("Snapshot did not reach boundary " + boundary);
    }

    private static List<Long> offsets(List<Row> rows) { return rows.stream().map(Row::offset).toList(); }
    private static boolean next(Scanner input, String question) {
        System.out.println(question + " [Enter: 진행 / q: 종료]");
        return input.hasNextLine() && !input.nextLine().trim().equalsIgnoreCase("q");
    }
    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
