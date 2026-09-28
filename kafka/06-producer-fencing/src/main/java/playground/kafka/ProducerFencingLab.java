package playground.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Two live producer objects, ordered API calls. No process kill or production access. */
public final class ProducerFencingLab {
    private static final String SHARED = "shared-id-output";
    private static final String DISTINCT = "distinct-id-output";
    private static final Duration API = Duration.ofSeconds(10);
    private record Row(long offset, String key, String value) {}

    public static void main(String[] args) throws Exception {
        if (args.length > 1 || (args.length == 1 && !Set.of("guided", "verify").contains(args[0])))
            throw new IllegalArgumentException("Usage: ProducerFencingLab [guided|verify]");
        boolean guided = args.length == 0 || args[0].equals("guided");
        try (var input = new Scanner(System.in)) {
            System.out.println("Kafka " + AppInfoParser.getVersion() + " | two live producers | no process kill");
            if (guided && !next(input, "같은 transactional.id로 두 번째 Producer가 초기화되면 첫 번째의 commit은 성공할까요?")) return;
            var broker = new EmbeddedKafkaKraftBroker(1, 1, SHARED, DISTINCT);
            broker.brokerProperties(Map.of("offsets.topic.replication.factor", "1",
                    "offsets.topic.num.partitions", "1", "transaction.state.log.replication.factor", "1",
                    "transaction.state.log.min.isr", "1", "transaction.state.log.num.partitions", "1"));
            try {
                broker.afterPropertiesSet();
                String bootstrap = broker.getBrokersAsString();
                sharedId(bootstrap);
                if (guided && !next(input, "서로 다른 transactional.id이지만 메시지 key/value가 같다면 두 commit은 성공할까요?")) return;
                distinctIds(bootstrap);
                System.out.println("DONE: same-ID takeover fenced the old producer; distinct IDs committed two copies.");
            } finally { broker.destroy(); }
        }
    }

    private static void sharedId(String bootstrap) throws Exception {
        try (var old = producer(bootstrap, "logical-worker", "old-client");
             var replacement = producer(bootstrap, "logical-worker", "new-client")) {
            old.initTransactions();
            old.beginTransaction();
            Row pending = send(old, SHARED);
            require(snapshot(bootstrap, SHARED, "read_uncommitted", pending.offset + 1).equals(List.of(pending)), "Pending record missing");
            System.out.println("PASS shared-before: RU=1 oldSendAcknowledged=true");

            // The old producer is still alive and its transaction is unfinished.
            replacement.initTransactions();
            require(snapshot(bootstrap, SHARED, "read_committed", pending.offset + 1).isEmpty(), "Old transaction became visible");
            boolean fenced = false;
            try {
                old.commitTransaction();
            } catch (ProducerFencedException expected) {
                fenced = true;
            }
            require(fenced, "Old commit unexpectedly succeeded");
            System.out.println("PASS shared-takeover: oldCommit=ProducerFencedException RC=0");
            // No abort/retry on the fenced instance. Close it at scope exit.
            replacement.beginTransaction();
            Row accepted = send(replacement, SHARED);
            replacement.commitTransaction();
            var rc = snapshot(bootstrap, SHARED, "read_committed", accepted.offset + 1);
            var ru = snapshot(bootstrap, SHARED, "read_uncommitted", accepted.offset + 1);
            require(rc.equals(List.of(accepted)), "Wrong committed owner/record");
            require(ru.equals(List.of(pending, accepted)), "Old aborted record or new record missing");
            System.out.printf("PASS shared-final: RC=%d RU=%d sameBusinessKey=true%n", rc.size(), ru.size());
        }
    }

    private static void distinctIds(String bootstrap) throws Exception {
        // Same client.id, topic, key and value; only transactional identity differs.
        try (var first = producer(bootstrap, "worker-a", "same-client");
             var second = producer(bootstrap, "worker-b", "same-client")) {
            first.initTransactions();
            first.beginTransaction();
            Row a = send(first, DISTINCT);
            second.initTransactions();
            second.beginTransaction();
            Row b = send(second, DISTINCT);
            second.commitTransaction();
            first.commitTransaction();
            var expected = List.of(a, b);
            require(a.offset < b.offset && a.key.equals(b.key) && a.value.equals(b.value), "Control identities");
            var rc = snapshot(bootstrap, DISTINCT, "read_committed", b.offset + 1);
            var ru = snapshot(bootstrap, DISTINCT, "read_uncommitted", b.offset + 1);
            require(rc.equals(expected) && ru.equals(expected), "Distinct producer output mismatch");
            System.out.printf("PASS distinct-final: firstCommit=success secondCommit=success RC=%d RU=%d sameClientId=true%n", rc.size(), ru.size());
        }
    }

    private static KafkaProducer<String, String> producer(String bootstrap, String txId, String clientId) {
        var props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("transactional.id", txId);
        props.put("client.id", clientId);
        props.put("enable.idempotence", "true");
        props.put("acks", "all");
        props.put("max.block.ms", "15000");
        props.put("request.timeout.ms", "10000");
        props.put("delivery.timeout.ms", "20000");
        props.put("transaction.timeout.ms", "120000");
        return new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }

    private static Row send(KafkaProducer<String, String> p, String topic) throws Exception {
        var m = p.send(new ProducerRecord<>(topic, 0, "order-1", "processed")).get(25, TimeUnit.SECONDS);
        require(m.topic().equals(topic) && m.partition() == 0, "Unexpected destination");
        return new Row(m.offset(), "order-1", "processed");
    }

    private static List<Row> snapshot(String bootstrap, String topic, String isolation, long minimum) {
        var tp = new TopicPartition(topic, 0);
        var props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("enable.auto.commit", "false");
        props.put("isolation.level", isolation);
        props.put("default.api.timeout.ms", "10000");
        try (var c = new KafkaConsumer<String, String>(props, new StringDeserializer(), new StringDeserializer())) {
            c.assign(List.of(tp));
            c.seek(tp, 0);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
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
                    require(r.topic().equals(topic) && r.partition() == 0, "Unexpected record identity");
                    rows.add(new Row(r.offset(), r.key(), r.value()));
                }
                if (c.position(tp, API) >= boundary) return rows;
            } while (System.nanoTime() < deadline);
            throw new IllegalStateException("Output snapshot timeout");
        }
    }

    private static boolean next(Scanner input, String prompt) {
        System.out.println(prompt + " [Enter: 진행 / q: 종료]");
        return input.hasNextLine() && !input.nextLine().trim().equalsIgnoreCase("q");
    }
    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
