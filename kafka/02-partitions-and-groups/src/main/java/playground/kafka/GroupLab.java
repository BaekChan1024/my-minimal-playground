package playground.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Utils;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/** One real broker; each long-lived KafkaConsumer belongs to exactly one thread. */
public final class GroupLab implements AutoCloseable {
    private static final String TOPIC = "playground-groups";
    private static final int PARTITIONS = 3;
    private final EmbeddedKafkaKraftBroker broker = new EmbeddedKafkaKraftBroker(1, PARTITIONS, TOPIC);
    private final List<Worker> workers = new ArrayList<>();
    private final List<Item> sent = new ArrayList<>();
    private final Queue<Receipt> received = new ConcurrentLinkedQueue<>();
    private final Map<Integer, String> keys = new TreeMap<>();
    private Admin admin;
    private KafkaProducer<String, String> producer;

    public static void main(String[] args) throws Exception {
        if (args.length > 1 || (args.length == 1 && !Set.of("guided", "verify").contains(args[0]))) {
            throw new IllegalArgumentException("Usage: GroupLab [guided|verify]");
        }
        try (var lab = new GroupLab(); var input = new Scanner(System.in)) {
            lab.start();
            lab.run(args.length == 0 || args[0].equals("guided"), input);
        }
    }

    private void start() {
        broker.brokerProperties(Map.of(
                "offsets.topic.replication.factor", "1", "offsets.topic.num.partitions", "1",
                "group.initial.rebalance.delay.ms", "0", "auto.create.topics.enable", "false"));
        broker.afterPropertiesSet();
        admin = Admin.create(Map.of("bootstrap.servers", broker.getBrokersAsString()));
        var props = new Properties();
        props.put("bootstrap.servers", broker.getBrokersAsString());
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        // Deliberately choose one synthetic key for each partition; real traffic need not be balanced.
        for (int i = 0; keys.size() < PARTITIONS; i++) {
            String key = "order-" + i;
            int partition = Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % PARTITIONS;
            keys.putIfAbsent(partition, key);
        }
        System.out.printf("Kafka %s | partitions=3 | classic + RangeAssignor | temporary broker%n", AppInfoParser.getVersion());
        System.out.println("Synthetic key map (default Java producer, UTF-8, fixed partition count): " + keys);
    }

    private void run(boolean guided, Scanner input) throws Exception {
        for (int count = 1; count <= 4; count++) {
            if (!next(guided, input, count + "/7 Consumer " + count + "개",
                    "파티션 3개를 어떻게 나눌까요? Consumer가 4개면 모두 읽을까요?")) return;
            add("study", "study-" + count);
            Map<String, List<Integer>> assignment = awaitStable("study", count);
            List<Integer> sizes = assignment.values().stream().map(List::size).sorted().toList();
            List<Integer> expected = switch (count) {
                case 1 -> List.of(3);
                case 2 -> List.of(1, 2);
                case 3 -> List.of(1, 1, 1);
                default -> List.of(0, 1, 1, 1);
            };
            require(sizes.equals(expected), "Unexpected partition distribution: " + sizes);
            int phase = count;
            sendPhase(phase, false);
            awaitDrained("study");
            showCounts("study", phase);
            verifyOwnership("study", phase, assignment);
            pass("members=" + count + " partition-counts=" + sizes);
        }
        if (!next(guided, input, "5/7 다른 그룹 audit", "study가 커밋한 36개를 audit은 읽을 수 있을까요?")) return;
        add("audit", "audit-1");
        awaitStable("audit", 1);
        awaitDrained("audit");
        pass("independent group audit replayed " + sent.size() + " records");

        if (!next(guided, input, "6/7 담당 Consumer 정상 종료", "할당이 있던 Consumer가 떠나면 누가 이어받을까요?")) return;
        Map<String, List<Integer>> before = awaitStable("study", 4);
        String departing = before.entrySet().stream().filter(e -> !e.getValue().isEmpty()).findFirst().orElseThrow().getKey();
        System.out.println("Leaving gracefully: " + departing + " partitions=" + before.get(departing));
        workers.stream().filter(w -> w.name.equals(departing)).findFirst().orElseThrow().stop();
        Map<String, List<Integer>> after = awaitStable("study", 3);
        require(!after.containsKey(departing), "Departed member still present");
        sendPhase(5, false);
        awaitDrained("study");
        awaitDrained("audit");
        showCounts("study", 5);
        verifyOwnership("study", 5, after);
        pass("graceful leave: remaining 3 members cover all partitions and resume");

        if (!next(guided, input, "7/7 같은 키에 데이터 집중", "Consumer 3개에 같은 키 9개를 보내면 일이 고르게 나뉠까요?")) return;
        sendPhase(6, true);
        awaitDrained("study");
        awaitDrained("audit");
        showCounts("study", 6);
        long active = receipts("study").stream().filter(r -> r.item.phase == 6).map(Receipt::worker).distinct().count();
        require(active == 1, "A fixed hot key must stay with one owner in this stable phase");
        verifyAll("study");
        verifyAll("audit");
        pass("hot key: 9 records handled by one member");
        pass("both groups received all 54 unique records; per-key order and committed offsets verified");
        System.out.println("DONE: 7 steps verified. This is an assignment experiment, not a speed benchmark or crash test.");
    }

    private static boolean next(boolean guided, Scanner input, String title, String question) {
        System.out.printf("%n%s%n", title);
        if (!guided) return true;
        System.out.printf("예상: %s%nEnter 실행 / q 종료: ", question);
        return input.hasNextLine() && !input.nextLine().trim().equalsIgnoreCase("q");
    }

    private void add(String group, String name) {
        var worker = new Worker(group, name);
        workers.add(worker);
        worker.thread.start();
    }

    private Map<String, List<Integer>> awaitStable(String group, int count) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
        Map<String, List<Integer>> previous = Map.of();
        int confirmations = 0;
        while (System.nanoTime() < deadline) {
            checkWorkers();
            var description = admin.describeConsumerGroups(List.of(group)).all().get(5, TimeUnit.SECONDS).get(group);
            Map<String, List<Integer>> snapshot = new TreeMap<>();
            for (var member : description.members()) {
                snapshot.put(member.clientId(), member.assignment().topicPartitions().stream()
                        .filter(tp -> tp.topic().equals(TOPIC)).map(TopicPartition::partition).sorted().toList());
            }
            List<Integer> union = snapshot.values().stream().flatMap(Collection::stream).sorted().toList();
            boolean localMatch = workers.stream().filter(w -> w.group.equals(group) && w.running)
                    .allMatch(w -> w.assignment.equals(snapshot.get(w.name)));
            if (description.state().toString().equalsIgnoreCase("Stable") && snapshot.size() == count
                    && union.equals(List.of(0, 1, 2)) && localMatch) {
                confirmations = snapshot.equals(previous) ? confirmations + 1 : 1;
                if (confirmations >= 3) {
                    System.out.println("assignment group=" + group + " " + snapshot);
                    return snapshot;
                }
            } else confirmations = 0;
            previous = snapshot;
            Thread.sleep(150);
        }
        throw new IllegalStateException("Timed out stabilizing group " + group);
    }

    private void sendPhase(int phase, boolean hot) throws Exception {
        for (int i = 0; i < 9; i++) {
            int expectedPartition = hot ? 0 : i % PARTITIONS;
            String key = keys.get(expectedPartition);
            String value = "phase-" + phase + "-item-" + i;
            // No explicit partition: the real producer routes by serialized key.
            var metadata = producer.send(new ProducerRecord<>(TOPIC, key, value)).get(15, TimeUnit.SECONDS);
            require(metadata.partition() == expectedPartition, "Producer key routing mismatch");
            sent.add(new Item(phase, key, value, metadata.partition(), metadata.offset()));
        }
        System.out.println("sent phase=" + phase + " records=9 mode=" + (hot ? "one-key" : "three-keys"));
    }

    private void awaitDrained(String group) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            checkWorkers();
            if (receipts(group).size() >= sent.size()) {
                verifyAll(group);
                return;
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("Timed out reading group=" + group + " count=" + receipts(group).size());
    }

    private void verifyAll(String group) throws Exception {
        List<Receipt> actual = receipts(group);
        require(actual.size() == sent.size(), "Unexpected duplicate/missing record in " + group);
        require(new HashSet<>(actual.stream().map(Receipt::item).toList()).equals(new HashSet<>(sent)), "Record identity mismatch");
        for (String key : keys.values()) {
            List<Item> expectedOrder = sent.stream().filter(item -> item.key.equals(key)).toList();
            List<Item> actualOrder = actual.stream().map(Receipt::item).filter(item -> item.key.equals(key)).toList();
            require(actualOrder.equals(expectedOrder), "Per-key order mismatch: " + key);
        }
        var commits = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
        for (int partition = 0; partition < PARTITIONS; partition++) {
            int p = partition;
            long expected = sent.stream().filter(item -> item.partition == p).mapToLong(Item::offset).max().orElse(-1) + 1;
            var committed = commits.get(new TopicPartition(TOPIC, partition));
            require(committed != null && committed.offset() == expected, "Committed offset mismatch: " + group + "/" + partition);
        }
    }

    private void showCounts(String group, int phase) {
        Map<String, Long> counts = new TreeMap<>();
        workers.stream().filter(w -> w.group.equals(group) && w.running).forEach(w -> counts.put(w.name, 0L));
        receipts(group).stream().filter(r -> r.item.phase == phase).forEach(r -> counts.merge(r.worker, 1L, Long::sum));
        System.out.println("processed group=" + group + " phase=" + phase + " " + counts);
    }

    private void verifyOwnership(String group, int phase, Map<String, List<Integer>> assignment) {
        for (var receipt : receipts(group)) {
            if (receipt.item.phase == phase) require(assignment.get(receipt.worker).contains(receipt.item.partition), "Wrong owner");
        }
    }

    private List<Receipt> receipts(String group) {
        return received.stream().filter(receipt -> receipt.group.equals(group)).toList();
    }

    private void checkWorkers() {
        for (var worker : workers) if (worker.failure != null) throw new IllegalStateException("Consumer failed: " + worker.name, worker.failure);
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
    private static void pass(String message) { System.out.println("PASS: " + message); }

    private final class Worker {
        final String group;
        final String name;
        final Thread thread;
        volatile boolean running = true;
        volatile List<Integer> assignment = List.of();
        volatile Throwable failure;
        volatile KafkaConsumer<String, String> consumer;

        Worker(String group, String name) {
            this.group = group;
            this.name = name;
            this.thread = new Thread(this::consume, name);
        }

        void consume() {
            var props = new Properties();
            props.put("bootstrap.servers", broker.getBrokersAsString());
            props.put("group.id", group);
            props.put("client.id", name);
            props.put("group.protocol", "classic");
            props.put("partition.assignment.strategy", RangeAssignor.class.getName());
            props.put("enable.auto.commit", "false");
            props.put("auto.offset.reset", "earliest");
            props.put("max.poll.records", "3");
            props.put("session.timeout.ms", "10000");
            props.put("heartbeat.interval.ms", "1000");
            try (var c = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
                consumer = c;
                c.subscribe(List.of(TOPIC), new ConsumerRebalanceListener() {
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        assignment = List.of();
                        System.out.println("revoked " + name + " " + partitions.stream().map(TopicPartition::partition).sorted().toList());
                    }
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        assignment = partitions.stream().map(TopicPartition::partition).sorted().toList();
                        System.out.println("assigned " + name + " " + assignment);
                    }
                });
                while (running) {
                    var records = c.poll(Duration.ofMillis(200));
                    if (records.isEmpty()) continue;
                    List<Receipt> batch = new ArrayList<>();
                    for (var record : records) {
                        int phase = Integer.parseInt(record.value().split("-")[1]);
                        batch.add(new Receipt(group, name, new Item(phase, record.key(), record.value(), record.partition(), record.offset())));
                    }
                    // The lab's processing is constructing observations, not external business effects.
                    c.commitSync(records.nextOffsets());
                    received.addAll(batch); // Publish observations only after a successful commit.
                }
            } catch (WakeupException e) {
                if (running) failure = e;
            } catch (Throwable e) {
                failure = e;
            } finally {
                assignment = List.of();
            }
        }

        void stop() throws InterruptedException {
            running = false;
            var c = consumer;
            if (c != null) c.wakeup(); // The one cross-thread operation explicitly supported by KafkaConsumer.
            thread.join(15000);
            require(!thread.isAlive(), "Consumer did not stop: " + name);
        }
    }

    @Override public void close() throws Exception {
        try {
            for (var worker : workers) worker.stop();
        } finally {
            if (producer != null) producer.close(Duration.ofSeconds(5));
            if (admin != null) admin.close(Duration.ofSeconds(5));
            broker.destroy();
        }
    }

    private record Item(int phase, String key, String value, int partition, long offset) { }
    private record Receipt(String group, String worker, Item item) { }
}
