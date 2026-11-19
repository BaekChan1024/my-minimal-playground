package playground.saga;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * 서비스 하나의 실행 틀. 자기 DB, Transactional Outbox, Inbox, Kafka 소비 루프를 가진다.
 *
 * 받은 메시지는 한 DB 트랜잭션 안에서 처리한다: Inbox에 메시지 ID를 넣고(이미 있으면 건너뜀), 업무 데이터를 바꾸고,
 * 보낼 메시지를 Outbox에 넣는다. 커밋한 뒤에 Kafka 오프셋을 커밋한다. 별도 릴레이 스레드가 Outbox를 읽어 Kafka로 보낸다.
 * 그래서 "DB는 바뀌었는데 다음 메시지가 안 나감"과 "같은 메시지를 두 번 처리"가 구조적으로 막힌다.
 * (이 틀 자체를 깨뜨리는 실험은 다음 회차의 몫이다.)
 */
final class Node implements AutoCloseable {
    interface Out {
        void send(String topic, Msg msg) throws SQLException;
    }

    interface Handler {
        void handle(Msg msg, Connection tx, Out out) throws SQLException;
    }

    interface Work<T> {
        T run(Connection tx, Out out) throws SQLException;
    }

    /** 릴레이가 Kafka로 보낸 메시지. 실험이 Saga별 메시지 순서를 보려고 모은다. */
    record Sent(long nanos, String node, String topic, Msg msg) {}

    final String name;
    final HikariDataSource db;
    private final String bootstrap;
    private final ConcurrentLinkedQueue<Sent> trace;
    private final Map<String, Map<String, Handler>> handlers = new TreeMap<>();
    private final Set<String> produces = new TreeSet<>();
    private final List<Thread> threads = new ArrayList<>();
    private volatile boolean running = true;
    private volatile boolean paused;
    final AtomicInteger handled = new AtomicInteger();
    final AtomicInteger ignored = new AtomicInteger();
    final AtomicInteger duplicates = new AtomicInteger();

    Node(String name, HikariDataSource db, String bootstrap, ConcurrentLinkedQueue<Sent> trace) throws SQLException {
        this.name = name;
        this.db = db;
        this.bootstrap = bootstrap;
        this.trace = trace;
        sql("""
                CREATE TABLE outbox(seq bigserial PRIMARY KEY, topic text NOT NULL, saga uuid NOT NULL,
                  payload text NOT NULL, sent_at timestamptz);
                CREATE TABLE inbox(message_id uuid PRIMARY KEY, received_at timestamptz NOT NULL DEFAULT clock_timestamp());
                """);
    }

    /** 이 서비스가 topic에서 type 메시지를 처리한다. 등록하지 않은 type은 읽고 무시한다. */
    Node on(String topic, String type, Handler handler) {
        handlers.computeIfAbsent(topic, t -> new TreeMap<>()).put(type, handler);
        return this;
    }

    /** 구독 목록(토픽 → 처리하는 메시지 종류). 실험 4·5가 서비스 사이의 의존을 이 값으로 센다. */
    Map<String, Set<String>> subscriptions() {
        var view = new TreeMap<String, Set<String>>();
        handlers.forEach((topic, types) -> view.put(topic, new TreeSet<>(types.keySet())));
        return view;
    }

    /** 실제로 Outbox에 넣은 적이 있는 토픽. */
    Set<String> produced() {
        synchronized (produces) {
            return new TreeSet<>(produces);
        }
    }

    void sql(String ddl) throws SQLException {
        try (var c = db.getConnection(); var s = c.createStatement()) {
            s.execute(ddl);
        }
    }

    /** 업무 데이터 변경과 Outbox 기록을 한 트랜잭션으로. */
    <T> T tx(Work<T> work) throws SQLException {
        try (var c = db.getConnection()) {
            c.setAutoCommit(false);
            try {
                T result = work.run(c, outbox(c));
                c.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                c.rollback();
                throw e;
            }
        }
    }

    private Out outbox(Connection c) {
        return (topic, msg) -> {
            try (var ps = c.prepareStatement("INSERT INTO outbox(topic, saga, payload) VALUES (?,?,?)")) {
                ps.setString(1, topic);
                ps.setObject(2, msg.saga());
                ps.setString(3, msg.encode());
                ps.executeUpdate();
            }
            synchronized (produces) {
                produces.add(topic);
            }
        };
    }

    void pause() {
        paused = true;
    }

    void resume() {
        paused = false;
    }

    void start() {
        threads.add(Thread.ofPlatform().name(name + "-relay").daemon().start(this::relay));
        if (!handlers.isEmpty()) threads.add(Thread.ofPlatform().name(name + "-consumer").daemon().start(this::consume));
    }

    private void relay() {
        try (var producer = new KafkaProducer<>(Map.<String, Object>of("bootstrap.servers", bootstrap, "acks", "all",
                "linger.ms", "0"), new StringSerializer(), new StringSerializer())) {
            while (running) {
                int sent = 0;
                try (var c = db.getConnection();
                        var ps = c.prepareStatement(
                                "SELECT seq, topic, saga, payload FROM outbox WHERE sent_at IS NULL ORDER BY seq LIMIT 100");
                        var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String topic = rs.getString("topic");
                        String payload = rs.getString("payload");
                        producer.send(new ProducerRecord<>(topic, rs.getString("saga"), payload)).get(15, TimeUnit.SECONDS);
                        try (var mark = c.prepareStatement("UPDATE outbox SET sent_at = clock_timestamp() WHERE seq = ?")) {
                            mark.setLong(1, rs.getLong("seq"));
                            mark.executeUpdate();
                        }
                        trace.add(new Sent(System.nanoTime(), name, topic, Msg.decode(payload)));
                        sent++;
                    }
                } catch (Exception e) {
                    if (running) System.err.println(name + " relay: " + e);
                }
                if (sent == 0) sleep(5);
            }
        }
    }

    private void consume() {
        var config = new HashMap<String, Object>(Map.of(
                "bootstrap.servers", bootstrap,
                "group.id", name,
                "enable.auto.commit", "false",
                "auto.offset.reset", "earliest",
                "max.poll.records", "100"));
        try (var consumer = new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            var partitions = handlers.keySet().stream().map(t -> new TopicPartition(t, 0)).toList();
            consumer.assign(partitions);
            var stored = consumer.committed(Set.copyOf(partitions));
            for (var tp : partitions) {
                var at = stored.get(tp);
                if (at == null) consumer.seekToBeginning(List.of(tp));
                else consumer.seek(tp, at.offset());
            }
            while (running) {
                if (paused) {
                    sleep(10);
                    continue;
                }
                var records = consumer.poll(Duration.ofMillis(20));
                var commit = new HashMap<TopicPartition, OffsetAndMetadata>();
                for (var r : records) {
                    var msg = Msg.decode(r.value());
                    var handler = handlers.get(r.topic()).get(msg.type());
                    if (handler == null) {
                        ignored.incrementAndGet();
                    } else {
                        process(msg, handler);
                    }
                    commit.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
                }
                if (!commit.isEmpty()) consumer.commitSync(commit);
            }
        } catch (Exception e) {
            if (running) System.err.println(name + " consumer: " + e);
        }
    }

    private void process(Msg msg, Handler handler) throws SQLException {
        boolean fresh = tx((c, out) -> {
            try (var ps = c.prepareStatement("INSERT INTO inbox(message_id) VALUES (?) ON CONFLICT DO NOTHING")) {
                ps.setObject(1, msg.id());
                if (ps.executeUpdate() == 0) return false;
            }
            handler.handle(msg, c, out);
            return true;
        });
        if (fresh) handled.incrementAndGet();
        else duplicates.incrementAndGet();
    }

    long count(String sql, Object... args) throws SQLException {
        try (var c = db.getConnection(); var ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) ps.setObject(i + 1, args[i]);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    String text(String sql, Object... args) throws SQLException {
        try (var c = db.getConnection(); var ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) ps.setObject(i + 1, args[i]);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    /** 아직 Kafka로 나가지 않은 Outbox 행 수. */
    long pending() throws SQLException {
        return count("SELECT count(*) FROM outbox WHERE sent_at IS NULL");
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() throws Exception {
        running = false;
        for (var t : threads) t.join(5_000);
        db.close();
    }
}
