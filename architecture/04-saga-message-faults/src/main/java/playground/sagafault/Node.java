package playground.sagafault;

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
import java.util.concurrent.ConcurrentHashMap;
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
 * 서비스 하나의 실행 틀(2편과 같음)에 장애를 넣는 스위치를 붙였다.
 *
 * <ul>
 *   <li>{@link #inbox}: 끄면 받은 메시지의 ID를 기록하지 않고 매번 처리한다.</li>
 *   <li>{@link #duplicate}: 릴레이가 이 종류의 메시지를 Kafka에 두 번 보낸다. "보내고 나서 보냈다고 기록하기 전에
 *       릴레이가 죽고 다시 떠서 또 보낸" 경우를 흉내 낸다.</li>
 *   <li>{@link #delay}: 이 종류의 메시지를 Outbox에 쓴 뒤 정해진 시간 동안 릴레이가 보내지 않는다. 그동안 뒤에 쓴
 *       메시지가 먼저 나간다(네트워크 재시도나 다른 릴레이 경쟁으로 늦어진 경우를 흉내 낸다).</li>
 *   <li>{@link #crashAfter}: 이 종류의 메시지를 처음 처리한 직후, 오프셋을 커밋하지 않은 채 소비자를 닫고 새 소비자로
 *       다시 시작한다. Kafka가 커밋된 위치부터 다시 주므로 같은 메시지가 실제로 재전달된다.</li>
 * </ul>
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

    record Sent(long nanos, String node, String topic, Msg msg) {}

    final String name;
    final HikariDataSource db;
    private final String bootstrap;
    private final ConcurrentLinkedQueue<Sent> trace;
    private final Map<String, Map<String, Handler>> handlers = new TreeMap<>();
    private final List<Thread> threads = new ArrayList<>();
    private volatile boolean running = true;
    private volatile boolean paused;

    volatile boolean inbox = true;
    final Set<String> duplicate = ConcurrentHashMap.newKeySet();
    final Map<String, Long> delay = new ConcurrentHashMap<>();
    volatile String crashAfter;

    final AtomicInteger received = new AtomicInteger();
    final AtomicInteger handled = new AtomicInteger();
    final AtomicInteger ignored = new AtomicInteger();
    final AtomicInteger duplicates = new AtomicInteger();
    final AtomicInteger restarts = new AtomicInteger();

    Node(String name, HikariDataSource db, String bootstrap, ConcurrentLinkedQueue<Sent> trace) throws SQLException {
        this.name = name;
        this.db = db;
        this.bootstrap = bootstrap;
        this.trace = trace;
        sql("""
                CREATE TABLE outbox(seq bigserial PRIMARY KEY, topic text NOT NULL, saga uuid NOT NULL, type text NOT NULL,
                  payload text NOT NULL, available_at timestamptz NOT NULL DEFAULT clock_timestamp(), sent_at timestamptz);
                CREATE TABLE inbox(message_id uuid PRIMARY KEY, received_at timestamptz NOT NULL DEFAULT clock_timestamp());
                """);
    }

    Node on(String topic, String type, Handler handler) {
        handlers.computeIfAbsent(topic, t -> new TreeMap<>()).put(type, handler);
        return this;
    }

    Map<String, Set<String>> subscriptions() {
        var view = new TreeMap<String, Set<String>>();
        handlers.forEach((topic, types) -> view.put(topic, new TreeSet<>(types.keySet())));
        return view;
    }

    void sql(String ddl) throws SQLException {
        try (var c = db.getConnection(); var s = c.createStatement()) {
            s.execute(ddl);
        }
    }

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
            long wait = delay.getOrDefault(msg.type(), 0L);
            try (var ps = c.prepareStatement("""
                    INSERT INTO outbox(topic, saga, type, payload, available_at)
                    VALUES (?,?,?,?, clock_timestamp() + make_interval(secs => ?))""")) {
                ps.setString(1, topic);
                ps.setObject(2, msg.saga());
                ps.setString(3, msg.type());
                ps.setString(4, msg.encode());
                ps.setDouble(5, wait / 1000.0);
                ps.executeUpdate();
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
                        var ps = c.prepareStatement("""
                                SELECT seq, topic, saga, type, payload FROM outbox
                                 WHERE sent_at IS NULL AND available_at <= clock_timestamp() ORDER BY seq LIMIT 100""");
                        var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String topic = rs.getString("topic");
                        String payload = rs.getString("payload");
                        int copies = duplicate.contains(rs.getString("type")) ? 2 : 1;
                        for (int i = 0; i < copies; i++) {
                            producer.send(new ProducerRecord<>(topic, rs.getString("saga"), payload))
                                    .get(15, TimeUnit.SECONDS);
                            trace.add(new Sent(System.nanoTime(), name, topic, Msg.decode(payload)));
                        }
                        try (var mark = c.prepareStatement("UPDATE outbox SET sent_at = clock_timestamp() WHERE seq = ?")) {
                            mark.setLong(1, rs.getLong("seq"));
                            mark.executeUpdate();
                        }
                        sent++;
                    }
                } catch (Exception e) {
                    if (running) System.err.println(name + " relay: " + e);
                }
                if (sent == 0) sleep(5);
            }
        }
    }

    /** 소비 루프. crashAfter에 걸리면 커밋하지 않고 소비자를 버린 뒤 새 소비자로 다시 시작한다. */
    private void consume() {
        while (running) {
            if (!consumeUntilCrash()) return;
            restarts.incrementAndGet();
        }
    }

    private boolean consumeUntilCrash() {
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
                    received.incrementAndGet();
                    var handler = handlers.get(r.topic()).get(msg.type());
                    if (handler == null) {
                        ignored.incrementAndGet();
                    } else {
                        process(msg, handler);
                        if (msg.type().equals(crashAfter)) {
                            // DB 트랜잭션은 커밋됐고 오프셋은 아직이다. 여기서 소비자가 죽은 것처럼 버린다.
                            crashAfter = null;
                            return true;
                        }
                    }
                    commit.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
                }
                if (!commit.isEmpty()) consumer.commitSync(commit);
            }
            return false;
        } catch (Exception e) {
            if (running) System.err.println(name + " consumer: " + e);
            return false;
        }
    }

    private void process(Msg msg, Handler handler) throws SQLException {
        boolean fresh = tx((c, out) -> {
            if (inbox) {
                try (var ps = c.prepareStatement("INSERT INTO inbox(message_id) VALUES (?) ON CONFLICT DO NOTHING")) {
                    ps.setObject(1, msg.id());
                    if (ps.executeUpdate() == 0) return false;
                }
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
