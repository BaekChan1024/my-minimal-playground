package playground.sagadur;

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
 * 서비스 하나의 실행 틀(2~3편과 같은 Inbox·Outbox 틀)에 두 가지를 더했다.
 *
 * <ul>
 *   <li>{@link #direct}: Outbox를 쓰지 않고, DB 트랜잭션을 커밋한 뒤 Kafka로 바로 보낸다("커밋하고 보내기").</li>
 *   <li>{@link #crashAfter}: 이 종류의 메시지를 {@link #crashAt}번째로 처리해 DB에 커밋한 직후 서비스가 죽은 것처럼 멈춘다.
 *       direct면 보낼 메시지를 보내지 않고, Outbox면 릴레이도 함께 멈춘다. 어느 쪽이든 Kafka 오프셋은 커밋하지 않는다.</li>
 * </ul>
 * 멈춘 서비스는 {@link #dead}가 되고 스레드가 끝난다. 같은 DB·같은 소비 그룹으로 새 Node를 만들면 "재시작"이다.
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

    private record Pending(String topic, Msg msg) {}

    final String name;
    final HikariDataSource db;
    private final String bootstrap;
    private final ConcurrentLinkedQueue<Sent> trace;
    private final Map<String, Map<String, Handler>> handlers = new TreeMap<>();
    private final List<Thread> threads = new ArrayList<>();
    private volatile boolean running = true;
    private volatile boolean paused;
    private KafkaProducer<String, String> directProducer;

    volatile boolean direct;
    volatile String crashAfter;
    volatile int crashAt = 1;
    private int crashSeen;
    volatile boolean dead;

    final AtomicInteger handled = new AtomicInteger();
    final AtomicInteger duplicates = new AtomicInteger();

    Node(String name, HikariDataSource db, String bootstrap, ConcurrentLinkedQueue<Sent> trace, boolean createSchema)
            throws SQLException {
        this.name = name;
        this.db = db;
        this.bootstrap = bootstrap;
        this.trace = trace;
        if (createSchema) {
            sql("""
                    CREATE TABLE outbox(seq bigserial PRIMARY KEY, topic text NOT NULL, saga uuid NOT NULL,
                      payload text NOT NULL, sent_at timestamptz);
                    CREATE TABLE inbox(message_id uuid PRIMARY KEY, received_at timestamptz NOT NULL DEFAULT clock_timestamp());
                    """);
        }
    }

    Node on(String topic, String type, Handler handler) {
        handlers.computeIfAbsent(topic, t -> new TreeMap<>()).put(type, handler);
        return this;
    }

    Set<String> topics() {
        return handlers.keySet();
    }

    void sql(String ddl) throws SQLException {
        try (var c = db.getConnection(); var s = c.createStatement()) {
            s.execute(ddl);
        }
    }

    /** 업무 변경과 보낼 메시지를 한 트랜잭션에서. direct면 커밋한 뒤 바로 보낸다. */
    <T> T tx(Work<T> work) throws SQLException {
        var pending = new ArrayList<Pending>();
        T result;
        try (var c = db.getConnection()) {
            c.setAutoCommit(false);
            try {
                result = work.run(c, out(c, pending));
                c.commit();
            } catch (SQLException | RuntimeException e) {
                c.rollback();
                throw e;
            }
        }
        sendDirect(pending);
        return result;
    }

    private Out out(Connection c, List<Pending> pending) {
        return (topic, msg) -> {
            if (direct) {
                pending.add(new Pending(topic, msg));
                return;
            }
            try (var ps = c.prepareStatement("INSERT INTO outbox(topic, saga, payload) VALUES (?,?,?)")) {
                ps.setString(1, topic);
                ps.setObject(2, msg.saga());
                ps.setString(3, msg.encode());
                ps.executeUpdate();
            }
        };
    }

    private void sendDirect(List<Pending> pending) {
        for (var p : pending) {
            try {
                directProducer.send(new ProducerRecord<>(p.topic(), p.msg().saga().toString(), p.msg().encode()))
                        .get(15, TimeUnit.SECONDS);
                trace.add(new Sent(System.nanoTime(), name, p.topic(), p.msg()));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    void pause() {
        paused = true;
    }

    void resume() {
        paused = false;
    }

    void start() {
        directProducer = new KafkaProducer<>(Map.<String, Object>of("bootstrap.servers", bootstrap, "acks", "all",
                "linger.ms", "0"), new StringSerializer(), new StringSerializer());
        threads.add(Thread.ofPlatform().name(name + "-relay").daemon().start(this::relay));
        if (!handlers.isEmpty()) threads.add(Thread.ofPlatform().name(name + "-consumer").daemon().start(this::consume));
    }

    private void relay() {
        try (var producer = new KafkaProducer<>(Map.<String, Object>of("bootstrap.servers", bootstrap, "acks", "all",
                "linger.ms", "0"), new StringSerializer(), new StringSerializer())) {
            while (running && !dead) {
                int sent = 0;
                try (var c = db.getConnection();
                        var ps = c.prepareStatement(
                                "SELECT seq, topic, saga, payload FROM outbox WHERE sent_at IS NULL ORDER BY seq LIMIT 100");
                        var rs = ps.executeQuery()) {
                    while (rs.next() && !dead) {
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
                    if (running && !dead) System.err.println(name + " relay: " + e);
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
            while (running && !dead) {
                if (paused) {
                    sleep(10);
                    continue;
                }
                var records = consumer.poll(Duration.ofMillis(20));
                var commit = new HashMap<TopicPartition, OffsetAndMetadata>();
                for (var r : records) {
                    var msg = Msg.decode(r.value());
                    var handler = handlers.get(r.topic()).get(msg.type());
                    if (handler != null) {
                        process(msg, handler);
                        if (dead) return;   // 죽었다: 오프셋을 커밋하지 않는다
                    }
                    commit.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
                }
                if (!commit.isEmpty()) consumer.commitSync(commit);
            }
        } catch (Exception e) {
            if (running && !dead) System.err.println(name + " consumer: " + e);
        }
    }

    private void process(Msg msg, Handler handler) throws SQLException {
        var pending = new ArrayList<Pending>();
        boolean fresh;
        try (var c = db.getConnection()) {
            c.setAutoCommit(false);
            try {
                try (var ps = c.prepareStatement("INSERT INTO inbox(message_id) VALUES (?) ON CONFLICT DO NOTHING")) {
                    ps.setObject(1, msg.id());
                    fresh = ps.executeUpdate() == 1;
                }
                if (fresh) handler.handle(msg, c, out(c, pending));
                c.commit();
            } catch (SQLException | RuntimeException e) {
                c.rollback();
                throw e;
            }
        }
        if (!fresh) {
            duplicates.incrementAndGet();
            return;
        }
        handled.incrementAndGet();
        if (msg.type().equals(crashAfter) && ++crashSeen == crashAt) {
            // DB는 커밋됐다. 보낼 메시지(direct)도, 릴레이도, 오프셋 커밋도 여기서 멈춘다.
            dead = true;
            return;
        }
        sendDirect(pending);
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
        if (directProducer != null) directProducer.close();
        db.close();
    }
}
