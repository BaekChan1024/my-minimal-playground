package playground.sagadur;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import java.util.HashMap;
import java.util.Collection;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.server.common.MetadataVersion;

/**
 * 실제 임시 Kafka(브로커 1·컨트롤러 1)와 임시 PostgreSQL 18 하나.
 * 서비스마다 PostgreSQL 안에 별도 database를 만든다(database-per-service). 서비스끼리 DB를 함께 읽지 않는다.
 */
final class Infra implements AutoCloseable {
    private final KafkaClusterTestKit cluster;
    private final Admin admin;
    private final EmbeddedPostgres pg;
    final String bootstrap;

    Infra() throws Exception {
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
        pg = EmbeddedPostgres.builder()
                .setServerConfig("listen_addresses", "127.0.0.1")
                .setServerConfig("max_connections", "200")
                .setPort(0)
                .start();
    }

    /** 파티션 1개 토픽. 이 실습은 Saga 사이의 처리 순서를 다루지 않으므로 파티션을 늘리지 않는다. */
    void topics(List<String> names) throws Exception {
        admin.createTopics(names.stream().map(n -> new NewTopic(n, 1, (short) 1)).toList())
                .all().get(15, TimeUnit.SECONDS);
    }

    /** 이 그룹이 아직 커밋하지 않은 메시지 수(토픽 끝 오프셋 - 커밋 오프셋의 합). */
    long lag(String group, Collection<String> topics) throws Exception {
        var committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(15, TimeUnit.SECONDS);
        var request = new HashMap<TopicPartition, OffsetSpec>();
        for (var t : topics) request.put(new TopicPartition(t, 0), OffsetSpec.latest());
        long lag = 0;
        for (var e : admin.listOffsets(request).all().get(15, TimeUnit.SECONDS).entrySet()) {
            var at = committed.get(e.getKey());
            lag += e.getValue().offset() - (at == null ? 0 : at.offset());
        }
        return lag;
    }

    HikariDataSource database(String name) throws Exception {
        try (var c = pg.getPostgresDatabase().getConnection(); var s = c.createStatement()) {
            s.execute("CREATE DATABASE " + name);
        }
        return connect(name);
    }

    /** 이미 있는 database에 새 연결 풀을 연다(재시작한 서비스가 같은 DB를 이어 쓸 때). */
    HikariDataSource connect(String name) {
        var config = new HikariConfig();
        config.setJdbcUrl(pg.getJdbcUrl("postgres", name));
        config.setMaximumPoolSize(4);
        config.setAutoCommit(true);
        config.setPoolName(name);
        return new HikariDataSource(config);
    }

    @Override
    public void close() throws Exception {
        admin.close();
        cluster.close();
        pg.close();
    }
}
