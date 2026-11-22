package playground.redisfail;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * 임시 PostgreSQL 18.4. 앱은 DelayProxy를 거쳐 접속하므로 Redis와 같은 네트워크 지연을 줄 수 있다.
 * view_count: 글별 누적 조회수(원본). content_view: postgres 방식의 (글, 세션) 영구 한 행.
 */
final class Db implements AutoCloseable {
    static final int POSTS = 60;

    final DelayProxy proxy;
    final HikariDataSource ds;
    private final EmbeddedPostgres pg;

    Db() throws Exception {
        pg = EmbeddedPostgres.builder()
                .setPort(0)
                .setServerConfig("listen_addresses", "127.0.0.1")
                .setServerConfig("fsync", "on")
                .setServerConfig("synchronous_commit", "on")
                .start();
        proxy = new DelayProxy(pg.getPort());
        var config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://127.0.0.1:" + proxy.port() + "/postgres");
        config.setUsername("postgres");
        config.setAutoCommit(false);
        config.setMaximumPoolSize(4);
        ds = new HikariDataSource(config);
        reset();
    }

    void reset() throws SQLException {
        try (Connection c = ds.getConnection(); var s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS view_count, content_view");
            s.execute("CREATE TABLE view_count (content_id BIGINT PRIMARY KEY, count BIGINT NOT NULL)");
            s.execute("CREATE TABLE content_view (content_id BIGINT NOT NULL, session_id TEXT NOT NULL,"
                    + " viewed_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE (content_id, session_id))");
            s.execute("INSERT INTO view_count SELECT g, 0 FROM generate_series(1," + POSTS + ") g");
            c.commit();
        }
    }

    long totalCount() throws SQLException {
        try (Connection c = ds.getConnection(); var s = c.createStatement();
                var rs = s.executeQuery("SELECT sum(count) FROM view_count")) {
            rs.next();
            long v = rs.getLong(1);
            c.commit();
            return v;
        }
    }

    @Override
    public void close() throws Exception {
        ds.close();
        proxy.close();
        pg.close();
    }
}
