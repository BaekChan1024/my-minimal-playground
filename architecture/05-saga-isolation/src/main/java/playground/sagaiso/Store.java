package playground.sagaiso;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * 임시 PostgreSQL 하나와, 실험마다 새로 만드는 Usage·Content database 두 개.
 * Saga의 각 단계는 둘 중 한 database에서 자기 트랜잭션으로 커밋된다. 두 database를 한 트랜잭션으로 묶는 일은 없다.
 */
final class Store implements AutoCloseable {
    interface Work<T> {
        T run(Connection c) throws SQLException;
    }

    private final EmbeddedPostgres pg;
    private int seq;

    Store() throws Exception {
        pg = EmbeddedPostgres.builder()
                .setServerConfig("listen_addresses", "127.0.0.1")
                .setServerConfig("max_connections", "200")
                .setPort(0)
                .start();
    }

    /** 한 실험의 두 서비스 DB. */
    final class World implements AutoCloseable {
        final HikariDataSource usage;
        final HikariDataSource content;

        World() throws Exception {
            int n = ++seq;
            usage = database("w" + n + "_usage");
            content = database("w" + n + "_content");
            sql(usage, """
                    CREATE TABLE quota(author text PRIMARY KEY, lim bigint NOT NULL, used bigint NOT NULL DEFAULT 0,
                      reserved bigint NOT NULL DEFAULT 0);
                    CREATE TABLE reservation(saga text PRIMARY KEY, author text NOT NULL, amount bigint NOT NULL,
                      status text NOT NULL, actual bigint);
                    """);
            sql(content, """
                    CREATE TABLE post(id bigint PRIMARY KEY, body text NOT NULL, version int NOT NULL DEFAULT 1,
                      summary text, summary_saga text, summary_version int, summary_visible boolean NOT NULL DEFAULT false,
                      locked_by text);
                    """);
        }

        <T> T usage(Work<T> work) throws SQLException {
            return tx(usage, work);
        }

        <T> T content(Work<T> work) throws SQLException {
            return tx(content, work);
        }

        @Override
        public void close() {
            usage.close();
            content.close();
        }
    }

    private HikariDataSource database(String name) throws Exception {
        try (var c = pg.getPostgresDatabase().getConnection(); var s = c.createStatement()) {
            s.execute("CREATE DATABASE " + name);
        }
        var config = new HikariConfig();
        config.setJdbcUrl(pg.getJdbcUrl("postgres", name));
        config.setMaximumPoolSize(16);
        config.setPoolName(name);
        return new HikariDataSource(config);
    }

    private static void sql(HikariDataSource ds, String ddl) throws SQLException {
        try (var c = ds.getConnection(); var s = c.createStatement()) {
            s.execute(ddl);
        }
    }

    static <T> T tx(HikariDataSource ds, Work<T> work) throws SQLException {
        try (var c = ds.getConnection()) {
            c.setAutoCommit(false);
            try {
                T result = work.run(c);
                c.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                c.rollback();
                throw e;
            }
        }
    }

    static int update(Connection c, String sql, Object... args) throws SQLException {
        try (var ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) ps.setObject(i + 1, args[i]);
            return ps.executeUpdate();
        }
    }

    static String one(Connection c, String sql, Object... args) throws SQLException {
        try (var ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) ps.setObject(i + 1, args[i]);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    @Override
    public void close() throws Exception {
        pg.close();
    }
}
