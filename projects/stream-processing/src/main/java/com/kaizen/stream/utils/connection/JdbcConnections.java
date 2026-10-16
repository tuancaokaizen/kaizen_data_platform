package com.kaizen.stream.utils.connection;

import com.kaizen.stream.config.StreamSettings;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * JDBC connection and batching options shared by PG/ClickHouse sinks.
 * Tuỳ chọn kết nối và gom batch JDBC dùng chung cho sink PG/ClickHouse.
 */
public final class JdbcConnections {

    private JdbcConnections() {
    }

    public static JdbcConnectionOptions postgres(StreamSettings.Postgres pg) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(pg.jdbcUrl())
                .withDriverName("org.postgresql.Driver")
                .withUsername(pg.getUsername())
                .withPassword(pg.getPassword())
                .build();
    }

    /**
     * Plain PG connection for code outside the JDBC connector (autocommit on).
     * Kết nối PG thường cho code ngoài JDBC connector (autocommit bật).
     */
    public static Connection openPostgres(StreamSettings.Postgres pg, String applicationName) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", pg.getUsername());
        props.setProperty("password", pg.getPassword());
        props.setProperty("ApplicationName", applicationName);
        props.setProperty("connectTimeout", "10");
        props.setProperty("tcpKeepAlive", "true");
        // Driver instance directly: DriverManager may not see drivers in Flink's user-code classloader
        // Dùng thẳng Driver: DriverManager có thể không thấy driver trong classloader user-code của Flink
        Connection c = new org.postgresql.Driver().connect(pg.jdbcUrl(), props);
        if (c == null) {
            throw new SQLException("Driver rejected URL " + pg.jdbcUrl());
        }
        return c;
    }

    public static JdbcConnectionOptions clickhouse(StreamSettings.ClickHouse ch) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(ch.jdbcUrl())
                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                .withUsername(ch.getUsername())
                .withPassword(ch.getPassword())
                .build();
    }

    /**
     * Small batches keep latency low at 5–10k orders/day; retries cover short DB blips.
     * Batch nhỏ giữ độ trễ thấp với 5–10k đơn/ngày; retry để vượt lỗi DB ngắn.
     */
    public static JdbcExecutionOptions batching(int batchSize, long intervalMs) {
        return JdbcExecutionOptions.builder()
                .withBatchSize(batchSize)
                .withBatchIntervalMs(intervalMs)
                .withMaxRetries(3)
                .build();
    }
}
