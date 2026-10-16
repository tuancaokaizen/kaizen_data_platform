package com.kaizen.stream.utils.connector.sink.clickhouse;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.utils.connection.JdbcConnections;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Collections;
import java.util.List;

/**
 * Batched JDBC inserts into ClickHouse, flushed on size, interval and every checkpoint (at-least-once).
 * Target tables must be ReplacingMergeTree with a business version so replayed rows collapse.
 * Insert JDBC theo batch vào ClickHouse, ghi khi đầy, theo chu kỳ và ở mỗi checkpoint (at-least-once).
 * Bảng đích phải là ReplacingMergeTree có version nghiệp vụ để dòng phát lại được gộp.
 */
public final class ClickHouseSinkFactory {

    private ClickHouseSinkFactory() {
    }

    public static <T> SinkFunction<T> insert(StreamSettings.ClickHouse ch, String table, List<String> columns,
                                             JdbcStatementBuilder<T> binder, int batchSize, long intervalMs) {
        return JdbcSink.sink(insertSql(table, columns), binder,
                JdbcConnections.batching(batchSize, intervalMs), JdbcConnections.clickhouse(ch));
    }

    /** "report.realtime_shipment_status" → "realtime_shipment_status" (used in operator names). / Bỏ tên database. */
    public static String tableName(String qualifiedTable) {
        return qualifiedTable.substring(qualifiedTable.indexOf('.') + 1);
    }

    static String insertSql(String table, List<String> columns) {
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("No columns for " + table);
        }
        return "INSERT INTO " + table + " (" + String.join(", ", columns) + ") VALUES ("
                + String.join(", ", Collections.nCopies(columns.size(), "?")) + ")";
    }
}
