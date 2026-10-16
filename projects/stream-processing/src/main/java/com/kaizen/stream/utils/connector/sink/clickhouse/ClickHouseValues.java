package com.kaizen.stream.utils.connector.sink.clickhouse;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Binding helpers for ClickHouse columns: String columns are not Nullable, DateTime columns may be.
 * Hàm bind cho cột ClickHouse: cột String không Nullable, cột DateTime có thể Nullable.
 */
public final class ClickHouseValues {

    private ClickHouseValues() {
    }

    public static void setNullableTimestamp(PreparedStatement statement, int index, Long epochMs) throws SQLException {
        if (epochMs == null) {
            statement.setNull(index, Types.TIMESTAMP);
        } else {
            statement.setTimestamp(index, new Timestamp(epochMs));
        }
    }

    public static String emptyIfNull(String value) {
        return value == null ? "" : value;
    }
}
