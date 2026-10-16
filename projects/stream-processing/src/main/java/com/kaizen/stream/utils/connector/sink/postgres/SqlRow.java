package com.kaizen.stream.utils.connector.sink.postgres;

import java.io.Serializable;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One table row as column → value, bound by name so column order never matters.
 * Một dòng bảng dạng cột → giá trị, gán theo tên nên thứ tự cột không quan trọng.
 */
public final class SqlRow implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, Object> values = new LinkedHashMap<>();

    public static SqlRow create() {
        return new SqlRow();
    }

    /** Values must be JDBC-ready (String, Long, Integer, Short, Boolean, LocalDate, OffsetDateTime). */
    public SqlRow set(String column, Object value) {
        if (values.containsKey(column)) {
            throw new IllegalStateException("Column " + column + " set twice");
        }
        values.put(column, value);
        return this;
    }

    public Map<String, Object> values() {
        return Collections.unmodifiableMap(values);
    }

    /** Epoch ms → timestamptz value. / Epoch ms → giá trị timestamptz. */
    public static OffsetDateTime timestamptz(Long epochMs) {
        return epochMs == null ? null : OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMs), ZoneOffset.UTC);
    }

    /** Integer → smallint value. / Integer → giá trị smallint. */
    public static Short smallint(Integer value) {
        return value == null ? null : value.shortValue();
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
