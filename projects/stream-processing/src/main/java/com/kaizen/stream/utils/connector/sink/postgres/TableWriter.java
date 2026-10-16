package com.kaizen.stream.utils.connector.sink.postgres;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One table of an aggregate: the upsert statement plus how to turn a snapshot into its rows.
 * Một bảng của aggregate: câu upsert và cách biến snapshot thành các dòng của bảng.
 */
public final class TableWriter<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Snapshot → rows of this table (may be empty). / Snapshot → các dòng của bảng (có thể rỗng). */
    @FunctionalInterface
    public interface Rows<T> extends Serializable {
        List<SqlRow> of(T snapshot);
    }

    private final VersionedUpsert upsert;
    private final Rows<T> rows;

    private TableWriter(VersionedUpsert upsert, Rows<T> rows) {
        this.upsert = upsert;
        this.rows = rows;
    }

    public static <T> TableWriter<T> of(VersionedUpsert upsert, Rows<T> rows) {
        return new TableWriter<>(upsert, rows);
    }

    public VersionedUpsert upsert() {
        return upsert;
    }

    /**
     * Rows for a snapshot; every row must set exactly the table's columns.
     * Các dòng của snapshot; mỗi dòng phải có đúng các cột của bảng.
     */
    public List<SqlRow> rows(T snapshot) {
        List<SqlRow> out = rows.of(snapshot);
        Set<String> expected = new HashSet<>(upsert.columns());
        for (SqlRow row : out) {
            if (!row.values().keySet().equals(expected)) {
                Set<String> missing = new HashSet<>(expected);
                missing.removeAll(row.values().keySet());
                Set<String> extra = new HashSet<>(row.values().keySet());
                extra.removeAll(expected);
                throw new IllegalStateException(upsert.table() + " row columns mismatch, missing " + missing
                        + ", unknown " + extra);
            }
        }
        return out;
    }

    void bind(PreparedStatement ps, SqlRow row) throws SQLException {
        Map<String, Object> values = row.values();
        List<String> columns = upsert.columns();
        for (int i = 0; i < columns.size(); i++) {
            ps.setObject(i + 1, values.get(columns.get(i)));
        }
    }
}
