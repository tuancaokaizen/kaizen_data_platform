package com.kaizen.stream.utils.connector.sink.postgres;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * PostgreSQL upsert that never lets an older snapshot overwrite a newer one:
 * INSERT … ON CONFLICT (keys) DO UPDATE SET … WHERE EXCLUDED."ModifiedDate" &gt;= t."ModifiedDate".
 * Rows that never change after creation use {@link Builder#immutable()}: ON CONFLICT (keys) DO NOTHING.
 * Upsert PostgreSQL không cho snapshot cũ ghi đè bản mới hơn (idempotent khi đọc lại từ EARLIEST).
 * Dòng không đổi sau khi tạo dùng {@link Builder#immutable()}: ON CONFLICT (keys) DO NOTHING.
 */
public final class VersionedUpsert implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String table;
    private final List<String> keys;
    private final List<String> columns;
    private final String versionColumn;
    private final Set<String> insertOnly;
    private final boolean immutable;

    private VersionedUpsert(String table, List<String> keys, List<String> columns,
                            String versionColumn, Set<String> insertOnly, boolean immutable) {
        this.table = table;
        this.keys = keys;
        this.columns = columns;
        this.versionColumn = versionColumn;
        this.insertOnly = insertOnly;
        this.immutable = immutable;
    }

    public String table() {
        return table;
    }

    /** Table as written in SQL, e.g. "order"."Order". / Tên bảng như trong SQL, vd. "order"."Order". */
    public static Builder into(String schema, String table) {
        return new Builder(quote(schema) + "." + quote(table));
    }

    /**
     * Bind order for PreparedStatement parameters (1-based index = position + 1).
     * Thứ tự gán tham số cho PreparedStatement (chỉ số 1-based = vị trí + 1).
     */
    public List<String> columns() {
        return columns;
    }

    public int indexOf(String column) {
        int idx = columns.indexOf(column);
        if (idx < 0) {
            throw new IllegalArgumentException("Unknown column " + column + " in " + table);
        }
        return idx + 1;
    }

    public String sql() {
        String cols = columns.stream().map(VersionedUpsert::quote).collect(Collectors.joining(", "));
        String params = String.join(", ", Collections.nCopies(columns.size(), "?"));
        String conflict = keys.stream().map(VersionedUpsert::quote).collect(Collectors.joining(", "));
        String insert = "INSERT INTO " + table + " AS t (" + cols + ") VALUES (" + params + ")";
        if (immutable) {
            return insert + " ON CONFLICT (" + conflict + ") DO NOTHING";
        }
        // Keys and write-once columns (e.g. CreatedDate, OrderDate) are never updated
        // Khoá và cột chỉ ghi lần đầu (vd. CreatedDate, OrderDate) không bao giờ bị cập nhật
        String updates = columns.stream()
                .filter(c -> !keys.contains(c) && !insertOnly.contains(c))
                .map(c -> quote(c) + " = EXCLUDED." + quote(c))
                .collect(Collectors.joining(", "));
        String v = quote(versionColumn);
        return insert + " ON CONFLICT (" + conflict + ") DO UPDATE SET " + updates
                + " WHERE EXCLUDED." + v + " >= t." + v;
    }

    @Override
    public String toString() {
        return sql();
    }

    static String quote(String identifier) {
        if (identifier.indexOf('"') >= 0) {
            throw new IllegalArgumentException("Identifier must not contain quotes: " + identifier);
        }
        return "\"" + identifier + "\"";
    }

    public static final class Builder {
        private final String table;
        private final List<String> keys = new ArrayList<>();
        private final List<String> columns = new ArrayList<>();
        private final Set<String> insertOnly = new LinkedHashSet<>();
        private String versionColumn = "ModifiedDate";
        private boolean immutable;

        private Builder(String table) {
            this.table = table;
        }

        public Builder key(String... keyColumns) {
            keys.addAll(Arrays.asList(keyColumns));
            return this;
        }

        public Builder columns(String... cols) {
            columns.addAll(Arrays.asList(cols));
            return this;
        }

        public Builder insertOnly(String... cols) {
            insertOnly.addAll(Arrays.asList(cols));
            return this;
        }

        public Builder versionColumn(String column) {
            this.versionColumn = column;
            return this;
        }

        /**
         * Rows are written once and never updated; a replay leaves the stored row untouched.
         * Dòng chỉ ghi một lần, không bao giờ cập nhật; đọc lại không đụng tới dòng đã có.
         */
        public Builder immutable() {
            this.immutable = true;
            return this;
        }

        /**
         * Final column order = keys, then other columns; validates the definition.
         * Thứ tự cột cuối = khoá trước, rồi các cột còn lại; kiểm tra định nghĩa.
         */
        public VersionedUpsert build() {
            if (keys.isEmpty()) {
                throw new IllegalStateException("No key columns for " + table);
            }
            List<String> all = new ArrayList<>(keys);
            for (String c : columns) {
                if (all.contains(c)) {
                    throw new IllegalStateException("Duplicate column " + c + " in " + table);
                }
                all.add(c);
            }
            if (immutable) {
                if (!insertOnly.isEmpty()) {
                    throw new IllegalStateException("Immutable table " + table + " needs no insert-only columns");
                }
                return new VersionedUpsert(table, Collections.unmodifiableList(new ArrayList<>(keys)),
                        Collections.unmodifiableList(all), versionColumn, Collections.emptySet(), true);
            }
            if (!all.contains(versionColumn)) {
                throw new IllegalStateException("Version column " + versionColumn + " not in columns of " + table);
            }
            if (insertOnly.contains(versionColumn)) {
                throw new IllegalStateException("Version column cannot be insert-only in " + table);
            }
            if (!all.containsAll(insertOnly)) {
                throw new IllegalStateException("Insert-only columns must be listed in columns of " + table);
            }
            if (all.size() == keys.size() + insertOnly.size()) {
                throw new IllegalStateException("Nothing to update in " + table);
            }
            return new VersionedUpsert(table, Collections.unmodifiableList(new ArrayList<>(keys)),
                    Collections.unmodifiableList(all), versionColumn,
                    Collections.unmodifiableSet(new LinkedHashSet<>(insertOnly)), false);
        }
    }
}
