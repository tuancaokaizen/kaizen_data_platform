package com.kaizen.stream.processing.outbox.source.store;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.model.outbox.OutboxRow;
import com.kaizen.stream.utils.connection.JdbcConnections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * inventory."Outbox" over one autocommit connection, reopened after any error.
 * inventory."Outbox" qua một kết nối autocommit, mở lại sau mỗi lỗi.
 */
public class PostgresOutboxStore implements OutboxStore {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PostgresOutboxStore.class);

    // Partial index ix_outbox_unpublished keeps the scan to pending rows / Index partial chỉ quét dòng chờ phát
    static final String POLL = "SELECT \"Seq\", \"Topic\", \"AggregateId\", \"EventType\", \"Version\","
            + " \"Payload\"::text, (extract(epoch FROM \"EventTime\") * 1000)::bigint"
            + " FROM inventory.\"Outbox\""
            + " WHERE \"PublishedAt\" IS NULL AND \"EventTime\" <= now() AND NOT (\"Seq\" = ANY (?))"
            + " ORDER BY \"Seq\" LIMIT ?";
    static final String MARK = "UPDATE inventory.\"Outbox\" SET \"PublishedAt\" = now()"
            + " WHERE \"Seq\" = ANY (?) AND \"PublishedAt\" IS NULL";
    static final String PURGE = "DELETE FROM inventory.\"Outbox\""
            + " WHERE \"PublishedAt\" < now() - ? * interval '1 millisecond'";

    private final StreamSettings.Postgres pg;
    private transient Connection connection;

    public PostgresOutboxStore(StreamSettings.Postgres pg) {
        this.pg = pg;
    }

    @Override
    public List<OutboxRow> poll(Collection<Long> exclude, int limit) throws SQLException {
        return run(c -> {
            Array ids = c.createArrayOf("bigint", exclude.toArray());
            try (PreparedStatement ps = c.prepareStatement(POLL)) {
                ps.setArray(1, ids);
                ps.setInt(2, limit);
                List<OutboxRow> rows = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        rows.add(new OutboxRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4),
                                rs.getLong(5), rs.getString(6), rs.getLong(7)));
                    }
                }
                return rows;
            } finally {
                ids.free();
            }
        });
    }

    @Override
    public void markPublished(Collection<Long> seqs) throws SQLException {
        if (seqs.isEmpty()) {
            return;
        }
        run(c -> {
            Array ids = c.createArrayOf("bigint", seqs.toArray());
            try (PreparedStatement ps = c.prepareStatement(MARK)) {
                ps.setArray(1, ids);
                return ps.executeUpdate();
            } finally {
                ids.free();
            }
        });
    }

    @Override
    public int purge(long retentionMs) throws SQLException {
        return run(c -> {
            try (PreparedStatement ps = c.prepareStatement(PURGE)) {
                ps.setLong(1, retentionMs);
                return ps.executeUpdate();
            }
        });
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.debug("Closing outbox connection failed", e);
            }
            connection = null;
        }
    }

    @FunctionalInterface
    private interface Work<R> {
        R apply(Connection c) throws SQLException;
    }

    private <R> R run(Work<R> work) throws SQLException {
        try {
            if (connection == null || connection.isClosed()) {
                connection = JdbcConnections.openPostgres(pg, "kaizen-flink-outbox-relay");
            }
            return work.apply(connection);
        } catch (SQLException e) {
            // Next call starts from a fresh connection / Lần gọi sau dùng kết nối mới
            close();
            throw e;
        }
    }
}
