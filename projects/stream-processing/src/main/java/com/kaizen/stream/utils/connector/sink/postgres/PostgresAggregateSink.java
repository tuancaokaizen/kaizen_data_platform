package com.kaizen.stream.utils.connector.sink.postgres;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.model.envelope.Envelope;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Writes whole aggregates (e.g. an order with its lines and returns) to PostgreSQL, one transaction per batch,
 * tables in the given parent-to-child order. Batches flush when full, on a timer and on every checkpoint,
 * so a completed checkpoint means everything before it is in PG (at-least-once; upserts make replays safe).
 * Ghi trọn aggregate (vd. đơn kèm dòng và phiếu trả) vào PostgreSQL, mỗi batch một transaction, các bảng theo
 * thứ tự cha trước con. Batch được ghi khi đầy, theo timer và ở mỗi checkpoint, nên checkpoint xong nghĩa là
 * mọi thứ trước đó đã nằm trong PG (at-least-once; upsert giúp đọc lại an toàn).
 */
public final class PostgresAggregateSink<T> extends RichSinkFunction<Envelope<T>> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PostgresAggregateSink.class);
    private static final int MAX_RETRIES = 3;

    /** Aggregate identity used to keep only its newest snapshot. / Định danh aggregate để chỉ giữ snapshot mới nhất. */
    @FunctionalInterface
    public interface AggregateKey<T> extends Serializable {
        String of(Envelope<T> envelope);
    }

    /**
     * Extra work in the batch transaction after every table is written (e.g. posting stock for the batch).
     * Việc thêm trong transaction của batch sau khi ghi xong mọi bảng (vd. ghi sổ kho cho batch).
     */
    @FunctionalInterface
    public interface BatchStep<T> extends Serializable {
        void apply(Connection connection, List<Envelope<T>> batch) throws SQLException;
    }

    private final String name;
    private final StreamSettings.Postgres pg;
    private final AggregateKey<T> keyOf;
    private final List<TableWriter<T>> tables;
    private final List<BatchStep<T>> afterTables;

    private transient LatestByKey<T> buffer;
    private transient Connection connection;
    private transient ScheduledExecutorService flusher;
    private transient volatile Exception failure;

    public PostgresAggregateSink(String name, StreamSettings.Postgres pg, AggregateKey<T> keyOf,
                             List<TableWriter<T>> tables) {
        this(name, pg, keyOf, tables, new ArrayList<>());
    }

    public PostgresAggregateSink(String name, StreamSettings.Postgres pg, AggregateKey<T> keyOf,
                             List<TableWriter<T>> tables, List<BatchStep<T>> afterTables) {
        if (tables.isEmpty()) {
            throw new IllegalArgumentException("No tables for sink " + name);
        }
        this.name = name;
        this.pg = pg;
        this.keyOf = keyOf;
        this.tables = new ArrayList<>(tables);
        this.afterTables = new ArrayList<>(afterTables);
    }

    @Override
    public void open(Configuration parameters) {
        buffer = new LatestByKey<>();
        long interval = pg.getFlushIntervalMs();
        if (interval > 0) {
            flusher = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "pg-flush-" + name);
                t.setDaemon(true);
                return t;
            });
            flusher.scheduleWithFixedDelay(this::flushOnTimer, interval, interval, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void invoke(Envelope<T> value, Context context) throws Exception {
        synchronized (this) {
            checkFailure();
            buffer.add(keyOf.of(value), value);
            if (buffer.size() >= pg.getBatchSize()) {
                flush();
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        synchronized (this) {
            checkFailure();
            flush();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        // Nothing to restore: the buffer is always empty after a checkpoint
        // Không có gì để khôi phục: bộ đệm luôn rỗng sau mỗi checkpoint
    }

    @Override
    public void close() {
        if (flusher != null) {
            flusher.shutdownNow();
        }
        synchronized (this) {
            closeConnection();
        }
    }

    private void flushOnTimer() {
        synchronized (this) {
            if (failure != null) {
                return;
            }
            try {
                flush();
            } catch (Exception e) {
                // Surfaced on the next record or checkpoint / Báo ra ở record hoặc checkpoint kế tiếp
                LOG.error("Timed flush of {} failed", name, e);
            }
        }
    }

    private void checkFailure() throws Exception {
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * A failed batch is already drained, so the failure is sticky: no later checkpoint may succeed.
     * Batch lỗi đã bị rút khỏi bộ đệm nên lỗi được giữ lại: không checkpoint nào sau đó được thành công.
     */
    private void flush() throws Exception {
        if (buffer.isEmpty()) {
            return;
        }
        List<Envelope<T>> batch = buffer.drain();
        for (int attempt = 0; ; attempt++) {
            try {
                write(batch);
                return;
            } catch (SQLException e) {
                closeConnection();
                if (!isTransient(e) || attempt >= MAX_RETRIES) {
                    failure = new IOException("PG sink " + name + " failed on a batch of " + batch.size()
                            + " aggregates starting with " + keyOf.of(batch.get(0)) + ": " + describe(e), e);
                    throw failure;
                }
                LOG.warn("PG sink {} attempt {} failed, retrying: {}", name, attempt + 1, describe(e));
                Thread.sleep(1_000L * (attempt + 1));
            } catch (RuntimeException e) {
                failure = e;
                throw e;
            }
        }
    }

    private void write(List<Envelope<T>> batch) throws SQLException {
        Connection c = connection();
        try {
            for (TableWriter<T> table : tables) {
                try (PreparedStatement ps = c.prepareStatement(table.upsert().sql())) {
                    int rows = 0;
                    for (Envelope<T> envelope : batch) {
                        for (SqlRow row : table.rows(envelope.getData())) {
                            table.bind(ps, row);
                            ps.addBatch();
                            rows++;
                        }
                    }
                    if (rows > 0) {
                        ps.executeBatch();
                    }
                }
            }
            // Same transaction: the steps see the rows just written and commit or fail with them
            // Cùng transaction: các bước thấy dòng vừa ghi và commit hoặc lỗi cùng chúng
            for (BatchStep<T> step : afterTables) {
                step.apply(c, batch);
            }
            c.commit();
        } catch (SQLException | RuntimeException e) {
            rollbackQuietly(c);
            throw e;
        }
    }

    private Connection connection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            Properties props = new Properties();
            props.setProperty("user", pg.getUsername());
            props.setProperty("password", pg.getPassword());
            props.setProperty("ApplicationName", "kaizen-flink-" + name);
            props.setProperty("connectTimeout", "10");
            props.setProperty("tcpKeepAlive", "true");
            // Driver instance directly: DriverManager may not see drivers in Flink's user-code classloader
            // Dùng thẳng Driver: DriverManager có thể không thấy driver trong classloader user-code của Flink
            connection = new org.postgresql.Driver().connect(pg.jdbcUrl(), props);
            if (connection == null) {
                throw new SQLException("Driver rejected URL " + pg.jdbcUrl());
            }
            connection.setAutoCommit(false);
        }
        return connection;
    }

    private void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.debug("Closing connection of {} failed", name, e);
            }
            connection = null;
        }
    }

    private static void rollbackQuietly(Connection c) {
        try {
            c.rollback();
        } catch (SQLException e) {
            LOG.debug("Rollback failed", e);
        }
    }

    /**
     * Connection loss, deadlock and resource errors are retried; bad data and constraint violations are not.
     * Mất kết nối, deadlock, thiếu tài nguyên thì thử lại; dữ liệu sai và vi phạm ràng buộc thì không.
     */
    static boolean isTransient(SQLException e) {
        String state = root(e).getSQLState();
        if (state == null || state.length() < 2) {
            return true;
        }
        String cls = state.substring(0, 2);
        return cls.equals("08") || cls.equals("40") || cls.equals("53") || cls.equals("57");
    }

    private static SQLException root(SQLException e) {
        SQLException next = e.getNextException();
        return next != null ? next : e;
    }

    private static String describe(SQLException e) {
        SQLException r = root(e);
        return "[" + r.getSQLState() + "] " + r.getMessage();
    }
}
