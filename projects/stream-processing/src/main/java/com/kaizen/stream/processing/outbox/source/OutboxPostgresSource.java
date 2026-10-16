package com.kaizen.stream.processing.outbox.source;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.model.outbox.OutboxRow;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.job.OperatorNames;
import com.kaizen.stream.processing.outbox.source.store.OutboxStore;
import com.kaizen.stream.processing.outbox.source.store.PostgresOutboxStore;
import com.kaizen.stream.processing.outbox.transform.OutboxLedger;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Polls unpublished, due rows of inventory."Outbox" and emits them; a row gets PublishedAt only after the next
 * completed checkpoint, so a crash re-emits it (at-least-once; consumers dedupe by meta.version). Rows are
 * not tracked by "last Seq" because transactions commit out of Seq order.
 * Quét dòng chưa phát, đã tới giờ trong inventory."Outbox" rồi phát; dòng chỉ được ghi PublishedAt sau checkpoint
 * hoàn tất kế tiếp, nên sự cố sẽ phát lại (at-least-once; consumer khử trùng theo meta.version). Không theo
 * "Seq cuối" vì các transaction commit không theo thứ tự Seq.
 */
public class OutboxPostgresSource extends RichSourceFunction<OutboxRow>
        implements CheckpointedFunction, CheckpointListener {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OutboxPostgresSource.class);

    private final OutboxStore store;
    private final StreamSettings.Outbox settings;

    private transient ListState<Long> inFlightState;
    private transient OutboxLedger ledger;
    private transient long lastPurgeAt;
    private transient int failures;
    private transient volatile boolean running;

    public OutboxPostgresSource(OutboxStore store, StreamSettings.Outbox settings) {
        this.store = store;
        this.settings = settings;
    }

    /**
     * source.postgres.outbox: a single poller, so in-flight bookkeeping and Seq order stay in one place.
     * source.postgres.outbox: một poller duy nhất để sổ theo dõi dòng đang phát và thứ tự Seq nằm một chỗ.
     */
    public static DataStream<OutboxRow> read(JobContext context) {
        StreamSettings settings = context.settings();
        OutboxPostgresSource source = new OutboxPostgresSource(new PostgresOutboxStore(settings.postgres()),
                settings.outbox());
        return context.source(source, OperatorNames.POSTGRES, "outbox", 1);
    }

    @Override
    public void initializeState(FunctionInitializationContext ctx) throws Exception {
        inFlightState = ctx.getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("outbox-in-flight", Long.class));
        ledger = new OutboxLedger();
        if (ctx.isRestored()) {
            ledger.restore(inFlightState.get());
        }
    }

    @Override
    public void open(Configuration parameters) {
        running = true;
    }

    @Override
    public void run(SourceContext<OutboxRow> ctx) throws Exception {
        Object lock = ctx.getCheckpointLock();
        while (running) {
            int emitted = 0;
            try {
                markConfirmed();
                purgeIfDue(System.currentTimeMillis());
                List<Long> exclude;
                synchronized (ledger) {
                    exclude = ledger.inFlight();
                }
                List<OutboxRow> rows = store.poll(exclude, settings.getBatchSize());
                synchronized (lock) {
                    synchronized (ledger) {
                        for (OutboxRow row : rows) {
                            ctx.collect(row);
                            ledger.emitted(row.getSeq());
                        }
                    }
                }
                emitted = rows.size();
                failures = 0;
            } catch (SQLException e) {
                // PG blips are retried; a long outage fails the job so it shows up / Lỗi PG ngắn thì thử lại
                failures++;
                if (failures >= settings.getMaxConsecutiveFailures()) {
                    throw e;
                }
                LOG.warn("Outbox relay PG error {}/{}: {}", failures, settings.getMaxConsecutiveFailures(),
                        e.getMessage());
            }
            // A full batch means more is waiting / Batch đầy nghĩa là còn dòng chờ
            if (emitted < settings.getBatchSize() && running) {
                Thread.sleep(settings.getPollIntervalMs());
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void close() throws Exception {
        store.close();
        super.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        List<Long> state;
        synchronized (ledger) {
            state = ledger.snapshot(ctx.getCheckpointId());
        }
        inFlightState.update(state);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // Only bookkeeping here; the PG update runs in the poll loop / Chỉ ghi sổ; cập nhật PG chạy trong vòng quét
        synchronized (ledger) {
            ledger.checkpointCompleted(checkpointId);
        }
    }

    private void markConfirmed() throws SQLException {
        List<Long> seqs;
        synchronized (ledger) {
            seqs = ledger.toMark();
        }
        if (seqs.isEmpty()) {
            return;
        }
        store.markPublished(seqs);
        synchronized (ledger) {
            ledger.marked(new ArrayList<>(seqs));
        }
    }

    private void purgeIfDue(long now) throws SQLException {
        if (now - lastPurgeAt < settings.getPurgeIntervalMs()) {
            return;
        }
        int deleted = store.purge(settings.getRetentionMs());
        lastPurgeAt = now;
        if (deleted > 0) {
            LOG.info("Purged {} published outbox rows", deleted);
        }
    }
}
