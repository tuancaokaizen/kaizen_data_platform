package com.kaizen.stream.simulation.order.source;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.job.OperatorNames;
import com.kaizen.stream.simulation.order.lookup.MasterData;
import com.kaizen.stream.simulation.order.lookup.MasterDataSource;
import com.kaizen.stream.simulation.order.lookup.OrderNumberSource;
import com.kaizen.stream.simulation.order.lookup.PostgresMasterDataSource;
import com.kaizen.stream.simulation.order.lookup.PostgresOrderNumberSource;
import com.kaizen.stream.simulation.order.transform.OrderEmitter;
import com.kaizen.stream.simulation.order.transform.OrderGenerator;
import com.kaizen.stream.simulation.order.transform.state.OrderCursor;
import com.kaizen.stream.utils.time.IctTime;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * Single-instance source of ORDER_PLACED events in real time. The cursor is checkpointed, so a restore
 * replays the same orders (same event ids) instead of inventing new ones.
 * Source duy nhất phát ORDER_PLACED theo thời gian thực. Con trỏ được checkpoint nên restore sẽ phát lại
 * đúng các đơn cũ (cùng event id) thay vì sinh đơn mới.
 */
public class OrderGeneratorSource extends RichSourceFunction<Envelope<OrderSnapshot>> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OrderGeneratorSource.class);
    /** Orders emitted per lock hold, so checkpoints are not starved during catch-up. / Số đơn mỗi lần giữ lock. */
    static final int BATCH = 500;
    static final long MAX_SLEEP_MS = 1_000L;

    private final OrderEmitter emitter;
    private final MasterDataSource masterSource;
    private final OrderNumberSource numberSource;
    private final long masterRefreshMs;

    private transient ListState<OrderCursor> cursorState;
    private transient OrderCursor restored;
    private transient volatile OrderCursor cursor;
    private transient MasterData master;
    private transient long masterLoadedAt;
    private transient volatile boolean running;

    public OrderGeneratorSource(OrderEmitter emitter, MasterDataSource masterSource, OrderNumberSource numberSource,
                                long masterRefreshMs) {
        this.emitter = emitter;
        this.masterSource = masterSource;
        this.numberSource = numberSource;
        this.masterRefreshMs = masterRefreshMs;
    }

    /**
     * source.generator.order: one instance, so a single cursor keeps sequence numbers and order ids unique.
     * Master data (shops, products, customers) is a lookup read from Postgres, not a stream source.
     * source.generator.order: một instance để một con trỏ duy nhất giữ số thứ tự và id đơn không trùng.
     * Master data (shop, sản phẩm, khách) là lookup đọc từ Postgres, không phải nguồn stream.
     */
    public static DataStream<Envelope<OrderSnapshot>> read(OrderGenerator generator, JobContext context) {
        StreamSettings settings = context.settings();
        StreamSettings.Order profile = settings.order();
        OrderGeneratorSource source = new OrderGeneratorSource(
                new OrderEmitter(generator, profile.getMaxCatchupMs(), context.eventSource()),
                new PostgresMasterDataSource(settings.postgres()), new PostgresOrderNumberSource(settings.postgres()),
                profile.getMasterRefreshMs());
        return context.source(source, OperatorNames.GENERATOR, "order", 1);
    }

    @Override
    public void initializeState(FunctionInitializationContext ctx) throws Exception {
        cursorState = ctx.getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("order-cursor", OrderCursor.class));
        for (OrderCursor c : cursorState.get()) {
            restored = c;
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // No master data → fail fast with a clear message / Không có master data → dừng ngay với thông báo rõ
        master = masterSource.load();
        masterLoadedAt = System.currentTimeMillis();
        running = true;
    }

    @Override
    public void run(SourceContext<Envelope<OrderSnapshot>> ctx) throws Exception {
        Object lock = ctx.getCheckpointLock();
        long startedAt = System.currentTimeMillis();
        // Without state, continue today's numbering from Postgres so existing orders are never overwritten
        // Không có state thì đánh số tiếp từ Postgres để không ghi đè đơn đã có
        Map<String, Integer> usedToday = restored == null
                ? numberSource.highestPerShop(IctTime.businessDate(startedAt)) : Map.of();
        if (restored == null) {
            LOG.info("Fresh start: continuing order numbers of {} shops from Postgres", usedToday.size());
        }
        synchronized (lock) {
            cursor = emitter.start(restored, startedAt, usedToday);
        }
        while (running) {
            long now = System.currentTimeMillis();
            boolean reloaded = refreshMaster(now);
            int emitted;
            synchronized (lock) {
                if (reloaded) {
                    emitter.forgetSettledSeries(cursor, master);
                }
                emitted = emitter.emitDue(cursor, master, now, BATCH, ctx::collect);
            }
            if (emitted == 0) {
                Thread.sleep(Math.max(1L, Math.min(MAX_SLEEP_MS, cursor.getNextAt() - now)));
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        // Before run() starts, keep whatever was restored / Trước khi run() chạy, giữ nguyên trạng thái đã restore
        OrderCursor current = cursor != null ? cursor : restored;
        cursorState.update(current == null ? Collections.emptyList() : Collections.singletonList(current.copy()));
    }

    /** True when a new snapshot was loaded. / True khi đã nạp được ảnh chụp mới. */
    private boolean refreshMaster(long now) {
        if (now - masterLoadedAt < masterRefreshMs) {
            return false;
        }
        masterLoadedAt = now;
        try {
            master = masterSource.load();
            return true;
        } catch (Exception e) {
            LOG.warn("Reloading master data failed, keeping the previous snapshot: {}", e.getMessage());
            return false;
        }
    }
}
