package com.kaizen.stream.simulation.order.transform;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderEventType;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.processing.job.JobContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Optional;

/**
 * Per order: forwards ORDER_PLACED, and for the few orders that will be returned keeps the planned return
 * until its business time, then emits ORDER_RETURNED (processing-time timer; past instants fire at once).
 * Theo từng đơn: chuyển tiếp ORDER_PLACED; với số ít đơn sẽ bị trả, giữ phiếu trả đã lập tới giờ nghiệp vụ
 * rồi phát ORDER_RETURNED (timer processing-time; thời điểm đã qua thì bắn ngay).
 */
public class ReturnSchedulerFunction
        extends KeyedProcessFunction<String, Envelope<OrderSnapshot>, Envelope<OrderSnapshot>> {

    private static final long serialVersionUID = 1L;

    private final OrderGenerator generator;
    private final int stateDays;
    private final String eventSource;

    /** Snapshot to emit when the return is due. / Snapshot sẽ phát khi tới giờ trả. */
    private transient ValueState<OrderSnapshot> pending;
    /** Return already emitted; replays of the order must not plan it again. / Đã phát phiếu trả; phát lại đơn không lập lại. */
    private transient ValueState<Boolean> done;

    public ReturnSchedulerFunction(OrderGenerator generator, int stateDays, String eventSource) {
        this.generator = generator;
        this.stateDays = stateDays;
        this.eventSource = eventSource;
    }

    /**
     * transform.return-scheduler, keyed by order id so a return always follows its own order.
     * transform.return-scheduler, key theo id đơn để phiếu trả luôn đi sau chính đơn của nó.
     */
    public static SingleOutputStreamOperator<Envelope<OrderSnapshot>> apply(DataStream<Envelope<OrderSnapshot>> placed,
                                                                           OrderGenerator generator, JobContext context) {
        ReturnSchedulerFunction scheduler = new ReturnSchedulerFunction(generator,
                context.settings().order().getStateDays(), context.eventSource());
        return context.transform(placed
                        .keyBy(event -> event.getData().getOrder().getId())
                        .process(scheduler),
                "return-scheduler", context.settings().flink().getParallelism());
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.days(stateDays))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000)
                .build();
        ValueStateDescriptor<OrderSnapshot> pendingDesc = new ValueStateDescriptor<>("pending-return", OrderSnapshot.class);
        pendingDesc.enableTimeToLive(ttl);
        ValueStateDescriptor<Boolean> doneDesc = new ValueStateDescriptor<>("return-done", Boolean.class);
        doneDesc.enableTimeToLive(ttl);
        pending = getRuntimeContext().getState(pendingDesc);
        done = getRuntimeContext().getState(doneDesc);
    }

    @Override
    public void processElement(Envelope<OrderSnapshot> event, Context ctx, Collector<Envelope<OrderSnapshot>> out)
            throws Exception {
        out.collect(event);
        if (!OrderEventType.ORDER_PLACED.name().equals(event.getMeta().getEventType()) || done.value() != null) {
            return;
        }
        // Only returners keep state; the plan is deterministic, so replays store the same value
        // Chỉ đơn bị trả mới giữ state; kế hoạch tất định nên phát lại ghi đúng giá trị cũ
        Optional<OrderSnapshot> returned = generator.planReturn(event.getData());
        if (returned.isPresent()) {
            pending.update(returned.get());
            ctx.timerService().registerProcessingTimeTimer(returned.get().getOrder().getModifiedDate());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Envelope<OrderSnapshot>> out) throws Exception {
        OrderSnapshot s = pending.value();
        if (s == null || s.getOrder().getModifiedDate() > timestamp) {
            return;
        }
        long version = s.getOrder().getModifiedDate();
        out.collect(Envelope.of(OrderEventType.ORDER_RETURNED, eventSource, s.getOrder().getId(), version, version, s));
        pending.clear();
        done.update(Boolean.TRUE);
    }
}
