package com.kaizen.stream.simulation.payment.transform;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderHeader;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.order.ReturnDocument;
import com.kaizen.stream.model.payment.PaymentEventType;
import com.kaizen.stream.model.payment.PaymentHeader;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.simulation.payment.transform.state.PaymentReceipt;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Per order: plans the receipt once and one refund per return, then emits each payment event when its
 * business time is due (a processing-time timer at that instant; past instants fire at once).
 * Theo từng đơn: lập lịch phiếu thu một lần và một phiếu chi cho mỗi phiếu trả, rồi phát từng sự kiện phiếu
 * khi tới giờ nghiệp vụ của nó (timer processing-time tại thời điểm đó; thời điểm đã qua thì bắn ngay).
 */
public class PaymentSchedulerFunction
        extends KeyedProcessFunction<String, Envelope<OrderSnapshot>, Envelope<PaymentSnapshot>> {

    private static final long serialVersionUID = 1L;

    private static final Comparator<PaymentSnapshot> EMIT_ORDER = Comparator
            .comparing((PaymentSnapshot s) -> s.getPayment().getModifiedDate())
            .thenComparing(s -> s.getPayment().getId());

    private final PaymentDecider decider;
    private final int stateDays;
    private final String eventSource;

    private transient ValueState<PaymentReceipt> receipt;
    private transient MapState<String, Boolean> refunds;
    private transient MapState<String, PaymentSnapshot> due;

    public PaymentSchedulerFunction(PaymentDecider decider, int stateDays, String eventSource) {
        this.decider = decider;
        this.stateDays = stateDays;
        this.eventSource = eventSource;
    }

    /**
     * transform.payment-scheduler, keyed by order id so the receipt and refunds of an order share one state.
     * transform.payment-scheduler, key theo id đơn để phiếu thu và phiếu chi của một đơn dùng chung state.
     */
    public static SingleOutputStreamOperator<Envelope<PaymentSnapshot>> apply(DataStream<Envelope<OrderSnapshot>> orders,
                                                                             JobContext context) {
        StreamSettings.Payment profile = context.settings().payment();
        PaymentSchedulerFunction scheduler = new PaymentSchedulerFunction(new PaymentDecider(profile),
                profile.getStateDays(), context.eventSource());
        return context.transform(orders
                        .keyBy(event -> event.getData().getOrder().getId())
                        .process(scheduler),
                "payment-scheduler", context.settings().flink().getParallelism());
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.days(stateDays))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000)
                .build();
        ValueStateDescriptor<PaymentReceipt> receiptDesc = new ValueStateDescriptor<>("receipt", PaymentReceipt.class);
        receiptDesc.enableTimeToLive(ttl);
        MapStateDescriptor<String, Boolean> refundDesc =
                new MapStateDescriptor<>("refunds", String.class, Boolean.class);
        refundDesc.enableTimeToLive(ttl);
        MapStateDescriptor<String, PaymentSnapshot> dueDesc =
                new MapStateDescriptor<>("due", String.class, PaymentSnapshot.class);
        dueDesc.enableTimeToLive(ttl);
        receipt = getRuntimeContext().getState(receiptDesc);
        refunds = getRuntimeContext().getMapState(refundDesc);
        due = getRuntimeContext().getMapState(dueDesc);
    }

    @Override
    public void processElement(Envelope<OrderSnapshot> event, Context ctx, Collector<Envelope<PaymentSnapshot>> out)
            throws Exception {
        OrderHeader order = event.getData().getOrder();
        PaymentReceipt planned = receipt.value();
        // The receipt is planned once per order, whichever order event comes first
        // Phiếu thu chỉ lập lịch một lần mỗi đơn, theo sự kiện đơn tới trước
        if (planned == null) {
            List<PaymentSnapshot> timeline = decider.receipts(order);
            for (PaymentSnapshot s : timeline) {
                schedule(s, ctx);
            }
            planned = timeline.isEmpty()
                    ? new PaymentReceipt(order.getTransactionDate(), PaymentDecider.CASH)
                    : PaymentReceipt.of(timeline.get(timeline.size() - 1));
            receipt.update(planned);
        }
        // One refund per new return; replays find the return already planned
        // Một phiếu chi cho mỗi phiếu trả mới; phát lại thì phiếu trả đã có trong state
        for (ReturnDocument ret : event.getData().getReturns()) {
            if (refunds.contains(ret.getId())) {
                continue;
            }
            refunds.put(ret.getId(), Boolean.TRUE);
            decider.refund(order, ret, planned.getMethodId(), planned.getSettledAt()).ifPresent(s -> schedule(s, ctx));
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Envelope<PaymentSnapshot>> out)
            throws Exception {
        List<Map.Entry<String, PaymentSnapshot>> ready = new ArrayList<>();
        for (Map.Entry<String, PaymentSnapshot> e : due.entries()) {
            if (e.getValue().getPayment().getModifiedDate() <= timestamp) {
                ready.add(e);
            }
        }
        ready.sort(Map.Entry.comparingByValue(EMIT_ORDER));
        for (Map.Entry<String, PaymentSnapshot> e : ready) {
            out.collect(envelope(e.getValue()));
            due.remove(e.getKey());
        }
    }

    private void schedule(PaymentSnapshot s, Context ctx) {
        PaymentHeader p = s.getPayment();
        try {
            due.put(p.getId() + "|" + p.getStatus(), s);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot store payment " + p.getId(), e);
        }
        ctx.timerService().registerProcessingTimeTimer(p.getModifiedDate());
    }

    private Envelope<PaymentSnapshot> envelope(PaymentSnapshot s) {
        PaymentHeader p = s.getPayment();
        return Envelope.of(PaymentEventType.of(p.getType(), p.getStatus()), eventSource, p.getOrderId(),
                p.getModifiedDate(), p.getModifiedDate(), s);
    }
}
