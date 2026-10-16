package com.kaizen.stream.processing.businessrealtime.transform;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.model.payment.PaymentStatus;
import com.kaizen.stream.model.realtime.RealtimeBusinessByOrderRow;
import com.kaizen.stream.processing.businessrealtime.lookup.ShopNames;
import com.kaizen.stream.processing.businessrealtime.transform.state.BusinessRealtimeState;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.utils.time.IctTime;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Joins order and payment snapshots per orderId and emits the report rows whose inputs changed.
 * State expires stateDays after its last write; rows older than that are frozen (see BusinessRealtimeRowBuilder).
 * Ghép snapshot đơn và phiếu theo orderId, phát các dòng báo cáo có đầu vào thay đổi.
 * State hết hạn sau stateDays kể từ lần ghi cuối; dòng cũ hơn mốc đó bị đóng băng (xem BusinessRealtimeRowBuilder).
 */
public class BusinessRealtimeJoinFunction
        extends KeyedCoProcessFunction<String, Envelope<OrderSnapshot>, Envelope<PaymentSnapshot>, RealtimeBusinessByOrderRow> {

    private static final long serialVersionUID = 1L;

    private final int stateDays;
    private final ShopNames shopNames;

    private transient ValueState<BusinessRealtimeState.Order> order;
    private transient MapState<String, BusinessRealtimeState.Payment> payments;

    public BusinessRealtimeJoinFunction(int stateDays, ShopNames shopNames) {
        this.stateDays = stateDays;
        this.shopNames = shopNames;
    }

    /**
     * transform.business-realtime-join: both topics are keyed by orderId, so one keyed operator sees an order and
     * all its payments.
     * transform.business-realtime-join: hai topic cùng key orderId nên một operator theo key thấy đơn và mọi phiếu.
     */
    public static SingleOutputStreamOperator<RealtimeBusinessByOrderRow> apply(DataStream<Envelope<OrderSnapshot>> orders,
                                                                              DataStream<Envelope<PaymentSnapshot>> payments,
                                                                              JobContext context) {
        StreamSettings settings = context.settings();
        StreamSettings.BusinessRealtime businessRealtime = settings.businessRealtime();
        BusinessRealtimeJoinFunction join = new BusinessRealtimeJoinFunction(businessRealtime.getStateDays(),
                new ShopNames(settings.postgres(), businessRealtime.getShopRefreshMs()));
        return context.transform(orders
                        .keyBy(event -> event.getData().getOrder().getId())
                        .connect(payments.keyBy(event -> event.getData().getPayment().getOrderId()))
                        .process(join),
                "business-realtime-join", settings.flink().getParallelism());
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.days(stateDays))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000)
                .build();
        ValueStateDescriptor<BusinessRealtimeState.Order> orderDesc =
                new ValueStateDescriptor<>("order", BusinessRealtimeState.Order.class);
        orderDesc.enableTimeToLive(ttl);
        MapStateDescriptor<String, BusinessRealtimeState.Payment> paymentDesc =
                new MapStateDescriptor<>("payments", String.class, BusinessRealtimeState.Payment.class);
        paymentDesc.enableTimeToLive(ttl);
        order = getRuntimeContext().getState(orderDesc);
        payments = getRuntimeContext().getMapState(paymentDesc);
    }

    @Override
    public void processElement1(Envelope<OrderSnapshot> event, Context ctx, Collector<RealtimeBusinessByOrderRow> out)
            throws Exception {
        BusinessRealtimeState.Order current = order.value();
        long version = event.getMeta().getVersion();
        // Replays and stale snapshots change nothing / Phát lại và snapshot cũ không làm thay đổi gì
        if (current != null && version <= current.getVersion()) {
            return;
        }
        BusinessRealtimeState.Order next = BusinessRealtimeState.Order.of(event.getData(), version);
        order.update(next);
        long now = ctx.timerService().currentProcessingTime();
        BusinessRealtimeRowBuilder.all(next, paymentList(), shopNames.nameOf(next.getShopCode(), now), frozenBefore(now), now)
                .forEach(out::collect);
    }

    @Override
    public void processElement2(Envelope<PaymentSnapshot> event, Context ctx, Collector<RealtimeBusinessByOrderRow> out)
            throws Exception {
        // The source filter already keeps successes only; this guards a misconfigured filter
        // Bộ lọc source đã chỉ giữ phiếu thành công; kiểm tra lại phòng cấu hình lọc sai
        if (event.getData().getPayment().getStatus() != PaymentStatus.SUCCESS) {
            return;
        }
        long version = event.getMeta().getVersion();
        BusinessRealtimeState.Payment next = BusinessRealtimeState.Payment.of(event.getData(), version);
        BusinessRealtimeState.Payment current = payments.get(next.getPaymentId());
        if (current != null && version <= current.getVersion()) {
            return;
        }
        payments.put(next.getPaymentId(), next);
        BusinessRealtimeState.Order o = order.value();
        // Payment before its order: kept until the order event arrives / Phiếu tới trước đơn: giữ chờ sự kiện đơn
        if (o == null) {
            return;
        }
        long now = ctx.timerService().currentProcessingTime();
        BusinessRealtimeRowBuilder.grain(o, paymentList(), next.grain(), shopNames.nameOf(o.getShopCode(), now),
                        frozenBefore(now), now)
                .ifPresent(out::collect);
    }

    private List<BusinessRealtimeState.Payment> paymentList() throws Exception {
        List<BusinessRealtimeState.Payment> out = new ArrayList<>();
        for (BusinessRealtimeState.Payment p : payments.values()) {
            out.add(p);
        }
        return out;
    }

    /** Same cutoff as the CH table TTL. / Cùng mốc với TTL của bảng CH. */
    private LocalDate frozenBefore(long nowMs) {
        return IctTime.businessDate(nowMs).minusDays(stateDays);
    }
}
