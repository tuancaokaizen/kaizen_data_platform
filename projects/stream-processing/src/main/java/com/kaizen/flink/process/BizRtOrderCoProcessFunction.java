package com.kaizen.flink.process;

import com.kaizen.flink.model.BizRtOrderRow;
import com.kaizen.flink.model.Order;
import com.kaizen.flink.model.OrderBundle;
import com.kaizen.flink.model.OrderPayment;
import com.kaizen.flink.model.Payment;
import com.kaizen.flink.model.PaymentBundle;
import com.kaizen.flink.model.PaymentTransaction;
import com.kaizen.flink.model.Return;
import com.kaizen.flink.model.ReturnPayment;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Join order + payment by OrderId; emit biz_rt_order only when both sides are paired.
 * Join order + payment theo OrderId; chỉ emit khi đủ cặp order + payment SUCCESS.
 * Pending SUCCESS payments wait in keyed state until order doc arrives (TTL = hot window).
 */
public class BizRtOrderCoProcessFunction
        extends KeyedCoProcessFunction<String, OrderBundle, PaymentBundle, BizRtOrderRow> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(BizRtOrderCoProcessFunction.class);
    private static final ZoneId ZONE = ZoneId.of("Asia/Ho_Chi_Minh");
    private static final DateTimeFormatter DAY_FMT = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final String SALE_RETURN_ID = "";

    private final long stateTtlDays;

    private transient ValueState<OrderSideState> orderState;
    private transient ValueState<PaymentSideState> paymentState;

    public BizRtOrderCoProcessFunction(long stateTtlDays) {
        this.stateTtlDays = stateTtlDays;
    }

    @Override
    public void open(Configuration parameters) {
        // TTL aligns with ClickHouse hot window / TTL khớp cửa sổ hot ClickHouse
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.days(stateTtlDays))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();

        ValueStateDescriptor<OrderSideState> orderDesc =
                new ValueStateDescriptor<>("biz-rt-order", OrderSideState.class);
        orderDesc.enableTimeToLive(ttl);
        orderState = getRuntimeContext().getState(orderDesc);

        ValueStateDescriptor<PaymentSideState> payDesc =
                new ValueStateDescriptor<>("biz-rt-payment", PaymentSideState.class);
        payDesc.enableTimeToLive(ttl);
        paymentState = getRuntimeContext().getState(payDesc);
    }

    @Override
    public void processElement1(OrderBundle bundle, Context ctx, Collector<BizRtOrderRow> out)
            throws Exception {
        if (bundle == null || bundle.getOrder() == null || bundle.getOrder().getId() == null) {
            return;
        }
        Order order = bundle.getOrder();
        OrderSideState side = orderState.value();
        if (side == null) {
            side = new OrderSideState();
        }
        side.orderId = order.getId();
        side.shopCode = nullToEmpty(order.getShopCode());
        side.shopName = side.shopCode;
        side.orderStatus = nullToEmpty(order.getStatus());
        side.salesAmount = order.getTotalPayment() == null ? 0.0 : order.getTotalPayment().doubleValue();
        side.saleReportDate = toReportDate(order.getTransactionDate(), order.getCreatedDate());

        Return ret = bundle.getReturnRecord();
        if (ret != null && ret.getId() != null) {
            side.returnId = ret.getId();
            side.returnAmount = ret.getReturnAmount() == null ? 0.0 : ret.getReturnAmount().doubleValue();
            long retTs = ret.getCreatedDate() != null ? ret.getCreatedDate() : order.getCreatedDate();
            side.returnReportDate = toReportDate(retTs, order.getCreatedDate());
        }
        orderState.update(side);

        // Drain pending SUCCESS payments now that order doc exists / Gom payment chờ khi đã có order
        PaymentSideState pay = paymentState.value();
        if (pay != null && !pay.pending.isEmpty()) {
            pay.applied.putAll(pay.pending);
            pay.pending.clear();
            paymentState.update(pay);
        }

        tryEmitPairedRows(out);
    }

    @Override
    public void processElement2(PaymentBundle bundle, Context ctx, Collector<BizRtOrderRow> out)
            throws Exception {
        if (bundle == null || bundle.getPayment() == null) {
            return;
        }
        Payment payment = bundle.getPayment();
        // Ignore non-SUCCESS — matches batch and avoids phantom rows / Bỏ FAILED/PENDING
        if (payment.getStatus() != null && !"SUCCESS".equalsIgnoreCase(payment.getStatus())) {
            return;
        }

        String orderId = resolveOrderId(bundle);
        if (orderId == null || orderId.isEmpty()) {
            LOG.warn("Skip PaymentBundle {}: missing orderId", payment.getId());
            return;
        }

        String returnId = SALE_RETURN_ID;
        if (bundle.getReturnPayment() != null && bundle.getReturnPayment().getReturnId() != null) {
            returnId = bundle.getReturnPayment().getReturnId();
        }
        final String refundReturnId = returnId;

        PaymentFact fact = new PaymentFact();
        fact.paymentId = payment.getId();
        fact.paymentType = payment.getPaymentType();
        fact.returnId = returnId;
        fact.totalPayment = payment.getTotalPayment();
        fact.methodAmounts = sumByMethod(bundle.getTransactions());

        PaymentSideState side = paymentState.value();
        if (side == null) {
            side = new PaymentSideState();
        }

        // When adding refund, keep latest only per returnId (avoid duplicate replay) / Hoàn trùng giữ phiếu mới nhất
        if (fact.paymentType == 2 && !refundReturnId.isEmpty()) {
            side.applied.entrySet().removeIf(e ->
                    e.getValue().paymentType == 2 && refundReturnId.equals(nullToEmpty(e.getValue().returnId)));
            side.pending.entrySet().removeIf(e ->
                    e.getValue().paymentType == 2 && refundReturnId.equals(nullToEmpty(e.getValue().returnId)));
        }

        OrderSideState order = orderState.value();
        if (order == null) {
            // Hold in pending until order arrives / Giữ pending, chưa emit CH
            side.pending.put(payment.getId(), fact);
            paymentState.update(side);
            return;
        }

        // Order already present — apply immediately / Order đã có — map ngay
        side.applied.put(payment.getId(), fact);
        side.pending.remove(payment.getId());
        paymentState.update(side);
        tryEmitPairedRows(out);
    }

    /**
     * Emit only grains that have order doc + at least one matched SUCCESS payment.
     * Chỉ emit grain đã đủ order + payment SUCCESS (không ghi dòng thiếu payment).
     */
    private void tryEmitPairedRows(Collector<BizRtOrderRow> out) throws Exception {
        OrderSideState order = orderState.value();
        PaymentSideState pay = paymentState.value();
        if (order == null || pay == null || pay.applied.isEmpty()) {
            return;
        }

        // SALE grain — requires type-1 receipt payment / Grain bán cần phiếu thu
        if (hasPaymentForGrain(pay.applied, SALE_RETURN_ID, (short) 1)) {
            out.collect(buildRow(
                    order.orderId,
                    SALE_RETURN_ID,
                    "SALE",
                    order.saleReportDate,
                    order.shopCode,
                    order.shopName,
                    order.orderStatus,
                    order.salesAmount,
                    0.0,
                    order.salesAmount,
                    order.salesAmount,
                    0.0,
                    pay.applied));
        }

        // RETURN grain — order return doc + type-2 refund payment / Grain trả cần phiếu trả
        String returnId = nullToEmpty(order.returnId);
        if (!returnId.isEmpty()
                && hasPaymentForGrain(pay.applied, returnId, (short) 2)) {
            String reportDate = order.returnReportDate != null
                    ? order.returnReportDate
                    : order.saleReportDate;
            out.collect(buildRow(
                    order.orderId,
                    returnId,
                    "RETURN",
                    reportDate,
                    order.shopCode,
                    order.shopName,
                    order.orderStatus,
                    0.0,
                    order.returnAmount,
                    -order.returnAmount,
                    0.0,
                    order.returnAmount,
                    pay.applied));
        }
    }

    private static boolean hasPaymentForGrain(
            Map<String, PaymentFact> applied, String returnId, short paymentType) {
        for (PaymentFact f : applied.values()) {
            if (f.paymentType != paymentType) {
                continue;
            }
            if (paymentType == 1 && SALE_RETURN_ID.equals(returnId)) {
                return true;
            }
            if (paymentType == 2 && returnId.equals(nullToEmpty(f.returnId))) {
                return true;
            }
        }
        return false;
    }

    private BizRtOrderRow buildRow(
            String orderId,
            String returnId,
            String documentType,
            String reportDate,
            String shopCode,
            String shopName,
            String orderStatus,
            double salesAmount,
            double returnAmount,
            double netRevenue,
            double totalOrderAmount,
            double totalReturnAmount,
            Map<String, PaymentFact> applied) {

        double receipts = 0.0;
        double expenditures = 0.0;
        Map<Short, Double> methods = new TreeMap<>();

        for (PaymentFact f : applied.values()) {
            boolean forThisGrain = SALE_RETURN_ID.equals(returnId)
                    ? f.paymentType == 1
                    : f.paymentType == 2 && returnId.equals(nullToEmpty(f.returnId));
            if (!forThisGrain) {
                continue;
            }
            // Sum from tx lines when present / Cộng từ dòng tx nếu có
            double factTotal = sumMethodAmounts(f.methodAmounts);
            if (factTotal <= 0.0) {
                factTotal = f.totalPayment;
            }
            if (f.paymentType == 1) {
                // SALE may have retried payments — sum all SUCCESS receipts / Thu bán cộng dồn
                receipts += factTotal;
                for (Map.Entry<Short, Double> e : f.methodAmounts.entrySet()) {
                    methods.merge(e.getKey(), e.getValue(), Double::sum);
                }
            } else if (f.paymentType == 2) {
                // One refund per returnId — max avoids duplicate kafka/PG payments / Hoàn trùng lấy max
                expenditures = Math.max(expenditures, factTotal);
            }
        }
        // PTTT summary for RETURN from winning refund payment / Tóm tắt PTTT phiếu hoàn được chọn
        if (!SALE_RETURN_ID.equals(returnId)) {
            methods.clear();
            for (PaymentFact f : applied.values()) {
                if (f.paymentType == 2 && returnId.equals(nullToEmpty(f.returnId))) {
                    double factTotal = sumMethodAmounts(f.methodAmounts);
                    if (factTotal <= 0.0) {
                        factTotal = f.totalPayment;
                    }
                    if (Math.abs(factTotal - expenditures) < 0.01) {
                        for (Map.Entry<Short, Double> e : f.methodAmounts.entrySet()) {
                            methods.merge(e.getKey(), e.getValue(), Double::sum);
                        }
                        break;
                    }
                }
            }
        }

        double netCash = receipts - expenditures;
        double reconciliationDiff = netCash - (totalOrderAmount - totalReturnAmount);

        BizRtOrderRow row = new BizRtOrderRow();
        row.setReportDate(reportDate != null ? reportDate : today());
        row.setOrderId(orderId);
        row.setReturnId(returnId);
        row.setDocumentType(documentType);
        row.setShopCode(shopCode);
        row.setShopName(shopName);
        row.setOrderStatus(orderStatus);
        row.setSalesAmount(salesAmount);
        row.setReturnAmount(returnAmount);
        row.setNetRevenue(netRevenue);
        row.setTotalReceipts(receipts);
        row.setTotalExpenditures(expenditures);
        row.setNetCashFlow(netCash);
        row.setPaymentMethodSummary(formatMethods(methods));
        row.setTotalOrderAmount(totalOrderAmount);
        row.setTotalReturnAmount(totalReturnAmount);
        row.setReconciliationDiff(reconciliationDiff);
        row.setPendingOrder((byte) 0);
        row.setUpdatedAtMs(System.currentTimeMillis());
        return row;
    }

    private static double sumMethodAmounts(Map<Short, Double> methodAmounts) {
        if (methodAmounts == null || methodAmounts.isEmpty()) {
            return 0.0;
        }
        double sum = 0.0;
        for (double v : methodAmounts.values()) {
            sum += v;
        }
        return sum;
    }

    private static String resolveOrderId(PaymentBundle bundle) {
        OrderPayment op = bundle.getOrderPayment();
        if (op != null && op.getOrderId() != null) {
            return op.getOrderId();
        }
        ReturnPayment rp = bundle.getReturnPayment();
        if (rp != null && rp.getOrderId() != null) {
            return rp.getOrderId();
        }
        return null;
    }

    private static Map<Short, Double> sumByMethod(List<PaymentTransaction> txs) {
        Map<Short, Double> map = new HashMap<>();
        if (txs == null) {
            return map;
        }
        for (PaymentTransaction tx : txs) {
            if (tx == null) {
                continue;
            }
            map.merge(tx.getPaymentMethodId(), (double) tx.getAmount(), Double::sum);
        }
        return map;
    }

    private static String formatMethods(Map<Short, Double> methods) {
        if (methods == null || methods.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Short, Double> e : methods.entrySet()) {
            if (sb.length() > 0) {
                sb.append('|');
            }
            sb.append(methodName(e.getKey())).append(':').append(e.getValue().longValue());
        }
        return sb.toString();
    }

    private static String methodName(short id) {
        switch (id) {
            case 1:
                return "TienMat";
            case 2:
                return "The";
            case 3:
                return "ViDienTu";
            default:
                return "PTTT" + id;
        }
    }

    private static String toReportDate(Long primaryMs, long fallbackMs) {
        long ms = primaryMs != null && primaryMs > 0 ? primaryMs : fallbackMs;
        if (ms <= 0) {
            return today();
        }
        return LocalDate.ofInstant(Instant.ofEpochMilli(ms), ZONE).format(DAY_FMT);
    }

    private static String today() {
        return LocalDate.now(ZONE).format(DAY_FMT);
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    /** Order-side facts held in keyed state / Fact phía order trong keyed state */
    public static class OrderSideState implements Serializable {
        private static final long serialVersionUID = 1L;
        public String orderId;
        public String shopCode = "";
        public String shopName = "";
        public String orderStatus = "";
        public double salesAmount;
        public String saleReportDate;
        public String returnId = "";
        public double returnAmount;
        public String returnReportDate;
    }

    /**
     * pending = SUCCESS payments waiting for order doc;
     * applied = mapped SUCCESS payments kept for re-emit on return updates.
     * pending = payment SUCCESS chờ order; applied = payment đã map (giữ để re-emit).
     */
    public static class PaymentSideState implements Serializable {
        private static final long serialVersionUID = 1L;
        public Map<String, PaymentFact> pending = new HashMap<>();
        public Map<String, PaymentFact> applied = new HashMap<>();
    }

    public static class PaymentFact implements Serializable {
        private static final long serialVersionUID = 1L;
        public String paymentId;
        public short paymentType;
        public String returnId = "";
        public double totalPayment;
        public Map<Short, Double> methodAmounts = new HashMap<>();
    }
}
