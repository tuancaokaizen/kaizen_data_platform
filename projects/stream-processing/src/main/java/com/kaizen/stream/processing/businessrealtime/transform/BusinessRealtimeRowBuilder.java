package com.kaizen.stream.processing.businessrealtime.transform;

import com.kaizen.stream.model.realtime.RealtimeBusinessByOrderRow;
import com.kaizen.stream.processing.businessrealtime.transform.state.BusinessRealtimeState.Order;
import com.kaizen.stream.processing.businessrealtime.transform.state.BusinessRealtimeState.Payment;
import com.kaizen.stream.processing.businessrealtime.transform.state.BusinessRealtimeState.Return;
import com.kaizen.stream.utils.time.IctTime;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Pure mapping from an order's state to its report rows: one SALE row plus one RETURN row per return.
 * Only successful payments are passed in; a grain without one is emitted as pending (PendingOrder = 1).
 * Ánh xạ thuần từ state của đơn ra các dòng báo cáo: 1 dòng bán và mỗi phiếu trả 1 dòng.
 * Chỉ phiếu thành công được đưa vào; grain chưa có phiếu thì phát ở trạng thái chờ (PendingOrder = 1).
 */
public final class BusinessRealtimeRowBuilder {

    private BusinessRealtimeRowBuilder() {
    }

    /** Every grain of the order. / Mọi grain của đơn. */
    public static List<RealtimeBusinessByOrderRow> all(Order order, Collection<Payment> payments, String shopName,
                                     LocalDate frozenBefore, long nowMs) {
        List<RealtimeBusinessByOrderRow> out = new ArrayList<>();
        sale(order, payments, shopName, frozenBefore, nowMs).ifPresent(out::add);
        for (Return r : order.getReturns()) {
            refund(order, r, payments, shopName, frozenBefore, nowMs).ifPresent(out::add);
        }
        return out;
    }

    /** The grain settled by one payment ("" = SALE, else a returnId). / Grain do một phiếu thanh toán. */
    public static Optional<RealtimeBusinessByOrderRow> grain(Order order, Collection<Payment> payments, String grain,
                                           String shopName, LocalDate frozenBefore, long nowMs) {
        if (RealtimeBusinessByOrderRow.SALE_GRAIN.equals(grain)) {
            return sale(order, payments, shopName, frozenBefore, nowMs);
        }
        for (Return r : order.getReturns()) {
            if (r.getId().equals(grain)) {
                return refund(order, r, payments, shopName, frozenBefore, nowMs);
            }
        }
        // Refund arrived before the order event that carries its return / Phiếu chi tới trước sự kiện đơn có phiếu trả
        return Optional.empty();
    }

    private static Optional<RealtimeBusinessByOrderRow> sale(Order o, Collection<Payment> payments, String shopName,
                                           LocalDate frozenBefore, long nowMs) {
        List<Payment> receipts = settling(payments, RealtimeBusinessByOrderRow.SALE_GRAIN);
        // A returned order always had a receipt; none in state means it expired or was never read,
        // so emitting would overwrite a correct row with a "pending" one
        // Đơn đã trả chắc chắn từng có phiếu thu; state không có nghĩa là đã hết hạn hoặc chưa đọc được,
        // phát ra sẽ ghi đè dòng đúng bằng dòng "chờ"
        if (receipts.isEmpty() && !o.getReturns().isEmpty()) {
            return Optional.empty();
        }
        LocalDate date = IctTime.businessDate(o.getTransactionDate());
        if (date.isBefore(frozenBefore)) {
            return Optional.empty();
        }
        long received = total(receipts);
        return Optional.of(RealtimeBusinessByOrderRow.builder()
                .reportDate(date)
                .orderId(o.getOrderId())
                .returnId(RealtimeBusinessByOrderRow.SALE_GRAIN)
                .documentType(RealtimeBusinessByOrderRow.SALE)
                .shopCode(o.getShopCode())
                .shopName(shopName)
                .orderStatus(o.getStatus())
                .salesAmount(o.getTotalPayment())
                .netRevenue(o.getTotalPayment())
                .totalReceipts(received)
                .netCashFlow(received)
                .paymentMethodSummary(methodSummary(receipts))
                .totalOrderAmount(o.getTotalPayment())
                .reconciliationDiff(received - o.getTotalPayment())
                .pendingOrder(receipts.isEmpty() ? 1 : 0)
                .version(version(o, payments))
                .updatedAtMs(nowMs)
                .build());
    }

    private static Optional<RealtimeBusinessByOrderRow> refund(Order o, Return r, Collection<Payment> payments, String shopName,
                                             LocalDate frozenBefore, long nowMs) {
        LocalDate date = IctTime.businessDate(r.getCreatedDate());
        if (date.isBefore(frozenBefore)) {
            return Optional.empty();
        }
        List<Payment> refunds = settling(payments, r.getId());
        long paid = total(refunds);
        return Optional.of(RealtimeBusinessByOrderRow.builder()
                .reportDate(date)
                .orderId(o.getOrderId())
                .returnId(r.getId())
                .documentType(RealtimeBusinessByOrderRow.RETURN)
                .shopCode(o.getShopCode())
                .shopName(shopName)
                .orderStatus(o.getStatus())
                .returnAmount(r.getReturnAmount())
                .netRevenue(-r.getReturnAmount())
                .totalExpenditures(paid)
                .netCashFlow(-paid)
                .paymentMethodSummary(methodSummary(refunds))
                .totalReturnAmount(r.getReturnAmount())
                .reconciliationDiff(r.getReturnAmount() - paid)
                .pendingOrder(refunds.isEmpty() ? 1 : 0)
                .version(version(o, payments))
                .updatedAtMs(nowMs)
                .build());
    }

    private static List<Payment> settling(Collection<Payment> payments, String grain) {
        List<Payment> out = new ArrayList<>();
        for (Payment p : payments) {
            if (grain.equals(p.grain())) {
                out.add(p);
            }
        }
        return out;
    }

    /**
     * All successful payments are summed, so a duplicate receipt shows up as a reconciliation diff.
     * Cộng mọi phiếu thành công, nên phiếu thu trùng sẽ lộ ra ở chênh lệch đối soát.
     */
    private static long total(List<Payment> payments) {
        long sum = 0;
        for (Payment p : payments) {
            sum += p.getAmount();
        }
        return sum;
    }

    /** Newest input version, so the row version only grows with the state. / Version mới nhất của đầu vào. */
    static long version(Order o, Collection<Payment> payments) {
        long v = o.getVersion();
        for (Payment p : payments) {
            v = Math.max(v, p.getVersion());
        }
        return v;
    }

    /** "TienMat:205000|ViDienTu:100000", ordered by method id. / Sắp theo mã PTTT. */
    static String methodSummary(List<Payment> payments) {
        Map<Integer, Long> byMethod = new TreeMap<>();
        for (Payment p : payments) {
            p.getMethodAmounts().forEach((method, amount) -> byMethod.merge(method, amount, Long::sum));
        }
        StringBuilder sb = new StringBuilder();
        byMethod.forEach((method, amount) -> {
            if (sb.length() > 0) {
                sb.append('|');
            }
            sb.append(methodName(method)).append(':').append(amount);
        });
        return sb.toString();
    }

    /** Codes of payment."PaymentMethod". / Mã trong payment."PaymentMethod". */
    private static String methodName(int id) {
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
}
