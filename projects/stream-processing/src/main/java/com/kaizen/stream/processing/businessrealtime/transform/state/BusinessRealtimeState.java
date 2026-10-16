package com.kaizen.stream.processing.businessrealtime.transform.state;

import com.kaizen.stream.model.order.OrderHeader;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.order.ReturnDocument;
import com.kaizen.stream.model.payment.PaymentHeader;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.model.payment.PaymentTransaction;
import com.kaizen.stream.model.payment.PaymentType;
import com.kaizen.stream.model.realtime.RealtimeBusinessByOrderRow;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Compact per-order state: only what the report rows need, not whole snapshots.
 * State gọn theo đơn: chỉ giữ những gì dòng báo cáo cần, không giữ cả snapshot.
 */
public final class BusinessRealtimeState {

    private BusinessRealtimeState() {
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order implements Serializable {
        private String orderId;
        private String shopCode;
        private long transactionDate;
        private long totalPayment;
        /** OrderStatus label as stored in PG. / Nhãn OrderStatus như lưu ở PG. */
        private String status;
        private long version;
        private List<Return> returns = new ArrayList<>();

        public static Order of(OrderSnapshot s, long version) {
            OrderHeader h = s.getOrder();
            List<Return> returns = new ArrayList<>();
            if (s.getReturns() != null) {
                for (ReturnDocument r : s.getReturns()) {
                    returns.add(new Return(r.getId(), r.getCreatedDate(), r.getReturnAmount()));
                }
            }
            return new Order(h.getId(), h.getShopCode(), h.getTransactionDate(), h.getTotalPayment(),
                    h.getStatus().label(), version, returns);
        }

        public boolean hasReturn(String returnId) {
            return returns.stream().anyMatch(r -> r.getId().equals(returnId));
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Return implements Serializable {
        private String id;
        private long createdDate;
        private long returnAmount;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Payment implements Serializable {
        private String paymentId;
        private PaymentType type;
        /** Set for refunds only. / Chỉ có ở phiếu chi. */
        private String returnId;
        private long amount;
        /** paymentMethodId → Σ amount. / paymentMethodId → tổng tiền. */
        private TreeMap<Integer, Long> methodAmounts = new TreeMap<>();
        private long version;

        public static Payment of(PaymentSnapshot s, long version) {
            PaymentHeader h = s.getPayment();
            TreeMap<Integer, Long> methods = new TreeMap<>();
            if (s.getTransactions() != null) {
                for (PaymentTransaction t : s.getTransactions()) {
                    methods.merge(t.getPaymentMethodId(), t.getAmount(), Long::sum);
                }
            }
            return new Payment(h.getId(), h.getType(), h.getReturnId(), h.getTotalAmount(), methods, version);
        }

        /** Grain this payment settles: "" for receipts, the returnId for refunds. / Grain mà phiếu này thanh toán. */
        public String grain() {
            return type == PaymentType.RECEIPT ? RealtimeBusinessByOrderRow.SALE_GRAIN : returnId;
        }
    }
}
