package com.kaizen.stream.processing.rule;

import com.kaizen.stream.model.payment.PaymentEventType;
import com.kaizen.stream.model.payment.PaymentHeader;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.model.payment.PaymentStatus;
import com.kaizen.stream.model.payment.PaymentTransaction;
import com.kaizen.stream.model.payment.PaymentType;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Per-message payment invariants. "Exactly one SUCCESS receipt per order" (Property 3) spans messages
 * and is enforced by the payment producer's keyed state, not here.
 * Bất biến trên từng message phiếu. "Đúng 1 phiếu thu SUCCESS mỗi đơn" (Property 3) trải qua nhiều message
 * nên do keyed state của payment producer đảm bảo, không kiểm ở đây.
 */
public final class PaymentRules {

    private PaymentRules() {
    }

    public static List<String> violations(String eventType, Long version, PaymentSnapshot s) {
        Violations v = new Violations();
        PaymentHeader p = s.getPayment();
        if (p == null) {
            return v.check(false, "payment is missing").list();
        }
        v.require("payment.id", p.getId())
                .require("payment.type", p.getType())
                .require("payment.status", p.getStatus())
                .require("payment.orderId", p.getOrderId())
                .require("payment.shopCode", p.getShopCode())
                .require("payment.totalAmount", p.getTotalAmount())
                .require("payment.attempt", p.getAttempt())
                .require("payment.createdDate", p.getCreatedDate())
                .require("payment.modifiedDate", p.getModifiedDate());
        if (!v.isEmpty()) {
            return v.list();
        }

        v.check(Objects.equals(version, p.getModifiedDate()),
                "meta.version " + version + " != payment.modifiedDate " + p.getModifiedDate());
        v.check(p.getModifiedDate() >= p.getCreatedDate(), "payment.modifiedDate < createdDate");
        v.check(p.getTotalAmount() > 0, "payment.totalAmount must be > 0");
        v.check(p.getAttempt() >= 1, "payment.attempt must be >= 1");

        // Event type must match (type, status) / eventType phải khớp (type, status)
        String expected;
        try {
            expected = PaymentEventType.of(p.getType(), p.getStatus()).name();
        } catch (IllegalArgumentException e) {
            expected = null;
            v.check(false, e.getMessage());
        }
        if (expected != null) {
            v.check(expected.equals(eventType), "eventType " + eventType + " but payment implies " + expected);
        }
        // Receipts link to the order only; refunds link to one return / Phiếu thu chỉ gắn đơn; phiếu chi gắn 1 phiếu trả
        if (p.getType() == PaymentType.RECEIPT) {
            v.check(p.getReturnId() == null, "RECEIPT must not have returnId");
        } else {
            v.check(p.getReturnId() != null && !p.getReturnId().isEmpty(), "REFUND needs returnId");
        }

        checkTransactions(v, p, s.getTransactions() == null ? Collections.emptyList() : s.getTransactions());
        return v.list();
    }

    /**
     * SUCCESS must be fully split over methods; PENDING/FAILED may carry no split yet.
     * SUCCESS phải chia đủ theo phương thức; PENDING/FAILED có thể chưa có.
     */
    private static void checkTransactions(Violations v, PaymentHeader p, List<PaymentTransaction> txns) {
        if (p.getStatus() == PaymentStatus.SUCCESS) {
            v.requireNonEmpty("transactions", txns);
        }
        if (txns.isEmpty()) {
            return;
        }
        Set<String> ids = new HashSet<>();
        long sum = 0;
        for (PaymentTransaction t : txns) {
            String q = "transactions[" + t.getId() + "]";
            v.require(q + ".id", t.getId())
                    .require(q + ".paymentMethodId", t.getPaymentMethodId())
                    .require(q + ".amount", t.getAmount());
            if (t.getId() == null || t.getAmount() == null) {
                return;
            }
            v.check(ids.add(t.getId()), q + " duplicate id");
            v.check(t.getAmount() > 0, q + ".amount must be > 0");
            sum += t.getAmount();
        }
        v.check(sum == p.getTotalAmount(), "Σ transaction amount " + sum + " != totalAmount " + p.getTotalAmount());
    }
}
