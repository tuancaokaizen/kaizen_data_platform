package com.kaizen.stream.processing.rule;

import com.kaizen.stream.model.payment.PaymentEventType;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.model.payment.PaymentStatus;
import com.kaizen.stream.model.payment.PaymentType;
import com.kaizen.stream.testing.Fixtures;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaymentRulesTest {

    private static List<String> check(String eventType, PaymentSnapshot s) {
        return PaymentRules.violations(eventType, s.getPayment().getModifiedDate(), s);
    }

    private static void assertViolation(String eventType, PaymentSnapshot s, String fragment) {
        List<String> v = check(eventType, s);
        assertTrue(v.stream().anyMatch(m -> m.contains(fragment)), "expected '" + fragment + "' in " + v);
    }

    @Test
    void successReceiptIsValid() {
        assertEquals(List.of(), check("PAYMENT_SUCCEEDED", Fixtures.successReceipt()));
    }

    @Test
    void pendingMayHaveNoTransactions() {
        PaymentSnapshot s = Fixtures.successReceipt();
        s.getPayment().setStatus(PaymentStatus.PENDING);
        s.setTransactions(new ArrayList<>());
        assertEquals(List.of(), check("PAYMENT_PENDING", s));
    }

    @Test
    void successNeedsFullSplit() {
        PaymentSnapshot s = Fixtures.successReceipt();
        s.getTransactions().remove(1);
        assertViolation("PAYMENT_SUCCEEDED", s, "Σ transaction amount 205000 != totalAmount 305000");

        PaymentSnapshot empty = Fixtures.successReceipt();
        empty.setTransactions(new ArrayList<>());
        assertViolation("PAYMENT_SUCCEEDED", empty, "transactions is empty");
    }

    @Test
    void eventTypeMustMatchTypeAndStatus() {
        assertViolation("PAYMENT_PENDING", Fixtures.successReceipt(), "payment implies PAYMENT_SUCCEEDED");
    }

    @Test
    void refundRules() {
        PaymentSnapshot refund = Fixtures.successReceipt();
        refund.getPayment().setType(PaymentType.REFUND);
        assertViolation("REFUND_SUCCEEDED", refund, "REFUND needs returnId");

        refund.getPayment().setReturnId("RET-1");
        assertEquals(List.of(), check("REFUND_SUCCEEDED", refund));

        refund.getPayment().setStatus(PaymentStatus.PENDING);
        assertViolation("REFUND_SUCCEEDED", refund, "Refunds are only emitted as SUCCESS");

        PaymentSnapshot receipt = Fixtures.successReceipt();
        receipt.getPayment().setReturnId("RET-1");
        assertViolation("PAYMENT_SUCCEEDED", receipt, "RECEIPT must not have returnId");
    }

    @Test
    void versionAndAmounts() {
        PaymentSnapshot s = Fixtures.successReceipt();
        assertTrue(PaymentRules.violations("PAYMENT_SUCCEEDED", 1L, s).stream()
                .anyMatch(m -> m.contains("meta.version 1")));
        s.getPayment().setAttempt(0);
        assertViolation("PAYMENT_SUCCEEDED", s, "attempt must be >= 1");
    }

    @Test
    void eventTypeMapping() {
        assertEquals(PaymentEventType.PAYMENT_FAILED, PaymentEventType.of(PaymentType.RECEIPT, PaymentStatus.FAILED));
        assertEquals(PaymentEventType.REFUND_SUCCEEDED, PaymentEventType.of(PaymentType.REFUND, PaymentStatus.SUCCESS));
        assertThrows(IllegalArgumentException.class, () -> PaymentEventType.of(PaymentType.REFUND, PaymentStatus.FAILED));
        assertEquals(2, PaymentType.REFUND.code());
    }
}
