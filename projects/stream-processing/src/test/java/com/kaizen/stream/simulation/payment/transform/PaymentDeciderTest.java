package com.kaizen.stream.simulation.payment.transform;

import com.kaizen.stream.config.RawSettings;
import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.model.order.OrderHeader;
import com.kaizen.stream.model.order.ReturnDocument;
import com.kaizen.stream.model.payment.PaymentEventType;
import com.kaizen.stream.model.payment.PaymentHeader;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.model.payment.PaymentStatus;
import com.kaizen.stream.model.payment.PaymentTransaction;
import com.kaizen.stream.processing.rule.PaymentRules;
import com.kaizen.stream.testing.Fixtures;
import com.kaizen.stream.utils.time.IctTime;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PaymentDeciderTest {

    private static final int ORDERS = 10_000;

    private final PaymentDecider decider = new PaymentDecider(defaults());

    @Test
    void sameOrderAlwaysGivesTheSameTimeline() {
        OrderHeader order = Fixtures.placedOrder().getOrder();

        assertEquals(decider.receipts(order), decider.receipts(order));
        assertEquals(decider.receipts(order), new PaymentDecider(defaults()).receipts(order));
    }

    @Test
    void everyTimelineEndsWithTheOnlySuccessForTheFullAmount() {
        for (int i = 0; i < ORDERS; i++) {
            OrderHeader order = order(i, Fixtures.T0);
            List<PaymentSnapshot> timeline = decider.receipts(order);

            PaymentSnapshot last = timeline.get(timeline.size() - 1);
            assertEquals(PaymentStatus.SUCCESS, last.getPayment().getStatus());
            assertEquals(1, timeline.stream().filter(s -> s.getPayment().getStatus() == PaymentStatus.SUCCESS).count());
            assertTrue(last.getPayment().getAttempt() <= 3);
            long previous = order.getTransactionDate();
            for (PaymentSnapshot s : timeline) {
                PaymentHeader p = s.getPayment();
                String type = PaymentEventType.of(p.getType(), p.getStatus()).name();
                assertEquals(List.of(), PaymentRules.violations(type, p.getModifiedDate(), s), p.getId());
                assertTrue(p.getModifiedDate() > previous, "events must move forward in time");
                previous = p.getModifiedDate();
            }
        }
    }

    @Test
    void firstAttemptOutcomesFollowTheProfile() {
        Map<PaymentStatus, Integer> first = new EnumMap<>(PaymentStatus.class);
        for (int i = 0; i < ORDERS; i++) {
            PaymentStatus s = decider.receipts(order(i, Fixtures.T0)).get(0).getPayment().getStatus();
            first.merge(s, 1, Integer::sum);
        }

        assertEquals(0.92, share(first.get(PaymentStatus.SUCCESS)), 0.01);
        assertEquals(0.05, share(first.get(PaymentStatus.FAILED)), 0.01);
        assertEquals(0.03, share(first.get(PaymentStatus.PENDING)), 0.01);
    }

    @Test
    void pendingSettlesOnTheSamePaymentAndFailuresRetryWithANewOne() {
        boolean sawPending = false;
        boolean sawRetry = false;
        for (int i = 0; i < 2_000; i++) {
            List<PaymentSnapshot> timeline = decider.receipts(order(i, Fixtures.T0));
            for (int k = 0; k < timeline.size() - 1; k++) {
                PaymentHeader p = timeline.get(k).getPayment();
                PaymentHeader next = timeline.get(k + 1).getPayment();
                if (p.getStatus() == PaymentStatus.PENDING) {
                    sawPending = true;
                    assertEquals(p.getId(), next.getId());
                    assertEquals(p.getCreatedDate(), next.getCreatedDate());
                    long wait = next.getModifiedDate() - p.getModifiedDate();
                    assertTrue(wait >= 600_000 && wait <= 1_800_000, "pending wait " + wait);
                } else {
                    sawRetry = true;
                    assertEquals(PaymentStatus.FAILED, p.getStatus());
                    assertEquals(p.getAttempt() + 1, next.getAttempt());
                    long wait = next.getCreatedDate() - p.getModifiedDate();
                    assertTrue(wait >= 60_000 && wait <= 300_000, "retry wait " + wait);
                }
            }
        }
        assertTrue(sawPending && sawRetry);
    }

    @Test
    void walletShareFollowsTheE7Ramp() {
        assertEquals(0.15, walletAmountShare(LocalDate.of(2024, 1, 1)), 0.02);
        assertEquals(0.45, walletAmountShare(LocalDate.of(2026, 10, 11)), 0.02);
        assertEquals(0.30, decider.walletShare(LocalDate.of(2025, 5, 16)), 0.01);
    }

    @Test
    void splitPaymentsUseTwoMethodsInWholeThousands() {
        int split = 0;
        for (int i = 0; i < ORDERS; i++) {
            List<PaymentSnapshot> timeline = decider.receipts(order(i, Fixtures.T0));
            List<PaymentTransaction> txns = timeline.get(timeline.size() - 1).getTransactions();
            if (txns.size() == 2) {
                split++;
                assertTrue(!txns.get(0).getPaymentMethodId().equals(txns.get(1).getPaymentMethodId()));
                assertEquals(0, txns.get(0).getAmount() % PaymentDecider.SPLIT_UNIT);
            }
        }
        assertEquals(0.15, split / (double) ORDERS, 0.015);
    }

    @Test
    void refundWaitsForTheReceiptAndGoesBackToItsMethod() {
        OrderHeader order = Fixtures.placedOrder().getOrder();
        ReturnDocument ret = Fixtures.partiallyReturnedOrder().getReturns().get(0);

        PaymentSnapshot early = decider.refund(order, ret, PaymentDecider.WALLET, 0L).orElseThrow();
        long receiptLater = ret.getCreatedDate() + 3_600_000L;
        PaymentSnapshot late = decider.refund(order, ret, PaymentDecider.WALLET, receiptLater).orElseThrow();

        long delay = early.getPayment().getModifiedDate() - ret.getCreatedDate();
        assertTrue(delay >= 2_000 && delay <= 10_000);
        assertEquals(receiptLater, late.getPayment().getModifiedDate());
        assertEquals("REF-" + ret.getId(), early.getPayment().getId());
        assertEquals(PaymentDecider.WALLET, early.getTransactions().get(0).getPaymentMethodId());
        assertEquals(List.of(), PaymentRules.violations("REFUND_SUCCEEDED", early.getPayment().getModifiedDate(), early));
    }

    @Test
    void nothingIsPaidForZeroAmounts() {
        OrderHeader free = order(1, Fixtures.T0).toBuilder().totalPayment(0L).build();
        ReturnDocument giftOnly = ReturnDocument.builder().id("RET-GIFT").createdDate(Fixtures.T_RETURN).returnAmount(0L).build();

        assertTrue(decider.receipts(free).isEmpty());
        assertEquals(Optional.empty(), decider.refund(free, giftOnly, PaymentDecider.CASH, 0L));
    }

    @Test
    void mainMethodIsTheLargestShare() {
        PaymentSnapshot s = Fixtures.successReceipt();
        assertEquals(1, PaymentDecider.mainMethod(s));
        s.getTransactions().get(1).setAmount(205_000L);
        assertEquals(1, PaymentDecider.mainMethod(s), "ties go to the lower method id");
    }

    @Test
    void idsAreUniquePerAttempt() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 2_000; i++) {
            for (PaymentSnapshot s : decider.receipts(order(i, Fixtures.T0))) {
                ids.add(s.getPayment().getId() + "|" + s.getPayment().getStatus());
                for (PaymentTransaction t : s.getTransactions()) {
                    assertTrue(t.getId().startsWith(s.getPayment().getId() + "-"));
                }
            }
        }
        assertTrue(ids.size() >= 2_000);
    }

    private double walletAmountShare(LocalDate date) {
        long wallet = 0;
        long total = 0;
        long t = IctTime.startOfDay(date) + 10 * 3_600_000L;
        for (int i = 0; i < ORDERS; i++) {
            List<PaymentSnapshot> timeline = decider.receipts(order(i, t));
            for (PaymentTransaction txn : timeline.get(timeline.size() - 1).getTransactions()) {
                total += txn.getAmount();
                wallet += txn.getPaymentMethodId() == PaymentDecider.WALLET ? txn.getAmount() : 0;
            }
        }
        return wallet / (double) total;
    }

    private static double share(Integer count) {
        return (count == null ? 0 : count) / (double) ORDERS;
    }

    private static OrderHeader order(int i, long t) {
        return Fixtures.placedOrder().getOrder().toBuilder()
                .id(String.format("SORD-20261011-SHOP%04d-%05d", i % 50, i))
                .transactionDate(t).createdDate(t).modifiedDate(t)
                .totalPayment(50_000L + (i % 97) * 3_500L)
                .build();
    }

    public static StreamSettings.Payment defaults() {
        return new StreamSettings(new RawSettings(new HashMap<>())).payment();
    }
}
