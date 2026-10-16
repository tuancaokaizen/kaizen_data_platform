package com.kaizen.stream.simulation.payment.transform;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.payment.PaymentHeader;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.model.payment.PaymentStatus;
import com.kaizen.stream.model.payment.PaymentType;
import com.kaizen.stream.processing.rule.PaymentRules;
import com.kaizen.stream.testing.Fixtures;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaymentSchedulerFunctionTest {

    private static final long DAY = 86_400_000L;

    private final PaymentDecider decider = new PaymentDecider(PaymentDeciderTest.defaults());

    private KeyedOneInputStreamOperatorTestHarness<String, Envelope<OrderSnapshot>, Envelope<PaymentSnapshot>> harness;

    @BeforeEach
    void open() throws Exception {
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new PaymentSchedulerFunction(decider, 16, "flink.simulation-payment")),
                e -> e.getData().getOrder().getId(),
                Types.STRING);
        harness.open();
        harness.setProcessingTime(Fixtures.T0);
    }

    @AfterEach
    void close() throws Exception {
        harness.close();
    }

    @Test
    void receiptEventsAreEmittedWhenTheirTimeComes() throws Exception {
        order(Fixtures.placedOrder());
        assertTrue(drain().isEmpty(), "the first attempt is 2–10 s after the sale");

        harness.setProcessingTime(Fixtures.T0 + DAY);

        List<Envelope<PaymentSnapshot>> out = drain();
        assertEquals(decider.receipts(Fixtures.placedOrder().getOrder()), payloads(out));
        for (Envelope<PaymentSnapshot> e : out) {
            assertEquals(Fixtures.ORDER_ID, e.key());
            assertEquals("flink.simulation-payment", e.getMeta().getSource());
            assertEquals(List.of(), PaymentRules.violations(e.getMeta().getEventType(), e.getMeta().getVersion(),
                    e.getData()));
        }
    }

    @Test
    void replayedOrdersScheduleNothingNew() throws Exception {
        order(Fixtures.placedOrder());
        order(Fixtures.partiallyReturnedOrder());
        harness.setProcessingTime(Fixtures.T0 + 2 * DAY);
        int first = drain().size();

        order(Fixtures.placedOrder());
        order(Fixtures.partiallyReturnedOrder());
        harness.setProcessingTime(Fixtures.T0 + 3 * DAY);

        assertTrue(first >= 2);
        assertTrue(drain().isEmpty());
    }

    @Test
    void returnedOrderGetsOneRefundAfterTheReceipt() throws Exception {
        order(Fixtures.placedOrder());
        harness.setProcessingTime(Fixtures.T0 + DAY / 2);
        List<PaymentSnapshot> receipts = payloads(drain());
        PaymentSnapshot success = receipts.get(receipts.size() - 1);

        order(Fixtures.partiallyReturnedOrder());
        harness.setProcessingTime(Fixtures.T_RETURN + 60_000);

        List<PaymentSnapshot> refunds = payloads(drain());
        assertEquals(1, refunds.size());
        PaymentHeader refund = refunds.get(0).getPayment();
        assertEquals(PaymentType.REFUND, refund.getType());
        assertEquals(108_000L, refund.getTotalAmount());
        assertEquals(PaymentDecider.mainMethod(success), refunds.get(0).getTransactions().get(0).getPaymentMethodId());
    }

    @Test
    void returnArrivingFirstStillWaitsForTheReceipt() throws Exception {
        // Return dated before the receipt settles / Phiếu trả có thời điểm trước lúc phiếu thu xong
        OrderSnapshot early = Fixtures.partiallyReturnedOrder();
        early.getReturns().get(0).setCreatedDate(Fixtures.T0 + 1);

        order(early);
        harness.setProcessingTime(Fixtures.T0 + DAY);

        List<PaymentSnapshot> out = payloads(drain());
        PaymentSnapshot last = out.get(out.size() - 1);
        PaymentSnapshot success = out.stream()
                .filter(s -> s.getPayment().getType() == PaymentType.RECEIPT
                        && s.getPayment().getStatus() == PaymentStatus.SUCCESS)
                .findFirst().orElseThrow();
        assertEquals(PaymentType.REFUND, last.getPayment().getType());
        assertTrue(last.getPayment().getModifiedDate() >= success.getPayment().getModifiedDate());
    }

    @Test
    void stateSurvivesASnapshotRestore() throws Exception {
        order(Fixtures.placedOrder());
        var snapshot = harness.snapshot(1L, Fixtures.T0);
        harness.close();

        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new PaymentSchedulerFunction(decider, 16, "flink.simulation-payment")),
                e -> e.getData().getOrder().getId(),
                Types.STRING);
        harness.setup();
        harness.initializeState(snapshot);
        harness.open();
        harness.setProcessingTime(Fixtures.T0 + DAY);

        assertEquals(decider.receipts(Fixtures.placedOrder().getOrder()), payloads(drain()));
        order(Fixtures.placedOrder());
        harness.setProcessingTime(Fixtures.T0 + 2 * DAY);
        assertTrue(drain().isEmpty());
    }

    private void order(OrderSnapshot s) throws Exception {
        harness.processElement(new StreamRecord<>(Fixtures.orderEnvelope(s)));
    }

    private List<Envelope<PaymentSnapshot>> drain() {
        List<Envelope<PaymentSnapshot>> out = harness.extractOutputValues();
        harness.getOutput().clear();
        return out;
    }

    private static List<PaymentSnapshot> payloads(List<Envelope<PaymentSnapshot>> envelopes) {
        return envelopes.stream().map(Envelope::getData).collect(Collectors.toList());
    }
}
