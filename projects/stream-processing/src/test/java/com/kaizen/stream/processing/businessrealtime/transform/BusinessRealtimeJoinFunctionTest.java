package com.kaizen.stream.processing.businessrealtime.transform;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.model.payment.PaymentStatus;
import com.kaizen.stream.model.realtime.RealtimeBusinessByOrderRow;
import com.kaizen.stream.processing.businessrealtime.lookup.ShopNames;
import com.kaizen.stream.testing.Fixtures;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BusinessRealtimeJoinFunctionTest {

    private static final String RET1 = "RET-" + Fixtures.ORDER_ID + "-1";

    private KeyedTwoInputStreamOperatorTestHarness<String, Envelope<OrderSnapshot>, Envelope<PaymentSnapshot>,
            RealtimeBusinessByOrderRow> harness;

    @BeforeEach
    void open() throws Exception {
        harness = new KeyedTwoInputStreamOperatorTestHarness<>(
                new KeyedCoProcessOperator<>(new BusinessRealtimeJoinFunction(16, new FixedShopNames())),
                e -> e.getData().getOrder().getId(),
                e -> e.getData().getPayment().getOrderId(),
                Types.STRING);
        harness.open();
        harness.setProcessingTime(Fixtures.T0 + 60_000);
    }

    @AfterEach
    void close() throws Exception {
        harness.close();
    }

    @Test
    void orderThenReceiptMovesTheSaleFromPendingToPaid() throws Exception {
        order(Fixtures.placedOrder());
        payment(Fixtures.successReceipt());

        List<RealtimeBusinessByOrderRow> rows = drain();
        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getPendingOrder());
        assertEquals(0, rows.get(1).getPendingOrder());
        assertEquals(305_000L, rows.get(1).getTotalReceipts());
        assertEquals("Nhà thuốc Quận 1", rows.get(1).getShopName());
        assertTrue(rows.get(1).getVersion() > rows.get(0).getVersion());
    }

    @Test
    void paymentBeforeOrderWaitsForTheOrder() throws Exception {
        payment(Fixtures.successReceipt());
        assertTrue(drain().isEmpty());

        order(Fixtures.placedOrder());
        List<RealtimeBusinessByOrderRow> rows = drain();
        assertEquals(1, rows.size());
        assertEquals(0, rows.get(0).getPendingOrder());
    }

    @Test
    void staleAndReplayedEventsEmitNothing() throws Exception {
        order(Fixtures.partiallyReturnedOrder());
        payment(Fixtures.successReceipt());
        drain();

        order(Fixtures.placedOrder());
        order(Fixtures.partiallyReturnedOrder());
        payment(Fixtures.successReceipt());

        assertTrue(drain().isEmpty());
    }

    @Test
    void refundUpdatesOnlyItsReturnRow() throws Exception {
        order(Fixtures.partiallyReturnedOrder());
        payment(Fixtures.successReceipt());
        drain();

        payment(Fixtures.successRefund(RET1, 108_000L, Fixtures.T_RETURN + 7_000));

        List<RealtimeBusinessByOrderRow> rows = drain();
        assertEquals(1, rows.size());
        assertEquals(RET1, rows.get(0).getReturnId());
        assertEquals(0, rows.get(0).getPendingOrder());
        assertEquals(0L, rows.get(0).getReconciliationDiff());
    }

    @Test
    void unsuccessfulPaymentsAreIgnored() throws Exception {
        order(Fixtures.placedOrder());
        drain();
        PaymentSnapshot pending = Fixtures.successReceipt();
        pending.getPayment().setStatus(PaymentStatus.PENDING);

        payment(pending);

        assertTrue(drain().isEmpty());
    }

    @Test
    void rowsPastTheStateWindowAreFrozen() throws Exception {
        harness.setProcessingTime(Fixtures.T0 + 17 * 86_400_000L);

        order(Fixtures.placedOrder());

        assertTrue(drain().isEmpty());
    }

    private void order(OrderSnapshot s) throws Exception {
        harness.processElement1(new StreamRecord<>(Fixtures.orderEnvelope(s)));
    }

    private void payment(PaymentSnapshot s) throws Exception {
        harness.processElement2(new StreamRecord<>(Fixtures.paymentEnvelope(s)));
    }

    private List<RealtimeBusinessByOrderRow> drain() {
        List<RealtimeBusinessByOrderRow> out = harness.extractOutputValues();
        harness.getOutput().clear();
        return out;
    }

    /** Static so the function stays serializable. / Static để function vẫn serializable. */
    static final class FixedShopNames extends ShopNames {
        FixedShopNames() {
            super(null, 600_000L);
        }

        @Override
        protected Map<String, String> load() {
            return Map.of("SHOP0001", "Nhà thuốc Quận 1");
        }
    }
}
