package com.kaizen.stream.processing.businessrealtime.transform;

import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.order.ReturnDocument;
import com.kaizen.stream.model.realtime.RealtimeBusinessByOrderRow;
import com.kaizen.stream.processing.businessrealtime.transform.state.BusinessRealtimeState.Order;
import com.kaizen.stream.processing.businessrealtime.transform.state.BusinessRealtimeState.Payment;
import com.kaizen.stream.testing.Fixtures;
import com.kaizen.stream.utils.time.IctTime;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BusinessRealtimeRowBuilderTest {

    private static final long NOW = Fixtures.T0 + 3 * 86_400_000L;
    private static final LocalDate OPEN = LocalDate.of(2000, 1, 1);
    private static final String RET1 = "RET-" + Fixtures.ORDER_ID + "-1";
    private static final String RET2 = "RET-" + Fixtures.ORDER_ID + "-2";
    private static final long T_RETURN2 = Fixtures.T_RETURN + 86_400_000L;

    private static final Payment RECEIPT = Payment.of(Fixtures.successReceipt(), Fixtures.T0 + 9_000);
    private static final Payment REFUND1 =
            Payment.of(Fixtures.successRefund(RET1, 108_000L, Fixtures.T_RETURN + 7_000), Fixtures.T_RETURN + 7_000);

    @Test
    void saleIsPendingUntilTheReceiptSucceeds() {
        Order placed = Order.of(Fixtures.placedOrder(), Fixtures.T0);

        RealtimeBusinessByOrderRow pending = single(BusinessRealtimeRowBuilder.all(placed, Collections.emptyList(), "Shop 1", OPEN, NOW));
        assertEquals(1, pending.getPendingOrder());
        assertEquals(RealtimeBusinessByOrderRow.SALE, pending.getDocumentType());
        assertEquals("", pending.getReturnId());
        assertEquals(305_000L, pending.getSalesAmount());
        assertEquals(0L, pending.getTotalReceipts());
        assertEquals(-305_000L, pending.getReconciliationDiff());
        assertEquals(Fixtures.T0, pending.getVersion());
        assertEquals(IctTime.businessDate(Fixtures.T0), pending.getReportDate());

        RealtimeBusinessByOrderRow paid = single(BusinessRealtimeRowBuilder.all(placed, List.of(RECEIPT), "Shop 1", OPEN, NOW));
        assertEquals(0, paid.getPendingOrder());
        assertEquals(305_000L, paid.getTotalReceipts());
        assertEquals(305_000L, paid.getNetCashFlow());
        assertEquals(305_000L, paid.getNetRevenue());
        assertEquals(0L, paid.getReconciliationDiff());
        assertEquals("TienMat:205000|ViDienTu:100000", paid.getPaymentMethodSummary());
        assertEquals(Fixtures.T0 + 9_000, paid.getVersion());
        assertEquals("Shop 1", paid.getShopName());
        assertEquals("Completed", paid.getOrderStatus());
    }

    @Test
    void everyReturnGetsItsOwnRow() {
        Order order = Order.of(twoReturns(), T_RETURN2);
        List<RealtimeBusinessByOrderRow> rows = BusinessRealtimeRowBuilder.all(order, List.of(RECEIPT, REFUND1), "", OPEN, NOW);

        assertEquals(3, rows.size());
        RealtimeBusinessByOrderRow sale = rows.get(0);
        assertEquals("PartiallyReturned", sale.getOrderStatus());
        assertEquals(T_RETURN2, sale.getVersion());

        RealtimeBusinessByOrderRow r1 = rows.get(1);
        assertEquals(RET1, r1.getReturnId());
        assertEquals(RealtimeBusinessByOrderRow.RETURN, r1.getDocumentType());
        assertEquals(108_000L, r1.getReturnAmount());
        assertEquals(-108_000L, r1.getNetRevenue());
        assertEquals(108_000L, r1.getTotalExpenditures());
        assertEquals(-108_000L, r1.getNetCashFlow());
        assertEquals(0L, r1.getReconciliationDiff());
        assertEquals(0, r1.getPendingOrder());
        assertEquals("The:108000", r1.getPaymentMethodSummary());
        assertEquals(IctTime.businessDate(Fixtures.T_RETURN), r1.getReportDate());

        RealtimeBusinessByOrderRow r2 = rows.get(2);
        assertEquals(RET2, r2.getReturnId());
        assertEquals(1, r2.getPendingOrder());
        assertEquals(89_000L, r2.getReconciliationDiff());
        assertEquals(IctTime.businessDate(T_RETURN2), r2.getReportDate());
    }

    @Test
    void returnedOrderWithoutReceiptInStateSkipsTheSaleRow() {
        Order order = Order.of(Fixtures.partiallyReturnedOrder(), Fixtures.T_RETURN);
        List<RealtimeBusinessByOrderRow> rows = BusinessRealtimeRowBuilder.all(order, Collections.emptyList(), "", OPEN, NOW);

        assertEquals(1, rows.size());
        assertEquals(RET1, rows.get(0).getReturnId());
        assertEquals(1, rows.get(0).getPendingOrder());
    }

    @Test
    void rowsOlderThanTheFreezeDateAreNotEmitted() {
        Order order = Order.of(Fixtures.partiallyReturnedOrder(), Fixtures.T_RETURN);
        // Sale day frozen, return day still open / Ngày bán đã đóng băng, ngày trả còn mở
        LocalDate frozenBefore = IctTime.businessDate(Fixtures.T_RETURN);

        List<RealtimeBusinessByOrderRow> rows = BusinessRealtimeRowBuilder.all(order, List.of(RECEIPT, REFUND1), "", frozenBefore, NOW);

        assertEquals(1, rows.size());
        assertEquals(RET1, rows.get(0).getReturnId());
    }

    @Test
    void grainSelectsTheRowSettledByOnePayment() {
        Order order = Order.of(twoReturns(), T_RETURN2);
        List<Payment> payments = List.of(RECEIPT, REFUND1);

        assertEquals(RET1, BusinessRealtimeRowBuilder.grain(order, payments, RET1, "", OPEN, NOW).get().getReturnId());
        assertEquals("", BusinessRealtimeRowBuilder.grain(order, payments, "", "", OPEN, NOW).get().getReturnId());
        assertTrue(BusinessRealtimeRowBuilder.grain(order, payments, "RET-UNKNOWN", "", OPEN, NOW).isEmpty());
    }

    @Test
    void duplicateReceiptsSurfaceAsReconciliationDiff() {
        Order placed = Order.of(Fixtures.placedOrder(), Fixtures.T0);
        Payment second = Payment.of(Fixtures.successReceipt(), Fixtures.T0 + 20_000);
        second.setPaymentId("PAY-DUP");

        RealtimeBusinessByOrderRow row = single(BusinessRealtimeRowBuilder.all(placed, List.of(RECEIPT, second), "", OPEN, NOW));

        assertEquals(610_000L, row.getTotalReceipts());
        assertEquals(305_000L, row.getReconciliationDiff());
        assertEquals("TienMat:410000|ViDienTu:200000", row.getPaymentMethodSummary());
    }

    @Test
    void versionIsTheNewestInput() {
        Order order = Order.of(Fixtures.partiallyReturnedOrder(), Fixtures.T_RETURN);
        Payment late = Payment.of(Fixtures.successRefund(RET1, 108_000L, T_RETURN2), T_RETURN2);

        assertEquals(Fixtures.T_RETURN, BusinessRealtimeRowBuilder.version(order, List.of(RECEIPT)));
        assertEquals(T_RETURN2, BusinessRealtimeRowBuilder.version(order, Arrays.asList(RECEIPT, late)));
    }

    /** Return 1 (108,000) and return 2 (89,000 of line 2). / Phiếu trả 1 và phiếu trả 2. */
    static OrderSnapshot twoReturns() {
        OrderSnapshot s = Fixtures.partiallyReturnedOrder();
        s.getReturns().add(ReturnDocument.builder().id(RET2).createdDate(T_RETURN2).reason("DEFECT")
                .returnAmount(89_000L).build());
        s.getOrder().setModifiedDate(T_RETURN2);
        return s;
    }

    private static RealtimeBusinessByOrderRow single(List<RealtimeBusinessByOrderRow> rows) {
        assertEquals(1, rows.size(), rows.toString());
        return rows.get(0);
    }
}
