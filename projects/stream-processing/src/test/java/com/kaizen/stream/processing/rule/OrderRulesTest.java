package com.kaizen.stream.processing.rule;

import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.order.OrderStatus;
import com.kaizen.stream.model.order.ReturnDocument;
import com.kaizen.stream.model.order.ReturnLine;
import com.kaizen.stream.testing.Fixtures;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderRulesTest {

    private static List<String> check(OrderSnapshot s) {
        String type = s.getReturns().isEmpty() ? "ORDER_PLACED" : "ORDER_RETURNED";
        return OrderRules.violations(type, s.getOrder().getModifiedDate(), s);
    }

    private static void assertViolation(OrderSnapshot s, String fragment) {
        List<String> v = check(s);
        assertTrue(v.stream().anyMatch(m -> m.contains(fragment)), "expected '" + fragment + "' in " + v);
    }

    @Test
    void fixturesAreValid() {
        assertEquals(List.of(), check(Fixtures.placedOrder()));
        assertEquals(List.of(), check(Fixtures.partiallyReturnedOrder()));
    }

    @Test
    void fullReturnMeansReturned() {
        OrderSnapshot s = Fixtures.partiallyReturnedOrder();
        ReturnDocument second = s.getReturns().get(0).toBuilder().id("RET-2").returnAmount(197_000L)
                .items(new java.util.ArrayList<>(List.of(
                        ReturnLine.builder().id("RET-2-L1").orderItemId(Fixtures.ORDER_ID + "-L1").productId("P0012")
                                .quantity(1).restockQty(0).damageQty(1).unitPrice(120_000L).unitCost(80_000L)
                                .lineAmount(108_000L).build(),
                        ReturnLine.builder().id("RET-2-L2").orderItemId(Fixtures.ORDER_ID + "-L2").productId("P0040")
                                .quantity(1).restockQty(1).damageQty(0).unitPrice(100_000L).unitCost(60_000L)
                                .lineAmount(89_000L).build())))
                .build();
        s.getReturns().add(second);

        assertViolation(s, "returns imply Returned");
        s.getOrder().setStatus(OrderStatus.RETURNED);
        assertEquals(List.of(), check(s));
    }

    @Test
    void statusMustFollowReturns() {
        OrderSnapshot placed = Fixtures.placedOrder();
        placed.getOrder().setStatus(OrderStatus.RETURNED);
        assertViolation(placed, "returns imply Completed");

        OrderSnapshot partial = Fixtures.partiallyReturnedOrder();
        partial.getOrder().setStatus(OrderStatus.COMPLETED);
        assertViolation(partial, "returns imply PartiallyReturned");
    }

    @Test
    void moneyMustAddUp() {
        OrderSnapshot s = Fixtures.placedOrder();
        s.getItems().get(0).setLineAmount(216_001L);
        assertViolation(s, "lineAmount != quantity × unitPrice − discountAmount");
        assertViolation(s, "Σ lineAmount 305001 != totalPayment 305000");

        OrderSnapshot t = Fixtures.placedOrder();
        t.getOrder().setTotalDiscount(30_000L);
        assertViolation(t, "Σ discountAmount 35000 != totalDiscount 30000");
    }

    @Test
    void giftLinesAreFree() {
        OrderSnapshot s = Fixtures.placedOrder();
        s.getItems().get(2).setUnitPrice(5_000L);
        assertViolation(s, "gift line must have unitPrice 0");
    }

    @Test
    void cannotReturnMoreThanSold() {
        OrderSnapshot s = Fixtures.partiallyReturnedOrder();
        ReturnLine rl = s.getReturns().get(0).getItems().get(0);
        rl.setQuantity(3);
        rl.setRestockQty(3);
        assertViolation(s, "returned quantity 3 > sold quantity");
    }

    @Test
    void returnLinesMustPointToSoldLines() {
        OrderSnapshot s = Fixtures.partiallyReturnedOrder();
        s.getReturns().get(0).getItems().get(0).setOrderItemId("NOPE");
        assertViolation(s, "unknown orderItemId NOPE");
    }

    @Test
    void restockPlusDamageEqualsQuantity() {
        OrderSnapshot s = Fixtures.partiallyReturnedOrder();
        s.getReturns().get(0).getItems().get(0).setDamageQty(1);
        assertViolation(s, "restockQty + damageQty != quantity");
    }

    @Test
    void eventTypeAndVersionMustMatchSnapshot() {
        OrderSnapshot placed = Fixtures.placedOrder();
        assertTrue(OrderRules.violations("ORDER_RETURNED", Fixtures.T0, placed).stream()
                .anyMatch(m -> m.contains("ORDER_RETURNED needs at least one return")));
        assertTrue(OrderRules.violations("ORDER_PLACED", Fixtures.T0 + 1, placed).stream()
                .anyMatch(m -> m.contains("meta.version")));
        assertTrue(OrderRules.violations("ORDER_CANCELLED", Fixtures.T0, placed).stream()
                .anyMatch(m -> m.contains("unknown order eventType")));
    }

    @Test
    void missingRequiredFieldsStopEarly() {
        OrderSnapshot s = Fixtures.placedOrder();
        s.getOrder().setShopCode(null);
        s.getOrder().setTotalPayment(null);
        List<String> v = check(s);
        assertEquals(List.of("order.shopCode is missing", "order.totalPayment is missing"), v);

        OrderSnapshot noOrder = new OrderSnapshot();
        assertEquals(List.of("order is missing"), OrderRules.violations("ORDER_PLACED", 1L, noOrder));
    }

    @Test
    void statusOfMatchesProperty4() {
        assertEquals(OrderStatus.COMPLETED, OrderStatus.of(100, 0, 0));
        assertEquals(OrderStatus.PARTIALLY_RETURNED, OrderStatus.of(100, 1, 99));
        assertEquals(OrderStatus.RETURNED, OrderStatus.of(100, 2, 100));
    }
}
