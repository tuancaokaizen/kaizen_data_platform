package com.kaizen.stream.processing.rule;

import com.kaizen.stream.model.inventory.StockMovementEvent;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class InventoryRulesTest {

    static StockMovementEvent sale() {
        return StockMovementEvent.builder()
                .movementId("SM-ORDER-L1-SALE-1").seq(42L).locationType("SHOP").locationCode("SHOP0001")
                .productId("PROD0001").lotId("LOT-OPEN-PROD0001").movementType("SALE").qty(-2).onHandAfter(5)
                .unitCost(9_000L).refType("ORDER").refId("O1").refLineId("L1").occurredAt(1_790_000_000_000L)
                .build();
    }

    private static List<String> check(StockMovementEvent e) {
        return InventoryRules.violations(e.getMovementType(), e.getSeq(),
                InventoryRules.positionKey(e.getLocationCode(), e.getProductId()), e);
    }

    @Test
    void validSalePasses() {
        assertTrue(check(sale()).isEmpty());
    }

    @Test
    void signFollowsTheMovementType() {
        assertTrue(check(sale().toBuilder().qty(2).build()).get(0).contains("wrong sign"));
        assertTrue(check(sale().toBuilder().movementType("RECEIPT").qty(-1).build()).get(0).contains("wrong sign"));
        assertTrue(check(sale().toBuilder().movementType("ADJUST").qty(3).reason("OVERSELL").build()).isEmpty());
    }

    @Test
    void reasonOnlyOnAdjust() {
        assertTrue(check(sale().toBuilder().movementType("ADJUST").qty(3).build()).get(0).contains("reason"));
        assertTrue(check(sale().toBuilder().reason("OVERSELL").build()).get(0).contains("reason"));
    }

    @Test
    void negativeStockAndUnknownValuesAreRejected() {
        assertTrue(check(sale().toBuilder().onHandAfter(-1).build()).get(0).contains("onHandAfter"));
        assertTrue(check(sale().toBuilder().locationType("STORE").build()).get(0).contains("locationType"));
        assertTrue(check(sale().toBuilder().movementType("LOST").build()).get(0).contains("unknown movementType"));
    }

    @Test
    void envelopeMustMatchThePayload() {
        StockMovementEvent e = sale();
        assertTrue(InventoryRules.violations("SALE", 41L, "SHOP0001:PROD0001", e).get(0).contains("meta.version"));
        assertTrue(InventoryRules.violations("RECEIPT", 42L, "SHOP0001:PROD0001", e).get(0).contains("meta.eventType"));
        assertTrue(InventoryRules.violations("SALE", 42L, "SHOP0002:PROD0001", e).get(0).contains("aggregateId"));
    }

    @Test
    void missingFieldsStopEarly() {
        List<String> v = check(sale().toBuilder().lotId(null).qty(null).build());
        assertTrue(v.contains("lotId is missing") && v.contains("qty is missing"));
    }
}
