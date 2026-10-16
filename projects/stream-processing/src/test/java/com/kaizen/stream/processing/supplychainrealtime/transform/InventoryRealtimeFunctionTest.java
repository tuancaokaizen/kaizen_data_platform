package com.kaizen.stream.processing.supplychainrealtime.transform;

import com.kaizen.stream.model.inventory.StockMovementEvent;
import com.kaizen.stream.model.realtime.RealtimeInventoryPositionRow;
import com.kaizen.stream.utils.time.IctTime;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InventoryRealtimeFunctionTest {

    private static final LocalDate DAY = LocalDate.of(2026, 10, 12);
    private static final long NOON = IctTime.startOfDay(DAY) + 12 * 3_600_000L;

    private static StockMovementEvent move(long seq, String type, int qty, int onHandAfter, String reason, long at) {
        return StockMovementEvent.builder()
                .movementId("SM-" + seq).seq(seq).locationType("SHOP").locationCode("SHOP0001").productId("PROD0001")
                .lotId("LOT-OPEN-PROD0001").movementType(type).qty(qty).onHandAfter(onHandAfter).reason(reason)
                .occurredAt(at).build();
    }

    @Test
    void countsTheDayAndTracksOnHand() {
        RealtimeInventoryPositionRow r = InventoryRealtimeFunction.nextRow(null, move(1, "ADJUST", 10, 10, "OPENING", NOON), 1);
        r = InventoryRealtimeFunction.nextRow(r, move(2, "SALE", -3, 7, null, NOON + 1), 2);
        r = InventoryRealtimeFunction.nextRow(r, move(3, "RETURN_RESTOCK", 1, 8, null, NOON + 2), 3);

        assertEquals(8, r.getOnHandQty());
        assertEquals(DAY, r.getBusinessDate());
        assertEquals(3, r.getSoldQty());
        assertEquals(1, r.getReturnedQty());
        assertEquals(10, r.getAdjustedQty());
        assertEquals(0, r.getOversellQty());
        assertEquals(3, r.getMovementCount());
        assertEquals(3, r.getVersion());
        assertEquals("RETURN_RESTOCK", r.getLastMovementType());
        assertFalse(r.isStockout());
    }

    @Test
    void replayedMovementsAreDropped() {
        RealtimeInventoryPositionRow r = InventoryRealtimeFunction.nextRow(null, move(5, "ADJUST", 10, 10, "OPENING", NOON), 1);
        assertNull(InventoryRealtimeFunction.nextRow(r, move(5, "ADJUST", 10, 10, "OPENING", NOON), 2));
        assertNull(InventoryRealtimeFunction.nextRow(r, move(4, "SALE", -1, 9, null, NOON), 2));
    }

    @Test
    void oversellLeavesThePositionOutOfStockUntilRestocked() {
        RealtimeInventoryPositionRow r = InventoryRealtimeFunction.nextRow(null, move(1, "ADJUST", 2, 2, "OVERSELL", NOON), 1);
        r = InventoryRealtimeFunction.nextRow(r, move(2, "SALE", -2, 0, null, NOON + 5), 2);
        assertTrue(r.isStockout());
        assertEquals(NOON + 5, r.getStockoutSince());
        assertEquals(2, r.getOversellQty());

        // Still empty: the stock-out start stays / Vẫn hết: giữ mốc bắt đầu hết hàng
        r = InventoryRealtimeFunction.nextRow(r, move(3, "SALE", -1, 0, null, NOON + 9), 3);
        assertEquals(NOON + 5, r.getStockoutSince());

        r = InventoryRealtimeFunction.nextRow(r, move(4, "RECEIPT", 20, 20, null, NOON + 60), 4);
        assertFalse(r.isStockout());
        assertNull(r.getStockoutSince());
    }

    @Test
    void aNewDayRestartsTheCountersButKeepsTheStockOut() {
        RealtimeInventoryPositionRow r = InventoryRealtimeFunction.nextRow(null, move(1, "SALE", -4, 0, null, NOON), 1);
        r = InventoryRealtimeFunction.nextRow(r, move(2, "SALE", -1, 0, null, NOON + 86_400_000L), 2);

        assertEquals(DAY.plusDays(1), r.getBusinessDate());
        assertEquals(1, r.getSoldQty());
        assertEquals(1, r.getMovementCount());
        assertTrue(r.isStockout());
        assertEquals(NOON, r.getStockoutSince());
    }

    @Test
    void aLateMovementOfAnEarlierDayOnlyMovesOnHand() {
        RealtimeInventoryPositionRow r = InventoryRealtimeFunction.nextRow(null, move(1, "SALE", -1, 9, null, NOON + 86_400_000L), 1);
        r = InventoryRealtimeFunction.nextRow(r, move(2, "RETURN_RESTOCK", 1, 10, null, NOON), 2);

        assertEquals(10, r.getOnHandQty());
        assertEquals(0, r.getReturnedQty());
        assertEquals(1, r.getMovementCount());
    }
}
