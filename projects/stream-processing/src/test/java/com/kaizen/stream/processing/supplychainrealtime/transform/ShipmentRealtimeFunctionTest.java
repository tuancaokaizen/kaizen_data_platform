package com.kaizen.stream.processing.supplychainrealtime.transform;

import com.kaizen.stream.model.logistics.ShipmentEvent;
import com.kaizen.stream.model.realtime.RealtimeShipmentStatusRow;
import com.kaizen.stream.utils.time.IctTime;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ShipmentRealtimeFunctionTest {

    private static final LocalDate DAY = LocalDate.of(2026, 10, 12);
    private static final long DEP = IctTime.startOfDay(DAY) + 23 * 3_600_000L;
    private static final long MIN = 60_000L;

    private static ShipmentEvent event(int seq, String type, String status, ShipmentEvent.Stop stop, long at) {
        return ShipmentEvent.builder()
                .shipmentId("SHP-1").eventSeq(seq).eventType(type).status(status).carrierCode("CAR01")
                .vehicleId("VEH01").originCode("DC01").plannedDeparture(DEP).stopCount(3).stop(stop)
                .loadKg(500.0).loadM3(3.0).costVnd(1_200_000L).occurredAt(at).build();
    }

    private static ShipmentEvent.Stop stop(int seq, long planned, Long actual) {
        return ShipmentEvent.Stop.builder().stopSeq(seq).shopCode("SHOP000" + seq).plannedArrival(planned)
                .actualArrival(actual).build();
    }

    @Test
    void foldsATripIntoOneRow() {
        RealtimeShipmentStatusRow r = ShipmentRealtimeFunction.nextRow(null, event(1, "SHIPMENT_PLANNED", "PLANNED", null, DEP - 60 * MIN), 1);
        r = ShipmentRealtimeFunction.nextRow(r, event(2, "DEPARTED", "IN_TRANSIT", null, DEP + 5 * MIN), 2);
        r = ShipmentRealtimeFunction.nextRow(r, event(3, "ARRIVED_STOP", "IN_TRANSIT", stop(1, DEP + 60 * MIN, DEP + 80 * MIN),
                DEP + 80 * MIN), 3);
        r = ShipmentRealtimeFunction.nextRow(r, event(4, "DELIVERED", "IN_TRANSIT", stop(1, DEP + 60 * MIN, DEP + 80 * MIN),
                DEP + 90 * MIN), 4);
        r = ShipmentRealtimeFunction.nextRow(r, event(5, "PARTIAL", "IN_TRANSIT", stop(2, DEP + 120 * MIN, DEP + 110 * MIN),
                DEP + 115 * MIN), 5);
        r = ShipmentRealtimeFunction.nextRow(r, event(6, "FAILED", "PARTIAL", stop(3, DEP + 180 * MIN, null),
                DEP + 200 * MIN), 6);

        // Departure late at night still belongs to that day / Xuất phát khuya vẫn thuộc ngày đó
        assertEquals(DAY, r.getShipmentDate());
        assertEquals(DEP + 5 * MIN, r.getActualDeparture());
        assertEquals("PARTIAL", r.getStatus());
        assertEquals(1, r.getStopsDelivered());
        assertEquals(1, r.getStopsPartial());
        assertEquals(1, r.getStopsFailed());
        assertEquals(1, r.getStopsDelayed());
        assertEquals(20, r.getMaxDelayMinutes());
        assertEquals(3, r.getLastStopSeq());
        assertEquals("SHOP0003", r.getLastStopShop());
        assertEquals(6, r.getVersion());
    }

    @Test
    void replaysAreDroppedAndArrivalNeverUndoesAnOutcome() {
        RealtimeShipmentStatusRow r = ShipmentRealtimeFunction.nextRow(null, event(4, "DELIVERED", "IN_TRANSIT",
                stop(1, DEP, DEP), DEP), 1);
        assertNull(ShipmentRealtimeFunction.nextRow(r, event(4, "DELIVERED", "IN_TRANSIT", stop(1, DEP, DEP), DEP), 2));
        assertNull(ShipmentRealtimeFunction.nextRow(r, event(3, "ARRIVED_STOP", "IN_TRANSIT", stop(1, DEP, DEP), DEP), 2));

        r = ShipmentRealtimeFunction.nextRow(r, event(5, "ARRIVED_STOP", "IN_TRANSIT", stop(1, DEP, DEP), DEP), 2);
        assertEquals(1, r.getStopsDelivered());
    }

    @Test
    void delayedEventsCountTheStopOnce() {
        RealtimeShipmentStatusRow r = ShipmentRealtimeFunction.nextRow(null, event(1, "DELAYED", "IN_TRANSIT",
                stop(2, DEP + 60 * MIN, null), DEP), 1);
        r = ShipmentRealtimeFunction.nextRow(r, event(2, "DELIVERED", "IN_TRANSIT",
                stop(2, DEP + 60 * MIN, DEP + 75 * MIN), DEP + 75 * MIN), 2);
        assertEquals(1, r.getStopsDelayed());
        assertEquals(15, r.getMaxDelayMinutes());
    }
}
