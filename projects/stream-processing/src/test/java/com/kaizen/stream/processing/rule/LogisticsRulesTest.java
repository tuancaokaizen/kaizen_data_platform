package com.kaizen.stream.processing.rule;

import com.kaizen.stream.model.logistics.ShipmentEvent;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class LogisticsRulesTest {

    static ShipmentEvent arrived() {
        return ShipmentEvent.builder()
                .shipmentId("SHP-20261012-DC01-003").eventSeq(4).eventType("ARRIVED_STOP").status("IN_TRANSIT")
                .carrierCode("CAR02").vehicleId("VEH07").originCode("DC01").plannedDeparture(1_791_765_000_000L)
                .stopCount(3)
                .stop(ShipmentEvent.Stop.builder().stopSeq(2).shopCode("SHOP0014")
                        .plannedArrival(1_791_772_200_000L).actualArrival(1_791_774_000_000L).build())
                .reasonCode("TRAFFIC").loadKg(640.0).loadM3(4.1).costVnd(1_850_000L).occurredAt(1_791_774_000_000L)
                .build();
    }

    private static List<String> check(ShipmentEvent e) {
        return LogisticsRules.violations(e.getEventType(), e.getEventSeq().longValue(), e.getShipmentId(), e);
    }

    @Test
    void validEventPasses() {
        assertTrue(check(arrived()).isEmpty());
    }

    @Test
    void stopLevelEventsNeedAStopInsideTheTrip() {
        assertTrue(check(arrived().toBuilder().stop(null).build()).get(0).contains("needs a stop"));
        ShipmentEvent outside = arrived().toBuilder()
                .stop(arrived().getStop().toBuilder().stopSeq(4).build()).build();
        assertTrue(check(outside).get(0).contains("outside 1..3"));
        assertTrue(check(arrived().toBuilder().eventType("DEPARTED").stop(null).build()).isEmpty());
    }

    @Test
    void unknownCodesAreRejected() {
        assertTrue(check(arrived().toBuilder().status("LOST").build()).get(0).contains("unknown status"));
        assertTrue(check(arrived().toBuilder().reasonCode("ALIENS").build()).get(0).contains("unknown reasonCode"));
        assertTrue(check(arrived().toBuilder().eventType("TELEPORTED").build()).get(0).contains("unknown eventType"));
    }

    @Test
    void envelopeMustMatchThePayload() {
        ShipmentEvent e = arrived();
        assertTrue(LogisticsRules.violations("ARRIVED_STOP", 3L, e.getShipmentId(), e).get(0).contains("meta.version"));
        assertTrue(LogisticsRules.violations("ARRIVED_STOP", 4L, "SHP-X", e).get(0).contains("aggregateId"));
    }
}
