package com.kaizen.stream.processing.rule;

import com.kaizen.stream.model.logistics.ShipmentEvent;
import com.kaizen.stream.model.logistics.ShipmentEventType;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Invariants of a logistics-events event; broken events go to the DLQ.
 * Bất biến của sự kiện logistics-events; sự kiện sai sang DLQ.
 */
public final class LogisticsRules {

    /** Same list as ck_shipment_status in logistics.sql. / Cùng danh sách với ck_shipment_status. */
    static final List<String> STATUSES =
            Arrays.asList("PLANNED", "PICKED", "IN_TRANSIT", "COMPLETED", "PARTIAL", "FAILED");
    static final List<String> REASONS =
            Arrays.asList("WEATHER", "TRAFFIC", "VEHICLE", "HOLIDAY", "SHORT_PICK", "TRUCK_BAN");

    private LogisticsRules() {
    }

    /**
     * @param eventType   meta.eventType, must equal data.eventType / phải bằng data.eventType
     * @param version     meta.version, must equal eventSeq / phải bằng eventSeq
     * @param aggregateId meta.aggregateId, must equal shipmentId / phải bằng shipmentId
     */
    public static List<String> violations(String eventType, Long version, String aggregateId, ShipmentEvent e) {
        Violations v = new Violations();
        v.require("shipmentId", e.getShipmentId())
                .require("eventSeq", e.getEventSeq())
                .require("eventType", e.getEventType())
                .require("status", e.getStatus())
                .require("originCode", e.getOriginCode())
                .require("plannedDeparture", e.getPlannedDeparture())
                .require("stopCount", e.getStopCount())
                .require("occurredAt", e.getOccurredAt());
        // Later checks need these fields / Các bước sau cần các trường này
        if (!v.isEmpty()) {
            return v.list();
        }
        ShipmentEventType type = ShipmentEventType.parse(e.getEventType());
        v.check(type != null, "unknown eventType " + e.getEventType());
        v.check(STATUSES.contains(e.getStatus()), "unknown status " + e.getStatus());
        v.check(e.getReasonCode() == null || REASONS.contains(e.getReasonCode()),
                "unknown reasonCode " + e.getReasonCode());
        v.check(e.getEventSeq() > 0 && e.getStopCount() >= 0, "eventSeq must be > 0 and stopCount >= 0");
        v.check(type == null || !type.stopRequired() || e.getStop() != null, type + " needs a stop");
        if (e.getStop() != null) {
            ShipmentEvent.Stop s = e.getStop();
            v.check(s.getStopSeq() != null && s.getStopSeq() >= 1 && s.getStopSeq() <= e.getStopCount(),
                    "stop.stopSeq " + s.getStopSeq() + " outside 1.." + e.getStopCount());
            v.require("stop.shopCode", s.getShopCode()).require("stop.plannedArrival", s.getPlannedArrival());
        }
        v.check((e.getLoadKg() == null || e.getLoadKg() >= 0) && (e.getLoadM3() == null || e.getLoadM3() >= 0)
                && (e.getCostVnd() == null || e.getCostVnd() >= 0), "load and cost must be >= 0");
        // Envelope must describe this event / Envelope phải mô tả đúng sự kiện này
        v.check(Objects.equals(eventType, e.getEventType()),
                "meta.eventType " + eventType + " != eventType " + e.getEventType());
        v.check(Objects.equals(version, e.getEventSeq().longValue()),
                "meta.version " + version + " != eventSeq " + e.getEventSeq());
        v.check(Objects.equals(aggregateId, e.getShipmentId()),
                "meta.aggregateId " + aggregateId + " != shipmentId " + e.getShipmentId());
        return v.list();
    }
}
