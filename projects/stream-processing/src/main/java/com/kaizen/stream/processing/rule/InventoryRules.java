package com.kaizen.stream.processing.rule;

import com.kaizen.stream.model.inventory.MovementType;
import com.kaizen.stream.model.inventory.StockMovementEvent;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Invariants of an inventory-movements event; broken events go to the DLQ.
 * Bất biến của sự kiện inventory-movements; sự kiện sai sang DLQ.
 */
public final class InventoryRules {

    static final List<String> LOCATION_TYPES = Arrays.asList("SHOP", "DC");

    private InventoryRules() {
    }

    /** Key of a stock position, also the Kafka key. / Khoá vị trí tồn, cũng là key Kafka. */
    public static String positionKey(String locationCode, String productId) {
        return locationCode + ":" + productId;
    }

    /**
     * @param eventType   meta.eventType, must equal movementType / phải bằng movementType
     * @param version     meta.version, must equal seq / phải bằng seq
     * @param aggregateId meta.aggregateId, must equal locationCode:productId
     */
    public static List<String> violations(String eventType, Long version, String aggregateId, StockMovementEvent e) {
        Violations v = new Violations();
        v.require("movementId", e.getMovementId())
                .require("seq", e.getSeq())
                .require("locationType", e.getLocationType())
                .require("locationCode", e.getLocationCode())
                .require("productId", e.getProductId())
                .require("lotId", e.getLotId())
                .require("movementType", e.getMovementType())
                .require("qty", e.getQty())
                .require("onHandAfter", e.getOnHandAfter())
                .require("occurredAt", e.getOccurredAt());
        // Later checks need these fields / Các bước sau cần các trường này
        if (!v.isEmpty()) {
            return v.list();
        }
        MovementType type = MovementType.parse(e.getMovementType());
        v.check(type != null, "unknown movementType " + e.getMovementType());
        v.check(type == null || type.allows(e.getQty()), "qty " + e.getQty() + " has the wrong sign for " + type);
        v.check((type == MovementType.ADJUST) == (e.getReason() != null && !e.getReason().isEmpty()),
                "reason is required for ADJUST and only for ADJUST");
        v.check(LOCATION_TYPES.contains(e.getLocationType()), "unknown locationType " + e.getLocationType());
        v.check(e.getOnHandAfter() >= 0, "onHandAfter " + e.getOnHandAfter() + " < 0");
        v.check(e.getUnitCost() == null || e.getUnitCost() >= 0, "unitCost < 0");
        // Envelope must describe this movement / Envelope phải mô tả đúng biến động này
        v.check(Objects.equals(eventType, e.getMovementType()),
                "meta.eventType " + eventType + " != movementType " + e.getMovementType());
        v.check(Objects.equals(version, e.getSeq()), "meta.version " + version + " != seq " + e.getSeq());
        v.check(Objects.equals(aggregateId, positionKey(e.getLocationCode(), e.getProductId())),
                "meta.aggregateId " + aggregateId + " != locationCode:productId");
        return v.list();
    }
}
