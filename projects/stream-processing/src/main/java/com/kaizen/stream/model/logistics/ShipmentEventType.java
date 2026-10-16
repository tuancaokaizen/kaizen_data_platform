package com.kaizen.stream.model.logistics;

/**
 * Event types of logistics-events, same list as ck_shipment_event_type in logistics.sql.
 * Loại sự kiện của logistics-events, cùng danh sách với ck_shipment_event_type trong logistics.sql.
 */
public enum ShipmentEventType {
    TRANSFER_CREATED(false),
    SHIPMENT_PLANNED(false),
    PICKED(false),
    DEPARTED(false),
    ARRIVED_STOP(true),
    DELIVERED(true),
    PARTIAL(true),
    DELAYED(false),
    FAILED(false);

    /** Event must name its stop; DELAYED/FAILED may name one. / Sự kiện bắt buộc có điểm dừng. */
    private final boolean stopRequired;

    ShipmentEventType(boolean stopRequired) {
        this.stopRequired = stopRequired;
    }

    public boolean stopRequired() {
        return stopRequired;
    }

    /** Null for unknown names instead of throwing. / Trả null khi tên lạ thay vì ném lỗi. */
    public static ShipmentEventType parse(String name) {
        for (ShipmentEventType t : values()) {
            if (t.name().equals(name)) {
                return t;
            }
        }
        return null;
    }
}
