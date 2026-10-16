package com.kaizen.stream.model.logistics;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * logistics-events payload (platform_v2_blueprint.md 3.4, plus plannedDeparture and stopCount).
 * Payload của logistics-events (platform_v2_blueprint.md 3.4, thêm plannedDeparture và stopCount).
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class ShipmentEvent implements Serializable {
    private String shipmentId;
    /** Order within the shipment, also meta.version. / Thứ tự trong chuyến, cũng là meta.version. */
    private Integer eventSeq;
    private String eventType;
    /** Shipment status after this event (logistics."Shipment"."Status"). / Trạng thái chuyến sau sự kiện. */
    private String status;
    private String carrierCode;
    private String vehicleId;
    /** Warehouse the trip leaves from. / Kho xuất phát. */
    private String originCode;
    /** Fixes the trip's business date. / Xác định ngày nghiệp vụ của chuyến. */
    private Long plannedDeparture;
    private Integer stopCount;
    private Stop stop;
    private String reasonCode;
    private Double loadKg;
    private Double loadM3;
    private Long costVnd;
    private Long occurredAt;

    @Data
    @Builder(toBuilder = true)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Stop implements Serializable {
        private Integer stopSeq;
        private String shopCode;
        private Long plannedArrival;
        /** Null until the truck arrives. / Null cho tới khi xe tới. */
        private Long actualArrival;
    }
}
