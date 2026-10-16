package com.kaizen.stream.model.realtime;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One row of report.realtime_shipment_status and the keyed state of its trip. stopStatus / delayedStops are state only.
 * Một dòng của report.realtime_shipment_status, cũng là keyed state của chuyến. stopStatus / delayedStops chỉ là state.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class RealtimeShipmentStatusRow implements Serializable {

    public static final String TABLE = "report.realtime_shipment_status";

    /** Must match the DDL in schema_clickhouse.sql (checked by a test). / Phải khớp DDL (có test kiểm tra). */
    public static final List<String> COLUMNS = Collections.unmodifiableList(Arrays.asList(
            "ShipmentDate", "ShipmentId", "OriginCode", "CarrierCode", "VehicleId", "Status", "PlannedDeparture",
            "ActualDeparture", "StopCount", "StopsDelivered", "StopsPartial", "StopsFailed", "StopsDelayed",
            "LastStopSeq", "LastStopShop", "MaxDelayMinutes", "LoadKg", "LoadM3", "CostVnd", "LastEventType",
            "LastReasonCode", "LastEventAt", "Version", "UpdatedAt"));

    private LocalDate shipmentDate;
    private String shipmentId;
    private String originCode;
    private String carrierCode;
    private String vehicleId;
    private String status;
    private Long plannedDeparture;
    private Long actualDeparture;
    private int stopCount;
    private int stopsDelivered;
    private int stopsPartial;
    private int stopsFailed;
    private int stopsDelayed;
    private int lastStopSeq;
    private String lastStopShop;
    private int maxDelayMinutes;
    private double loadKg;
    private double loadM3;
    private long costVnd;
    private String lastEventType;
    private String lastReasonCode;
    private long lastEventAt;
    /** eventSeq of the last applied event. / eventSeq của sự kiện cuối đã áp. */
    private long version;
    private long updatedAtMs;

    /** Latest outcome per stop: DELIVERED / PARTIAL / FAILED / ARRIVED. / Kết quả mới nhất mỗi điểm dừng. */
    @Builder.Default
    private Map<Integer, String> stopStatus = new HashMap<>();
    /** Stops that were late or got a DELAYED event. / Điểm dừng bị trễ hoặc có sự kiện DELAYED. */
    @Builder.Default
    private Set<Integer> delayedStops = new HashSet<>();
}
