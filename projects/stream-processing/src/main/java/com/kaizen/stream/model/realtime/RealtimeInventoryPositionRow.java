package com.kaizen.stream.model.realtime;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * One row of report.realtime_inventory_position and the keyed state of its position; flow counters belong to businessDate.
 * Một dòng của report.realtime_inventory_position, cũng là keyed state của vị trí; các bộ đếm thuộc về businessDate.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class RealtimeInventoryPositionRow implements Serializable {

    public static final String TABLE = "report.realtime_inventory_position";

    /** Must match the DDL in schema_clickhouse.sql (checked by a test). / Phải khớp DDL (có test kiểm tra). */
    public static final List<String> COLUMNS = Collections.unmodifiableList(Arrays.asList(
            "LocationType", "LocationCode", "ProductId", "OnHandQty", "BusinessDate", "SoldQty", "ReturnedQty",
            "ReceivedQty", "TransferInQty", "TransferOutQty", "ExpiredQty", "DamagedQty", "AdjustedQty",
            "OversellQty", "MovementCount", "IsStockout", "StockoutSince", "LastMovementType", "LastMovementAt",
            "Version", "UpdatedAt"));

    private String locationType;
    private String locationCode;
    private String productId;
    private long onHandQty;
    private LocalDate businessDate;
    private long soldQty;
    private long returnedQty;
    private long receivedQty;
    private long transferInQty;
    private long transferOutQty;
    private long expiredQty;
    private long damagedQty;
    /** Net of every ADJUST, OVERSELL included. / Tổng ròng các ADJUST, gồm cả OVERSELL. */
    private long adjustedQty;
    private long oversellQty;
    private int movementCount;
    private boolean stockout;
    /** Epoch ms when stock last hit zero; null while in stock. / Thời điểm tồn về 0; null khi còn hàng. */
    private Long stockoutSince;
    private String lastMovementType;
    private long lastMovementAt;
    /** Seq of the last applied movement. / Seq của biến động cuối đã áp. */
    private long version;
    private long updatedAtMs;
}
