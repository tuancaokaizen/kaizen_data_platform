package com.kaizen.stream.model.inventory;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * inventory-movements payload, built by inventory.apply_movement (platform_v2_blueprint.md 3.4).
 * Payload của inventory-movements, do inventory.apply_movement dựng (platform_v2_blueprint.md 3.4).
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class StockMovementEvent implements Serializable {
    private String movementId;
    /** Global ledger order, also meta.version. / Thứ tự sổ cái toàn cục, cũng là meta.version. */
    private Long seq;
    /** SHOP or DC. / SHOP hoặc DC. */
    private String locationType;
    private String locationCode;
    private String productId;
    private String lotId;
    /** ISO date, null for lots without expiry. / Ngày ISO, null với lô không hạn dùng. */
    private String expiryDate;
    private String movementType;
    /** Signed: outflows negative. / Có dấu: xuất là số âm. */
    private Integer qty;
    /** Stock of the position (all lots) after this movement. / Tồn của vị trí (mọi lô) sau biến động. */
    private Integer onHandAfter;
    private Long unitCost;
    private String refType;
    private String refId;
    private String refLineId;
    /** Why an ADJUST happened (OPENING, OVERSELL, COUNT). / Lý do điều chỉnh. */
    private String reason;
    private Long occurredAt;
}
