package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Ánh xạ bảng order."ReturnItem" – PK ("Id")
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReturnItem implements Serializable {
    private String id;            // PK
    private String returnId;      // FK → Return.Id (composite with orderId)
    private String productId;     // FK → Product, nullable
    private int quantity;
    private Integer unitPrice;    // nullable
    private String orderItemId;   // nullable
    private String orderId;       // nullable
    private Integer totalAmount;  // nullable
    private Long createdDate;     // epoch-ms, nullable
    private Long modifiedDate;    // epoch-ms, nullable
}
