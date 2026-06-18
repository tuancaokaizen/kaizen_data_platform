package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Ánh xạ bảng order."OrderItem" – PK composite ("Id", "OrderId")
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderItem implements Serializable {
    private String id;          // PK part 1
    private String orderId;     // PK part 2 + FK → Order
    private String productId;   // FK → Product, nullable
    private Integer quantity;   // nullable
    private Integer unitPrice;  // nullable
    private Long createdDate;   // nullable epoch-ms
    private Long modifiedDate;  // nullable epoch-ms
}
