package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Ánh xạ bảng promotion."Gift"
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Gift implements Serializable {
    private String id;               // PK
    private String productId;        // FK → Product
    private String promotionCode;    // FK → Promotion
    private Integer promotionQuantity; // nullable
    private Boolean status;          // nullable
    private long createdDate;
    private long modifiedDate;
}
