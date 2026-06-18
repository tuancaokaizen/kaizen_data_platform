package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Ánh xạ bảng product."Product"
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Product implements Serializable {
    private String id;          // PK
    private String name;
    private String type;        // "medicine" | "supplement"
    private String unit;        // nullable
    private int stockPrice;
    private int retailPrice;
    private int taxRate;
    private boolean isPromotion;
    private boolean status;
    private long createdDate;
    private long modifiedDate;
}
