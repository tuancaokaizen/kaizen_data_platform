package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Ánh xạ bảng promotion."Promotion"
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Promotion implements Serializable {
    private String code;        // PK
    private String name;
    private long startDate;     // epoch-ms
    private long endDate;       // epoch-ms
    private boolean status;
    private String type;        // nullable: "DISCOUNT" | "GIFT" | "COMBO"
    private long createdDate;
    private long modifiedDate;
}
