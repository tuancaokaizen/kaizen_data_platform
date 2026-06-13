package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/** Ánh xạ bảng promotion."Voucher" */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Voucher implements Serializable {
    private String code;           // PK
    private String name;
    private String promotionCode;  // FK → Promotion, nullable
    private Integer voucherAmount; // nullable (VND)
    private Integer minOrderValue; // nullable (VND)
    private long startDate;        // epoch-ms
    private long endDate;          // epoch-ms
    private int issuedQuantity;
    private boolean status;
    private long createdDate;
    private long modifiedDate;
}
