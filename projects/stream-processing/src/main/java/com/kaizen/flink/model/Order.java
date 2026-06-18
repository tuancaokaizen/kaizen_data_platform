package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Ánh xạ bảng order."Order"
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order implements Serializable {
    private String id;              // PK
    private String customerId;      // FK → Customer
    private String shopCode;        // FK → Shop, nullable
    private Long transactionDate;   // epoch-ms, nullable
    private Integer totalBill;      // nullable
    private Integer totalDiscount;  // nullable
    private Integer totalPayment;   // nullable
    private String employeeSaleId;  // FK → Employee
    private String status;          // "Completed" | "Returned"
    private String voucherSeriesCode; // FK → VoucherSeries, nullable
    private long createdDate;
    private long modifiedDate;
}
