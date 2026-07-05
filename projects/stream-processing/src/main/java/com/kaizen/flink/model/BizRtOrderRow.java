package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * One row of realtime business-by-order report for ClickHouse.
 * Một dòng báo cáo tình hình KD theo đơn realtime ghi vào ClickHouse.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BizRtOrderRow implements Serializable {
    private String reportDate;
    private String orderId;
    private String returnId;
    private String documentType;
    private String shopCode;
    private String shopName;
    private String orderStatus;
    private double salesAmount;
    private double returnAmount;
    private double netRevenue;
    private double totalReceipts;
    private double totalExpenditures;
    private double netCashFlow;
    private String paymentMethodSummary;
    private double totalOrderAmount;
    private double totalReturnAmount;
    private double reconciliationDiff;
    private byte pendingOrder;
    private long updatedAtMs;
}
