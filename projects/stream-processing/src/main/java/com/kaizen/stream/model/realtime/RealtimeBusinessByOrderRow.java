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
 * One row of report.realtime_business_by_order; grain (OrderId, ReturnId), money in VND.
 * Một dòng của report.realtime_business_by_order; grain (OrderId, ReturnId), tiền tính bằng VND.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RealtimeBusinessByOrderRow implements Serializable {

    public static final String TABLE = "report.realtime_business_by_order";

    /** Must match the DDL in schema_clickhouse.sql (checked by a test). / Phải khớp DDL (có test kiểm tra). */
    public static final List<String> COLUMNS = Collections.unmodifiableList(Arrays.asList(
            "ReportDate", "OrderId", "ReturnId", "DocumentType", "ShopCode", "ShopName", "OrderStatus",
            "SalesAmount", "ReturnAmount", "NetRevenue", "TotalReceipts", "TotalExpenditures", "NetCashFlow",
            "PaymentMethodSummary", "TotalOrderAmount", "TotalReturnAmount", "ReconciliationDiff",
            "PendingOrder", "Version", "UpdatedAt"));

    /** ReturnId of the sale row. / ReturnId của dòng bán. */
    public static final String SALE_GRAIN = "";
    public static final String SALE = "SALE";
    public static final String RETURN = "RETURN";

    private LocalDate reportDate;
    private String orderId;
    private String returnId;
    private String documentType;
    private String shopCode;
    private String shopName;
    private String orderStatus;
    private long salesAmount;
    private long returnAmount;
    private long netRevenue;
    private long totalReceipts;
    private long totalExpenditures;
    private long netCashFlow;
    private String paymentMethodSummary;
    private long totalOrderAmount;
    private long totalReturnAmount;
    private long reconciliationDiff;
    /** 1 while the receipt/refund of this grain has not succeeded yet. / 1 khi chưa có phiếu thu/chi thành công. */
    private int pendingOrder;
    /** ReplacingMergeTree version: newest input version. / Version cho ReplacingMergeTree. */
    private long version;
    private long updatedAtMs;
}
