package com.kaizen.stream.model.order;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * "order"."Order" row in the snapshot; money in VND, times in epoch ms.
 * Dòng "order"."Order" trong snapshot; tiền VND, thời gian epoch ms.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class OrderHeader implements Serializable {
    private String id;
    private String shopCode;
    /** Null for walk-in customers. / Null với khách vãng lai. */
    private String customerId;
    private String employeeSaleId;
    private Long transactionDate;
    /** Σ quantity × unitPrice. */
    private Long totalBill;
    /** Σ line discountAmount. / Σ chiết khấu dòng. */
    private Long totalDiscount;
    /** totalBill − totalDiscount = Σ lineAmount. */
    private Long totalPayment;
    private String voucherSeriesCode;
    private OrderStatus status;
    private Long createdDate;
    private Long modifiedDate;
}
