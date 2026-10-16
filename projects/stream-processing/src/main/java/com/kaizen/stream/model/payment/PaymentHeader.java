package com.kaizen.stream.model.payment;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * payment."Payment" row; orderId/returnId become OrderPayment/ReturnPayment links in PG.
 * Dòng payment."Payment"; orderId/returnId được sink PG tách thành bảng liên kết OrderPayment/ReturnPayment.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class PaymentHeader implements Serializable {
    private String id;
    private PaymentType type;
    private PaymentStatus status;
    /** Always set; also the Kafka key. / Luôn có; cũng là key Kafka. */
    private String orderId;
    /** Set only for REFUND. / Chỉ có với REFUND. */
    private String returnId;
    private String shopCode;
    private Long totalAmount;
    /** 1 for the first try; a retry after FAILED is a new payment. / 1 cho lần đầu. */
    private Integer attempt;
    private Long createdDate;
    private Long modifiedDate;
}
