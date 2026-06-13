package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/** Ánh xạ bảng payment."OrderPayment" – liên kết Payment ↔ Order */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderPayment implements Serializable {
    private String id;         // PK
    private String paymentId;  // FK → Payment
    private String orderId;    // FK → Order
    private long createdDate;
    private long modifiedDate;
}
