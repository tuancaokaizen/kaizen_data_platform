package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/** Ánh xạ bảng payment."ReturnPayment" – liên kết Payment ↔ Return */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReturnPayment implements Serializable {
    private String id;        // PK
    private String paymentId; // FK → Payment
    private String returnId;  // FK → Return.Id
    private String orderId;   // FK → Return.OrderId (composite FK)
    private long createdDate;
    private long modifiedDate;
}
