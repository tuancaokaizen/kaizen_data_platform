package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/** Ánh xạ bảng payment."Payment" */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Payment implements Serializable {
    private String id;          // PK
    private short paymentType;  // FK → PaymentType.Type (1=cash,2=card,3=ewallet)
    private String status;      // nullable: "SUCCESS" | "FAILED" | "PENDING"
    private int totalPayment;   // VND
    private long createdDate;
    private long modifiedDate;
}
