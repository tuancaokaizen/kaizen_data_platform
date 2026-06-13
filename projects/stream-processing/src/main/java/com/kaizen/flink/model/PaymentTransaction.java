package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/** Ánh xạ bảng payment."Transaction" */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentTransaction implements Serializable {
    private String id;              // PK
    private String paymentId;       // FK → Payment
    private short paymentMethodId;  // FK → PaymentMethod (1=Tiền mặt,2=Thẻ,3=Ví điện tử)
    private int amount;             // VND
    private long createdDate;
    private long modifiedDate;
}
