package com.kaizen.stream.model.payment;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * payment."Transaction" row: one payment method's share. / Phần tiền theo một phương thức thanh toán.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class PaymentTransaction implements Serializable {
    private String id;
    private Integer paymentMethodId;
    private Long amount;
}
