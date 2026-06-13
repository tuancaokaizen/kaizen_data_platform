package com.kaizen.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * Bundle gom Payment + Transactions + OrderPayment + ReturnPayment (optional)
 * vào một event duy nhất để đảm bảo thứ tự insert.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentBundle implements Serializable {
    private Payment payment;
    private List<PaymentTransaction> transactions;
    private OrderPayment orderPayment;
    private ReturnPayment returnPayment;  // null nếu không phải hoàn tiền
}
