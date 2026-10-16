package com.kaizen.stream.simulation.payment.transform.state;

import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.simulation.payment.transform.PaymentDecider;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Keyed state of PaymentSchedulerFunction: when and how the order's receipt succeeded; refunds wait for it
 * and go back to its main method.
 * State theo key của PaymentSchedulerFunction: thời điểm và phương thức phiếu thu thành công; phiếu chi chờ
 * mốc này và hoàn về phương thức chính.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentReceipt implements Serializable {

    private static final long serialVersionUID = 1L;

    private long settledAt;
    private int methodId;

    public static PaymentReceipt of(PaymentSnapshot success) {
        return new PaymentReceipt(success.getPayment().getModifiedDate(), PaymentDecider.mainMethod(success));
    }
}
