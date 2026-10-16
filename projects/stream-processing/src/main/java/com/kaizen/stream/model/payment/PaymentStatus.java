package com.kaizen.stream.model.payment;

/**
 * Payment lifecycle; a retry after FAILED is a new payment id. / Vòng đời phiếu; thử lại sau FAILED là id mới.
 */
public enum PaymentStatus {
    PENDING,
    SUCCESS,
    FAILED
}
