package com.kaizen.stream.model.payment;

/**
 * Event types on payment-bundles (kafka_topic_model.md 7.4). / Loại sự kiện trên payment-bundles.
 */
public enum PaymentEventType {
    PAYMENT_PENDING,
    PAYMENT_SUCCEEDED,
    PAYMENT_FAILED,
    REFUND_SUCCEEDED;

    /**
     * The only event type allowed for a (type, status) pair; refunds are emitted only once they succeed.
     * Loại sự kiện duy nhất hợp lệ cho cặp (type, status); phiếu chi chỉ phát khi đã thành công.
     */
    public static PaymentEventType of(PaymentType type, PaymentStatus status) {
        if (type == PaymentType.REFUND) {
            if (status != PaymentStatus.SUCCESS) {
                throw new IllegalArgumentException("Refunds are only emitted as SUCCESS, got " + status);
            }
            return REFUND_SUCCEEDED;
        }
        switch (status) {
            case PENDING:
                return PAYMENT_PENDING;
            case SUCCESS:
                return PAYMENT_SUCCEEDED;
            case FAILED:
                return PAYMENT_FAILED;
            default:
                throw new IllegalArgumentException("Unknown status " + status);
        }
    }
}
