package com.kaizen.stream.model.payment;

/**
 * Receipt (money in) or refund (money out); code = payment."Payment"."PaymentType".
 * Phiếu thu hoặc phiếu chi; code = cột payment."Payment"."PaymentType".
 */
public enum PaymentType {
    RECEIPT(1),
    REFUND(2);

    private final int code;

    PaymentType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}
