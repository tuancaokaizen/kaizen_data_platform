package com.kaizen.stream.model.order;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Order status derived from returns (Property 4). / Trạng thái đơn suy ra từ phiếu trả (Property 4).
 */
public enum OrderStatus {
    COMPLETED("Completed"),
    PARTIALLY_RETURNED("PartiallyReturned"),
    RETURNED("Returned");

    private final String label;

    OrderStatus(String label) {
        this.label = label;
    }

    /** Value stored in PG/JSON. / Giá trị lưu ở PG/JSON. */
    @JsonValue
    public String label() {
        return label;
    }

    @JsonCreator
    public static OrderStatus fromLabel(String label) {
        for (OrderStatus s : values()) {
            if (s.label.equals(label)) {
                return s;
            }
        }
        throw new IllegalArgumentException("Unknown order status: " + label);
    }

    /**
     * Completed ⇔ no return; Returned ⇔ Σ returned = totalPayment; otherwise PartiallyReturned.
     * Completed ⇔ không có phiếu trả; Returned ⇔ Σ trả = totalPayment; còn lại PartiallyReturned.
     */
    public static OrderStatus of(long totalPayment, int returnCount, long returnedAmount) {
        if (returnCount == 0) {
            return COMPLETED;
        }
        return returnedAmount >= totalPayment ? RETURNED : PARTIALLY_RETURNED;
    }
}
