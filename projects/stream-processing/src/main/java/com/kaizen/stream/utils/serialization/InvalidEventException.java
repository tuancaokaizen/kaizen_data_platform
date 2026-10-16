package com.kaizen.stream.utils.serialization;

/**
 * The message itself is bad (JSON, contract or invariant); it goes to the DLQ, never retried.
 * Lỗi do chính message (JSON, hợp đồng, bất biến); chuyển sang DLQ, không thử lại.
 */
public class InvalidEventException extends RuntimeException {

    public InvalidEventException(String message) {
        super(message);
    }

    public InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
