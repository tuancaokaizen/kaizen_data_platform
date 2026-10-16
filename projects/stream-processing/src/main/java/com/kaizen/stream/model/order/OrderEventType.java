package com.kaizen.stream.model.order;

/**
 * Event types on order-bundles (kafka_topic_model.md 7.3). / Loại sự kiện trên order-bundles.
 */
public enum OrderEventType {
    /** Sale order created. / Đơn bán tạo xong. */
    ORDER_PLACED,
    /** One more return document added (full or partial). / Thêm 1 phiếu trả (toàn phần hoặc một phần). */
    ORDER_RETURNED
}
