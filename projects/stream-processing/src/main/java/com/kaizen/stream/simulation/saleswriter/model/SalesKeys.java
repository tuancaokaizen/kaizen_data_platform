package com.kaizen.stream.simulation.saleswriter.model;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.payment.PaymentSnapshot;

/**
 * Aggregate identity of sales events, used to keep the newest snapshot per aggregate in the Postgres buffer.
 * Payments are keyed by orderId on Kafka, but an order has several payments, so they are stored by payment id.
 * Định danh aggregate của sự kiện bán hàng, dùng để giữ snapshot mới nhất mỗi aggregate trong bộ đệm Postgres.
 * Phiếu có key Kafka = orderId, nhưng một đơn có nhiều phiếu nên lưu theo id phiếu.
 */
public final class SalesKeys {

    private SalesKeys() {
    }

    public static String orderId(Envelope<OrderSnapshot> envelope) {
        return envelope.getData().getOrder().getId();
    }

    public static String paymentId(Envelope<PaymentSnapshot> envelope) {
        return envelope.getData().getPayment().getId();
    }
}
