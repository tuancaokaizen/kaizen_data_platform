package com.kaizen.stream.testing;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderEventType;
import com.kaizen.stream.model.order.OrderHeader;
import com.kaizen.stream.model.order.OrderLine;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.order.OrderStatus;
import com.kaizen.stream.model.order.ReturnDocument;
import com.kaizen.stream.model.order.ReturnLine;
import com.kaizen.stream.model.payment.PaymentEventType;
import com.kaizen.stream.model.payment.PaymentHeader;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.model.payment.PaymentStatus;
import com.kaizen.stream.model.payment.PaymentTransaction;
import com.kaizen.stream.model.payment.PaymentType;
import com.kaizen.stream.utils.connector.source.kafka.RawRecord;
import com.kaizen.stream.utils.serialization.EnvelopeCodec;
import com.kaizen.stream.utils.serialization.EventHeaders;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Valid sample snapshots; tests break one field at a time.
 * Snapshot mẫu hợp lệ; mỗi test làm hỏng đúng 1 trường.
 */
public final class Fixtures {

    public static final String ORDER_ID = "SORD-20261011-SHOP0001-00123";
    public static final long T0 = 1_791_705_600_000L;
    public static final long T_RETURN = T0 + 86_400_000L;

    private Fixtures() {
    }

    /**
     * Bill 340,000 − discount 35,000 = 305,000 over two paid lines and one gift line.
     * Tổng 340.000 − chiết khấu 35.000 = 305.000 trên 2 dòng bán và 1 dòng quà.
     */
    public static OrderSnapshot placedOrder() {
        OrderHeader header = OrderHeader.builder()
                .id(ORDER_ID).shopCode("SHOP0001").customerId("CUS000123").employeeSaleId("EMP0042")
                .transactionDate(T0).totalBill(340_000L).totalDiscount(35_000L).totalPayment(305_000L)
                .voucherSeriesCode("SR_V10_0001").status(OrderStatus.COMPLETED)
                .createdDate(T0).modifiedDate(T0)
                .build();
        OrderLine l1 = OrderLine.builder().id(ORDER_ID + "-L1").lineNo(1).productId("P0012").quantity(2)
                .unitPrice(120_000L).discountAmount(24_000L).lineAmount(216_000L).gift(false).unitCost(80_000L).build();
        OrderLine l2 = OrderLine.builder().id(ORDER_ID + "-L2").lineNo(2).productId("P0040").quantity(1)
                .unitPrice(100_000L).discountAmount(11_000L).lineAmount(89_000L).gift(false).unitCost(60_000L).build();
        OrderLine gift = OrderLine.builder().id(ORDER_ID + "-L3").lineNo(3).productId("G0001").quantity(1)
                .unitPrice(0L).discountAmount(0L).lineAmount(0L).gift(true).unitCost(5_000L).build();
        return OrderSnapshot.builder()
                .order(header)
                .items(new ArrayList<>(Arrays.asList(l1, l2, gift)))
                .returns(new ArrayList<>())
                .build();
    }

    /** One unit of line 1 returned (108,000). / Trả 1 đơn vị của dòng 1 (108.000). */
    public static OrderSnapshot partiallyReturnedOrder() {
        OrderSnapshot s = placedOrder();
        String returnId = "RET-" + ORDER_ID + "-1";
        ReturnLine rl = ReturnLine.builder().id(returnId + "-L1").orderItemId(ORDER_ID + "-L1").productId("P0012")
                .quantity(1).restockQty(1).damageQty(0).unitPrice(120_000L).unitCost(80_000L)
                .lineAmount(108_000L).build();
        ReturnDocument doc = ReturnDocument.builder().id(returnId).createdDate(T_RETURN).reason("CHANGED_MIND")
                .returnAmount(108_000L).items(new ArrayList<>(Arrays.asList(rl))).build();
        s.getReturns().add(doc);
        s.getOrder().setStatus(OrderStatus.PARTIALLY_RETURNED);
        s.getOrder().setModifiedDate(T_RETURN);
        return s;
    }

    public static Envelope<OrderSnapshot> orderEnvelope(OrderSnapshot s) {
        OrderEventType type = s.getReturns().isEmpty() ? OrderEventType.ORDER_PLACED : OrderEventType.ORDER_RETURNED;
        return Envelope.of(type, "flink.simulation-order", s.getOrder().getId(),
                s.getOrder().getModifiedDate(), s.getOrder().getModifiedDate(), s);
    }

    public static PaymentSnapshot successReceipt() {
        PaymentHeader p = PaymentHeader.builder()
                .id("PAY-" + ORDER_ID + "-1").type(PaymentType.RECEIPT).status(PaymentStatus.SUCCESS)
                .orderId(ORDER_ID).shopCode("SHOP0001").totalAmount(305_000L).attempt(1)
                .createdDate(T0 + 5_000).modifiedDate(T0 + 9_000)
                .build();
        return PaymentSnapshot.builder()
                .payment(p)
                .transactions(new ArrayList<>(Arrays.asList(
                        PaymentTransaction.builder().id("TXN-1").paymentMethodId(1).amount(205_000L).build(),
                        PaymentTransaction.builder().id("TXN-2").paymentMethodId(3).amount(100_000L).build())))
                .build();
    }

    /** Card refund of a return. / Phiếu chi hoàn tiền qua thẻ cho một phiếu trả. */
    public static PaymentSnapshot successRefund(String returnId, long amount, long modifiedDate) {
        PaymentHeader p = PaymentHeader.builder()
                .id("PAY-" + returnId).type(PaymentType.REFUND).status(PaymentStatus.SUCCESS)
                .orderId(ORDER_ID).returnId(returnId).shopCode("SHOP0001").totalAmount(amount).attempt(1)
                .createdDate(modifiedDate - 4_000).modifiedDate(modifiedDate)
                .build();
        return PaymentSnapshot.builder()
                .payment(p)
                .transactions(new ArrayList<>(Arrays.asList(
                        PaymentTransaction.builder().id("TXN-" + returnId).paymentMethodId(2).amount(amount).build())))
                .build();
    }

    public static Envelope<PaymentSnapshot> paymentEnvelope(PaymentSnapshot s) {
        return Envelope.of(PaymentEventType.of(s.getPayment().getType(), s.getPayment().getStatus()),
                "flink.simulation-payment", s.getPayment().getOrderId(), s.getPayment().getModifiedDate(),
                s.getPayment().getModifiedDate(), s);
    }

    /** Kafka record exactly as a producer would write it. / Record Kafka đúng như producer ghi. */
    public static RawRecord record(Envelope<?> envelope) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(EventHeaders.EVENT_TYPE, envelope.getMeta().getEventType());
        headers.put(EventHeaders.SCHEMA_VERSION, "2");
        headers.put(EventHeaders.EVENT_ID, envelope.getMeta().getEventId());
        headers.put(EventHeaders.SOURCE, envelope.getMeta().getSource());
        return new RawRecord("order-bundles", 2, 41L, envelope.getMeta().getEventTime(),
                envelope.key().getBytes(StandardCharsets.UTF_8), EnvelopeCodec.encode(envelope), headers);
    }

    public static RawRecord record(String key, String json, Map<String, String> headers) {
        return new RawRecord("order-bundles", 0, 7L, T0,
                key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                json.getBytes(StandardCharsets.UTF_8), headers);
    }
}
