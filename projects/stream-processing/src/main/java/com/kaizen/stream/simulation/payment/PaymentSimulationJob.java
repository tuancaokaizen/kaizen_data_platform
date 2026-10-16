package com.kaizen.stream.simulation.payment;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.job.JobRunner;
import com.kaizen.stream.processing.job.StreamJob;
import com.kaizen.stream.simulation.payment.sink.PaymentBundlesKafkaSink;
import com.kaizen.stream.simulation.payment.source.OrderBundlesKafkaSource;
import com.kaizen.stream.simulation.payment.transform.PaymentSchedulerFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Simulated payments: order-bundles → payment-bundles. Each order gets exactly one successful receipt and each
 * return exactly one refund, decided from the order itself (no Postgres polling).
 * Thanh toán giả lập: order-bundles → payment-bundles. Mỗi đơn đúng 1 phiếu thu thành công, mỗi phiếu trả đúng
 * 1 phiếu chi, quyết định từ chính dữ liệu đơn (không poll Postgres).
 *
 * <pre>
 * source.kafka.order-bundles → transform.payment-scheduler (keyBy orderId) → sink.kafka.payment-bundles
 * </pre>
 */
public final class PaymentSimulationJob implements StreamJob {

    public static void main(String[] args) throws Exception {
        JobRunner.run(new PaymentSimulationJob(), args);
    }

    @Override
    public String group() {
        return SIMULATION;
    }

    @Override
    public String name() {
        return "payment";
    }

    @Override
    public void build(JobContext context) {
        // Sources / Nguồn dữ liệu
        DataStream<Envelope<OrderSnapshot>> orders = OrderBundlesKafkaSource.read(context);

        // Transform / Biến đổi
        DataStream<Envelope<PaymentSnapshot>> payments = PaymentSchedulerFunction.apply(orders, context);

        // Sinks / Đích ghi
        PaymentBundlesKafkaSink.write(payments, context);
    }
}
