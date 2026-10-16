package com.kaizen.stream.simulation.payment.sink;

import com.kaizen.stream.config.Topic;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.processing.job.JobContext;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Simulated payment events → Kafka payment-bundles (sink.kafka.payment-bundles), forwarded so each order keeps
 * emit order.
 * Sự kiện phiếu giả lập → Kafka payment-bundles, nối thẳng để mỗi đơn giữ đúng thứ tự phát.
 */
public final class PaymentBundlesKafkaSink {

    private PaymentBundlesKafkaSink() {
    }

    public static void write(DataStream<Envelope<PaymentSnapshot>> payments, JobContext context) {
        context.sinkToKafka(payments, Topic.PAYMENT_BUNDLES);
    }
}
