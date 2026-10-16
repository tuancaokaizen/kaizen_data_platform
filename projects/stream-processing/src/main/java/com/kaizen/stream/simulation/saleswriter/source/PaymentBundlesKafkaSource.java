package com.kaizen.stream.simulation.saleswriter.source;

import com.kaizen.stream.config.Topic;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.rule.PaymentRules;
import com.kaizen.stream.utils.event.EventFilter;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Kafka payment-bundles (source.kafka.payment-bundles): every payment event is written to the Postgres payment schema.
 * Kafka payment-bundles: mọi sự kiện phiếu được ghi vào schema "payment" của Postgres.
 */
public final class PaymentBundlesKafkaSource {

    private PaymentBundlesKafkaSource() {
    }

    public static DataStream<Envelope<PaymentSnapshot>> read(JobContext context) {
        return context.source(Topic.PAYMENT_BUNDLES, PaymentSnapshot.class, EventFilter.all(),
                (meta, snapshot) -> PaymentRules.violations(meta.getEventType(), meta.getVersion(), snapshot));
    }
}
