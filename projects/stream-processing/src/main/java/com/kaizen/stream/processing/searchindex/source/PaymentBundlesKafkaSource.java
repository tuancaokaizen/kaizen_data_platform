package com.kaizen.stream.processing.searchindex.source;

import com.kaizen.stream.config.Topic;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.rule.PaymentRules;
import com.kaizen.stream.utils.event.EventFilter;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Kafka payment-bundles (source.kafka.payment-bundles): every payment event, including pending and failed ones,
 * is indexed; broken snapshots go to dead letter.
 * Kafka payment-bundles: mọi sự kiện phiếu (kể cả chờ và thất bại) đều được index; snapshot sai sang dead letter.
 */
public final class PaymentBundlesKafkaSource {

    private PaymentBundlesKafkaSource() {
    }

    public static DataStream<Envelope<PaymentSnapshot>> read(JobContext context) {
        return context.source(Topic.PAYMENT_BUNDLES, PaymentSnapshot.class, EventFilter.all(),
                (meta, snapshot) -> PaymentRules.violations(meta.getEventType(), meta.getVersion(), snapshot));
    }
}
