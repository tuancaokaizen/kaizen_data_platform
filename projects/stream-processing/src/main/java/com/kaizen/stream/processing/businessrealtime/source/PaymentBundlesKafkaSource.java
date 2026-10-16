package com.kaizen.stream.processing.businessrealtime.source;

import com.kaizen.stream.config.Topic;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.payment.PaymentEventType;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.rule.PaymentRules;
import com.kaizen.stream.utils.event.EventFilter;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Kafka payment-bundles (source.kafka.payment-bundles): only successful receipts and refunds count in the report.
 * Kafka payment-bundles: báo cáo chỉ tính phiếu thu và phiếu chi thành công.
 */
public final class PaymentBundlesKafkaSource {

    private PaymentBundlesKafkaSource() {
    }

    public static DataStream<Envelope<PaymentSnapshot>> read(JobContext context) {
        // Filter on the header before parsing / Lọc theo header trước khi parse
        return context.source(Topic.PAYMENT_BUNDLES, PaymentSnapshot.class,
                EventFilter.only(PaymentEventType.PAYMENT_SUCCEEDED, PaymentEventType.REFUND_SUCCEEDED),
                (meta, snapshot) -> PaymentRules.violations(meta.getEventType(), meta.getVersion(), snapshot));
    }
}
