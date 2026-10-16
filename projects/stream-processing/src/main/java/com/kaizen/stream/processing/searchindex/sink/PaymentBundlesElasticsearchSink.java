package com.kaizen.stream.processing.searchindex.sink;

import com.kaizen.stream.config.Topic;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.job.OperatorNames;
import com.kaizen.stream.utils.connector.sink.elasticsearch.ElasticsearchSinkFactory;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Payment snapshots → Elasticsearch index payment-bundles (sink.elasticsearch.payment-bundles). Kafka keys payments
 * by order id, but an order has several payments, so the document id is the payment id.
 * Snapshot phiếu → index Elasticsearch payment-bundles. Key Kafka là id đơn nhưng một đơn có nhiều phiếu nên
 * id document là id phiếu.
 */
public final class PaymentBundlesElasticsearchSink {

    static final String INDEX = Topic.PAYMENT_BUNDLES.topicName();

    private PaymentBundlesElasticsearchSink() {
    }

    /** Same parallelism as the source, so events of one order keep partition order. / Giữ thứ tự partition. */
    public static void write(DataStream<Envelope<PaymentSnapshot>> payments, JobContext context) {
        context.sink(payments,
                ElasticsearchSinkFactory.versionedUpsert(context.settings().elasticsearch(), INDEX,
                        PaymentBundlesElasticsearchSink::documentId),
                OperatorNames.ELASTICSEARCH, INDEX);
    }

    static String documentId(Envelope<PaymentSnapshot> envelope) {
        return envelope.getData().getPayment().getId();
    }
}
