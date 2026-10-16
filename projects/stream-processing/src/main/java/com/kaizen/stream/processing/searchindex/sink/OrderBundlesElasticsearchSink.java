package com.kaizen.stream.processing.searchindex.sink;

import com.kaizen.stream.config.Topic;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.job.OperatorNames;
import com.kaizen.stream.utils.connector.sink.elasticsearch.ElasticsearchSinkFactory;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Order snapshots → Elasticsearch index order-bundles (sink.elasticsearch.order-bundles), one document per order id;
 * an older snapshot never replaces a newer one.
 * Snapshot đơn → index Elasticsearch order-bundles, mỗi id đơn một document; snapshot cũ không thay bản mới hơn.
 */
public final class OrderBundlesElasticsearchSink {

    static final String INDEX = Topic.ORDER_BUNDLES.topicName();

    private OrderBundlesElasticsearchSink() {
    }

    /** Same parallelism as the source, so events of one order keep partition order. / Giữ thứ tự partition. */
    public static void write(DataStream<Envelope<OrderSnapshot>> orders, JobContext context) {
        context.sink(orders,
                ElasticsearchSinkFactory.versionedUpsert(context.settings().elasticsearch(), INDEX,
                        OrderBundlesElasticsearchSink::documentId),
                OperatorNames.ELASTICSEARCH, INDEX);
    }

    static String documentId(Envelope<OrderSnapshot> envelope) {
        return envelope.getData().getOrder().getId();
    }
}
