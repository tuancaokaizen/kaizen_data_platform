package com.kaizen.stream.processing.searchindex.source;

import com.kaizen.stream.config.Topic;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.rule.OrderRules;
import com.kaizen.stream.utils.event.EventFilter;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Kafka order-bundles (source.kafka.order-bundles): every order event is indexed; broken snapshots go to dead letter.
 * Kafka order-bundles: mọi sự kiện đơn đều được index; snapshot sai sang dead letter.
 */
public final class OrderBundlesKafkaSource {

    private OrderBundlesKafkaSource() {
    }

    public static DataStream<Envelope<OrderSnapshot>> read(JobContext context) {
        return context.source(Topic.ORDER_BUNDLES, OrderSnapshot.class, EventFilter.all(),
                (meta, snapshot) -> OrderRules.violations(meta.getEventType(), meta.getVersion(), snapshot));
    }
}
