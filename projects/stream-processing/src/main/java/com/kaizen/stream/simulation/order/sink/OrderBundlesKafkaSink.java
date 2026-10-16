package com.kaizen.stream.simulation.order.sink;

import com.kaizen.stream.config.Topic;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.processing.job.JobContext;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Simulated order events → Kafka order-bundles (sink.kafka.order-bundles), forwarded so each order keeps emit order.
 * Sự kiện đơn giả lập → Kafka order-bundles, nối thẳng để mỗi đơn giữ đúng thứ tự phát.
 */
public final class OrderBundlesKafkaSink {

    private OrderBundlesKafkaSink() {
    }

    public static void write(DataStream<Envelope<OrderSnapshot>> orders, JobContext context) {
        context.sinkToKafka(orders, Topic.ORDER_BUNDLES);
    }
}
