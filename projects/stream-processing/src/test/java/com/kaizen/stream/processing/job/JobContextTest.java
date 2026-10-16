package com.kaizen.stream.processing.job;

import com.kaizen.stream.config.RawSettings;
import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.config.Topic;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.payment.PaymentEventType;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.utils.event.EventFilter;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobContextTest {

    private static JobContext context(int sourceParallelism) {
        Map<String, String> values = new HashMap<>();
        values.put("kafka.bootstrap_servers", "kafka:9092");
        values.put("flink.parallelism", "3");
        values.put("flink.source_parallelism", String.valueOf(sourceParallelism));
        StreamSettings settings = new StreamSettings(new RawSettings(values));
        return new JobContext("processing-business-realtime", settings, StreamExecutionEnvironment.getExecutionEnvironment());
    }

    @Test
    void sourceWiresParserAndDeadLettersWithStableUids() {
        JobContext ctx = context(3);
        DataStream<?> orders = ctx.source(Topic.ORDER_BUNDLES, OrderSnapshot.class, EventFilter.all());
        DataStream<?> payments = ctx.source(Topic.PAYMENT_BUNDLES, PaymentSnapshot.class,
                EventFilter.only(PaymentEventType.PAYMENT_SUCCEEDED, PaymentEventType.REFUND_SUCCEEDED));
        orders.addSink(new DiscardingSink<>());
        payments.addSink(new DiscardingSink<>());

        Set<String> uids = ctx.env().getTransformations().stream()
                .flatMap(t -> t.getTransitivePredecessors().stream())
                .map(Transformation::getUid)
                .filter(u -> u != null)
                .collect(Collectors.toSet());
        assertTrue(uids.containsAll(Set.of(
                "source.kafka.order-bundles", "source.kafka.order-bundles.parse", "sink.deadletter.order-bundles",
                "source.kafka.payment-bundles", "source.kafka.payment-bundles.parse",
                "sink.deadletter.payment-bundles")), uids.toString());
        assertEquals(3, orders.getParallelism());
        assertEquals("flink.processing-business-realtime", ctx.eventSource());
    }

    @Test
    void sameTopicTwiceIsRejected() {
        JobContext ctx = context(3);
        ctx.source(Topic.ORDER_BUNDLES, OrderSnapshot.class, EventFilter.all());
        assertThrows(IllegalStateException.class,
                () -> ctx.source(Topic.ORDER_BUNDLES, OrderSnapshot.class, EventFilter.all()));
    }

    @Test
    void unevenSourceParallelismIsRejected() {
        JobContext ctx = context(4);
        assertThrows(IllegalArgumentException.class,
                () -> ctx.source(Topic.ORDER_BUNDLES, OrderSnapshot.class, EventFilter.all()));
    }

    @Test
    void fullNameJoinsGroupAndName() {
        assertEquals("processing-business-realtime", JobRunner.fullName(job("processing", "business-realtime")));
        assertEquals("Kaizen-simulation-order", JobRunner.flinkJobName(job("simulation", "order")));
    }

    @Test
    void jobNamesAndGroupsAreChecked() {
        assertThrows(IllegalArgumentException.class, () -> JobRunner.fullName(job("processing", "BusinessRealtime")));
        assertThrows(IllegalArgumentException.class, () -> JobRunner.fullName(job("pipeline", "business-realtime")));
    }

    private static StreamJob job(String group, String name) {
        return new StreamJob() {
            @Override
            public String group() {
                return group;
            }

            @Override
            public String name() {
                return name;
            }

            @Override
            public void build(JobContext context) {
            }
        };
    }
}
