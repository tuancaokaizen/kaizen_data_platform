package com.kaizen.stream.simulation.payment;

import com.kaizen.stream.testing.JobGraph;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PaymentSimulationJobTest {

    @Test
    void wiresSchedulerAndKafkaSink() {
        Map<String, Integer> expected = new TreeMap<>(Map.of(
                "source.kafka.order-bundles", 3,
                "source.kafka.order-bundles.parse", 3,
                "sink.deadletter.order-bundles", 3,
                "transform.payment-scheduler", 3,
                "sink.kafka.payment-bundles", 3));
        assertEquals(expected, JobGraph.parallelismByUid(new PaymentSimulationJob()));
    }

    @Test
    void impossibleProfilesStopTheJobAtStartup() {
        assertThrows(IllegalStateException.class, () -> JobGraph.settings(
                Map.of("payment.success_rate", "0.9", "payment.failed_rate", "0.2")).payment());
        assertThrows(IllegalStateException.class, () -> JobGraph.settings(
                Map.of("payment.retry_delay_min_ms", "5", "payment.retry_delay_max_ms", "1")).payment());
    }
}
