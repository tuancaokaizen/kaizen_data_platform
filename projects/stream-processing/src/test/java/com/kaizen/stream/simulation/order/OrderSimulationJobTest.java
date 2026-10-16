package com.kaizen.stream.simulation.order;

import com.kaizen.stream.testing.JobGraph;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OrderSimulationJobTest {

    @Test
    void singleGeneratorFeedsKeyedReturnsAndKafka() {
        // One generator instance keeps sequence numbers unique / Một instance bộ sinh giữ số thứ tự không trùng
        Map<String, Integer> expected = new TreeMap<>(Map.of(
                "source.generator.order", 1,
                "transform.return-scheduler", 3,
                "sink.kafka.order-bundles", 3));
        assertEquals(expected, JobGraph.parallelismByUid(new OrderSimulationJob()));
    }

    @Test
    void impossibleProfilesStopTheJobAtStartup() {
        assertThrows(IllegalStateException.class, () -> JobGraph.settings(Map.of("order.walk_in_rate", "1.5")).order());
        assertThrows(IllegalStateException.class, () -> JobGraph.settings(Map.of("order.state_days", "10")).order());
        assertThrows(IllegalStateException.class, () -> JobGraph.settings(Map.of("order.orders_per_day", "0")).order());
    }
}
