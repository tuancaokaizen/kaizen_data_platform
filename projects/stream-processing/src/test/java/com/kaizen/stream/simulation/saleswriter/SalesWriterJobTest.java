package com.kaizen.stream.simulation.saleswriter;

import com.kaizen.stream.testing.JobGraph;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SalesWriterJobTest {

    @Test
    void sinksRunAtSourceParallelismToKeepPartitionOrder() {
        Map<String, Integer> expected = new TreeMap<>(Map.of(
                "source.kafka.order-bundles", 3,
                "source.kafka.order-bundles.parse", 3,
                "sink.deadletter.order-bundles", 3,
                "sink.postgres.order", 3,
                "source.kafka.payment-bundles", 3,
                "source.kafka.payment-bundles.parse", 3,
                "sink.deadletter.payment-bundles", 3,
                "sink.postgres.payment", 3));
        assertEquals(expected, JobGraph.parallelismByUid(new SalesWriterJob()));
    }
}
