package com.kaizen.stream.processing.outbox;

import com.kaizen.stream.testing.JobGraph;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OutboxRelayJobTest {

    @Test
    void everythingRunsAtParallelismOneToKeepSeqOrder() {
        Map<String, Integer> expected = new TreeMap<>(Map.of(
                "source.postgres.outbox", 1,
                "transform.outbox-decoder", 1,
                "sink.kafka.inventory-movements", 1,
                "sink.deadletter.inventory-movements", 1,
                "sink.kafka.logistics-events", 1,
                "sink.deadletter.logistics-events", 1));
        assertEquals(expected, JobGraph.parallelismByUid(new OutboxRelayJob()));
    }
}
