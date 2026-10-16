package com.kaizen.stream.processing.supplychainrealtime;

import com.kaizen.stream.testing.JobGraph;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SupplyChainRealtimeJobTest {

    @Test
    void wiresTwoIndependentBranches() {
        Map<String, Integer> expected = new TreeMap<>(Map.ofEntries(
                Map.entry("source.kafka.inventory-movements", 3),
                Map.entry("source.kafka.inventory-movements.parse", 3),
                Map.entry("sink.deadletter.inventory-movements", 3),
                Map.entry("transform.inventory-realtime", 3),
                Map.entry("sink.clickhouse.realtime_inventory_position", 3),
                Map.entry("source.kafka.logistics-events", 3),
                Map.entry("source.kafka.logistics-events.parse", 3),
                Map.entry("sink.deadletter.logistics-events", 3),
                Map.entry("transform.shipment-realtime", 3),
                Map.entry("sink.clickhouse.realtime_shipment_status", 3)));
        assertEquals(expected, JobGraph.parallelismByUid(new SupplyChainRealtimeJob()));
    }
}
