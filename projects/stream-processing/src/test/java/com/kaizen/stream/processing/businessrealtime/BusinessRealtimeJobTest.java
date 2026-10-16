package com.kaizen.stream.processing.businessrealtime;

import com.kaizen.stream.testing.JobGraph;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BusinessRealtimeJobTest {

    @Test
    void wiresSourcesJoinAndClickHouseSink() {
        // Same parallelism end to end keeps the sink chained and rows in emit order
        // Cùng parallelism từ đầu tới cuối để sink được chain và dòng giữ thứ tự phát
        Map<String, Integer> expected = new TreeMap<>(Map.of(
                "source.kafka.order-bundles", 3,
                "source.kafka.order-bundles.parse", 3,
                "sink.deadletter.order-bundles", 3,
                "source.kafka.payment-bundles", 3,
                "source.kafka.payment-bundles.parse", 3,
                "sink.deadletter.payment-bundles", 3,
                "transform.business-realtime-join", 3,
                "sink.clickhouse.realtime_business_by_order", 3));
        assertEquals(expected, JobGraph.parallelismByUid(new BusinessRealtimeJob()));
    }
}
