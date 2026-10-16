package com.kaizen.stream.config;

import com.kaizen.stream.utils.connector.source.kafka.ConsumerGroups;
import com.kaizen.stream.utils.connector.source.kafka.KafkaSourceFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicTest {

    /** Topic enum must agree with the script that creates the topics. / Enum phải khớp script tạo topic. */
    @Test
    void enumMatchesCreateTopicsScript() throws IOException {
        String script = new String(Files.readAllBytes(Paths.get("deploy/create_kafka_topics.sh")));
        Matcher m = Pattern.compile("\"([a-z.-]+)\\|(\\d+)\\|").matcher(script);
        Map<String, Integer> declared = new HashMap<>();
        while (m.find()) {
            declared.put(m.group(1), Integer.parseInt(m.group(2)));
        }
        for (Topic t : Topic.values()) {
            assertEquals(t.partitions(), declared.get(t.topicName()), t.topicName());
            assertTrue(declared.containsKey(t.deadLetterTopicName()), "missing DLQ topic " + t.deadLetterTopicName());
        }
    }

    @Test
    void groupNamesFollowConvention() {
        assertEquals("kaizen.processing-business-realtime.payment", ConsumerGroups.of("processing-business-realtime", Topic.PAYMENT_BUNDLES));
        assertThrows(IllegalArgumentException.class, () -> ConsumerGroups.of("BusinessRealtime", Topic.ORDER_BUNDLES));
        assertThrows(IllegalArgumentException.class, () -> ConsumerGroups.of("business realtime", Topic.ORDER_BUNDLES));
    }

    @Test
    void sourceParallelismMustDividePartitions() {
        assertDoesNotThrow(() -> KafkaSourceFactory.checkParallelism(Topic.ORDER_BUNDLES, 3));
        assertDoesNotThrow(() -> KafkaSourceFactory.checkParallelism(Topic.LOGISTICS_EVENTS, 3));
        assertThrows(IllegalArgumentException.class, () -> KafkaSourceFactory.checkParallelism(Topic.ORDER_BUNDLES, 4));
        assertThrows(IllegalArgumentException.class, () -> KafkaSourceFactory.checkParallelism(Topic.LOGISTICS_EVENTS, 2));
        assertThrows(IllegalArgumentException.class, () -> KafkaSourceFactory.checkParallelism(Topic.ORDER_BUNDLES, 0));
    }
}
