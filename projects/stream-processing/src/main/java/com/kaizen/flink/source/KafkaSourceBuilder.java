package com.kaizen.flink.source;

import com.kaizen.flink.config.FlinkConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * Builder helper cho KafkaSource<String>.
 * Mọi consumer job đều dùng String JSON, deserialization xảy ra sau.
 */
public class KafkaSourceBuilder {

    private KafkaSourceBuilder() {}

    /** Source với group ID mặc định từ config. */
    public static KafkaSource<String> buildKafkaSource(FlinkConfig config, String topic) {
        return buildKafkaSource(config, topic, config.getOrDefault("kafka", "group_id", "kaizen-stream-group"));
    }

    /** Source với group ID tùy chỉnh – cần thiết để nhiều consumer group độc lập nhau. */
    public static KafkaSource<String> buildKafkaSource(FlinkConfig config, String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(config.get("kafka", "bootstrap_servers"))
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}
