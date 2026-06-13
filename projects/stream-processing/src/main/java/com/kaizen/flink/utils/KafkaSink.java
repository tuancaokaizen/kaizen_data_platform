package com.kaizen.flink.utils;

import com.kaizen.flink.config.FlinkConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

public class KafkaSink {

    protected KafkaSink() {}

    /** Sink nhận bất kỳ Object T và tự serialize sang JSON bytes. */
    public static <T> org.apache.flink.connector.kafka.sink.KafkaSink<T> buildObjectSink(FlinkConfig config, String topic) {
        return org.apache.flink.connector.kafka.sink.KafkaSink.<T>builder()
                .setBootstrapServers(config.get("kafka", "bootstrap_servers"))
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<T>builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new JsonSerializationSchema<>())
                                .build()
                )
                .build();
    }

    /** Sink nhận String (JSON đã serialized). */
    public static org.apache.flink.connector.kafka.sink.KafkaSink<String> buildStringSink(FlinkConfig config, String topic) {
        return org.apache.flink.connector.kafka.sink.KafkaSink.<String>builder()
                .setBootstrapServers(config.get("kafka", "bootstrap_servers"))
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();
    }
}
