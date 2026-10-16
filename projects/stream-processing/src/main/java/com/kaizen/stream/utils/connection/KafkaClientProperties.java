package com.kaizen.stream.utils.connection;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Approved Kafka client settings (kafka_topic_model.md 8 and 8.1).
 * Cấu hình Kafka client đã duyệt (kafka_topic_model.md mục 8 và 8.1).
 */
public final class KafkaClientProperties {

    private KafkaClientProperties() {
    }

    public static Properties producer() {
        Properties props = new Properties();
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768");
        // One producer per sink subtask lives on the TM heap / Mỗi subtask sink có 1 producer trên heap TM
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "8388608");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
        return props;
    }

    /** Flink assigns partitions itself, so no assignment strategy is set. / Flink tự gán partition. */
    public static Properties consumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1048576");
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "262144");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        return props;
    }
}
