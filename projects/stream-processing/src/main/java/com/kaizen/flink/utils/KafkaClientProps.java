package com.kaizen.flink.utils;

import com.kaizen.flink.config.FlinkConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/** Shared Kafka client tuning for Flink connectors.
 *  Tune chung Kafka client cho Flink connector — giảm memory spike và tăng throughput.
 */
public final class KafkaClientProps {

    private KafkaClientProps() {
    }

    /** Producer props — lz4 batching / Producer: nén lz4 + batch nhỏ */
    public static Properties producerProps(FlinkConfig config) {
        Properties props = new Properties();
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        return props;
    }

    /** Consumer props — bounded fetch size / Consumer: giới hạn fetch để tránh OOM */
    public static Properties consumerProps(FlinkConfig config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "200");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "5242880");
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1048576");
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        return props;
    }

    /** Kafka operator parallelism aligned with topic partitions / Parallelism khớp số partition */
    public static int kafkaParallelism(FlinkConfig config) {
        int topicPartitions = config.getInt("kafka", "topic_partitions", 3);
        int configured = config.getInt("flink", "kafka_parallelism",
                config.getInt("flink", "parallelism", topicPartitions));
        return Math.min(configured, topicPartitions);
    }
}
