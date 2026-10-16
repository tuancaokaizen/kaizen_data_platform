package com.kaizen.stream.utils.connector.source.kafka;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.config.Topic;
import com.kaizen.stream.utils.connection.KafkaClientProperties;
import com.kaizen.stream.utils.event.EventFilter;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Kafka sources with the approved offset policy (kafka_topic_model.md 9.2).
 * Source Kafka theo chính sách offset đã duyệt (kafka_topic_model.md mục 9.2).
 */
public final class KafkaSourceFactory {

    private KafkaSourceFactory() {
    }

    /**
     * Start from the group's committed offset, or EARLIEST when the group is new; sinks are idempotent.
     * Bắt đầu từ offset đã commit của group, group mới thì từ EARLIEST; sink idempotent nên an toàn.
     */
    public static KafkaSource<RawRecord> raw(StreamSettings.Kafka kafka, Topic topic, String groupId,
                                             EventFilter filter) {
        return KafkaSource.<RawRecord>builder()
                .setBootstrapServers(kafka.getBootstrapServers())
                .setTopics(topic.topicName())
                .setGroupId(groupId)
                .setClientIdPrefix(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new RawRecordDeserializer(filter))
                .setProperties(KafkaClientProperties.consumer())
                .build();
    }

    /**
     * Uneven partition → subtask spread (e.g. 6 over 4 = 2/2/1/1) is rejected at build time.
     * Chia partition không đều cho subtask (vd. 6 cho 4 = 2/2/1/1) bị từ chối khi dựng job.
     */
    public static void checkParallelism(Topic topic, int parallelism) {
        if (parallelism <= 0 || topic.partitions() % parallelism != 0) {
            throw new IllegalArgumentException(String.format(
                    "Source parallelism %d must divide %d partitions of %s",
                    parallelism, topic.partitions(), topic.topicName()));
        }
    }
}
