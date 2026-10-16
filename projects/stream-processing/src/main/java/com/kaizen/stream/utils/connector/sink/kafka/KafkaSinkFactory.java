package com.kaizen.stream.utils.connector.sink.kafka;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.config.Topic;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.utils.connection.KafkaClientProperties;
import com.kaizen.stream.utils.connector.sink.deadletter.DeadLetter;
import com.kaizen.stream.utils.connector.sink.deadletter.DeadLetterSerializer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

/**
 * Kafka sinks with the approved producer settings; at-least-once is enough because sinks are idempotent.
 * Sink Kafka theo cấu hình producer đã duyệt; at-least-once là đủ vì sink phía sau idempotent.
 */
public final class KafkaSinkFactory {

    private KafkaSinkFactory() {
    }

    public static <T> KafkaSink<Envelope<T>> envelopes(StreamSettings.Kafka kafka, Topic topic) {
        return build(kafka, new EnvelopeSerializer<>(topic.topicName()));
    }

    public static KafkaSink<DeadLetter> deadLetters(StreamSettings.Kafka kafka, Topic topic) {
        return build(kafka, new DeadLetterSerializer(topic.deadLetterTopicName()));
    }

    private static <T> KafkaSink<T> build(StreamSettings.Kafka kafka, KafkaRecordSerializationSchema<T> serializer) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(kafka.getBootstrapServers())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(serializer)
                .setKafkaProducerConfig(KafkaClientProperties.producer())
                .build();
    }
}
