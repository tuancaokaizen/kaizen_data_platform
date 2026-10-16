package com.kaizen.stream.utils.connector.sink.kafka;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.utils.serialization.EnvelopeCodec;
import com.kaizen.stream.utils.serialization.EventHeaders;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

/**
 * Envelope → Kafka record: key = aggregateId, timestamp = eventTime, headers from meta.
 * Envelope → Kafka record: key = aggregateId, timestamp = eventTime, header lấy từ meta.
 */
public class EnvelopeSerializer<T> implements KafkaRecordSerializationSchema<Envelope<T>> {

    private static final long serialVersionUID = 1L;

    private final String topic;

    public EnvelopeSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Envelope<T> envelope, KafkaSinkContext context, Long timestamp) {
        String key = envelope.key();
        // Keyless records break per-order ordering / Record không key phá thứ tự theo đơn
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Refusing to write keyless record to " + topic
                    + " (eventType " + envelope.getMeta().getEventType() + ")");
        }
        return new ProducerRecord<>(topic, null, envelope.getMeta().getEventTime(),
                key.getBytes(StandardCharsets.UTF_8), EnvelopeCodec.encode(envelope),
                EventHeaders.fromMeta(envelope.getMeta()));
    }
}
