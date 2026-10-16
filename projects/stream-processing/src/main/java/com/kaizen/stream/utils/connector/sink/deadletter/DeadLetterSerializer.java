package com.kaizen.stream.utils.connector.sink.deadletter;

import com.kaizen.stream.utils.connector.source.kafka.RawRecord;
import com.kaizen.stream.utils.serialization.EventHeaders;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

import java.util.Collections;

/**
 * Writes the original key/value unchanged; error details go into dlq.* headers.
 * Ghi nguyên key/value gốc; chi tiết lỗi nằm trong header dlq.*.
 */
public class DeadLetterSerializer implements KafkaRecordSerializationSchema<DeadLetter> {

    private static final long serialVersionUID = 1L;

    private final String dlqTopic;

    public DeadLetterSerializer(String dlqTopic) {
        this.dlqTopic = dlqTopic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(DeadLetter letter, KafkaSinkContext context, Long timestamp) {
        RawRecord record = letter.getRecord();
        // Keep original headers, then append error info / Giữ header gốc rồi thêm thông tin lỗi
        Headers headers = EventHeaders.fromMap(
                record.getHeaders() == null ? Collections.emptyMap() : record.getHeaders());
        EventHeaders.put(headers, "dlq.sourceTopic", record.getTopic());
        EventHeaders.put(headers, "dlq.partition", String.valueOf(record.getPartition()));
        EventHeaders.put(headers, "dlq.offset", String.valueOf(record.getOffset()));
        EventHeaders.put(headers, "dlq.consumer", letter.getConsumer());
        EventHeaders.put(headers, "dlq.errorClass", letter.getErrorClass());
        EventHeaders.put(headers, "dlq.errorMessage", letter.getErrorMessage());
        EventHeaders.put(headers, "dlq.failedAt", String.valueOf(letter.getFailedAt()));
        return new ProducerRecord<>(dlqTopic, null, letter.getFailedAt(),
                record.getKey(), record.getValue(), headers);
    }
}
