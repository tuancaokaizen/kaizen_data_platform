package com.kaizen.stream.utils.connector.source.kafka;

import com.kaizen.stream.utils.event.EventFilter;
import com.kaizen.stream.utils.serialization.EventHeaders;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

/**
 * Copies the Kafka record as-is and drops unwanted event types by header, before any JSON parsing.
 * Chép nguyên record Kafka và bỏ event type không cần theo header, trước khi parse JSON.
 */
public class RawRecordDeserializer implements KafkaRecordDeserializationSchema<RawRecord> {

    private static final long serialVersionUID = 1L;

    private final EventFilter filter;

    public RawRecordDeserializer(EventFilter filter) {
        this.filter = filter;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawRecord> out) {
        Map<String, String> headers = EventHeaders.toMap(record.headers());
        if (!filter.accepts(headers.get(EventHeaders.EVENT_TYPE))) {
            return;
        }
        out.collect(new RawRecord(record.topic(), record.partition(), record.offset(), record.timestamp(),
                record.key(), record.value(), headers));
    }

    @Override
    public TypeInformation<RawRecord> getProducedType() {
        return TypeInformation.of(RawRecord.class);
    }
}
