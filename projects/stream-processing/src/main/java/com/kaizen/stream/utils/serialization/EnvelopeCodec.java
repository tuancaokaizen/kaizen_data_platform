package com.kaizen.stream.utils.serialization;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.envelope.EventMeta;
import com.kaizen.stream.utils.connector.source.kafka.RawRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Envelope ↔ JSON bytes, with contract checks from kafka_topic_model.md 7.1–7.2.
 * Envelope ↔ JSON bytes, kèm kiểm tra hợp đồng theo kafka_topic_model.md 7.1–7.2.
 */
public final class EnvelopeCodec {

    private EnvelopeCodec() {
    }

    public static byte[] encode(Envelope<?> envelope) {
        return Json.toBytes(envelope);
    }

    /**
     * Parse and check meta, key and headers; throws InvalidEventException on any contract breach.
     * Parse và kiểm tra meta, key, header; vi phạm hợp đồng thì ném InvalidEventException.
     */
    public static <T> Envelope<T> decode(RawRecord record, Class<T> payload) {
        if (record.getValue() == null || record.getValue().length == 0) {
            throw new InvalidEventException("Empty value");
        }
        Envelope<T> envelope;
        try {
            JavaType type = Json.mapper().getTypeFactory().constructParametricType(Envelope.class, payload);
            envelope = Json.mapper().readValue(record.getValue(), type);
        } catch (JsonProcessingException e) {
            throw new InvalidEventException("Malformed JSON: " + e.getOriginalMessage(), e);
        } catch (IOException e) {
            throw new InvalidEventException("Unreadable value: " + e.getMessage(), e);
        }
        List<String> errors = contractViolations(envelope, record);
        if (!errors.isEmpty()) {
            throw new InvalidEventException(String.join("; ", errors));
        }
        return envelope;
    }

    static List<String> contractViolations(Envelope<?> envelope, RawRecord record) {
        List<String> errors = new ArrayList<>();
        EventMeta meta = envelope.getMeta();
        if (meta == null) {
            errors.add("meta is missing");
            return errors;
        }
        // Only schemaVersion 2 is supported / Chỉ hỗ trợ schemaVersion 2
        if (!Objects.equals(meta.getSchemaVersion(), EventMeta.SCHEMA_VERSION)) {
            errors.add("unsupported schemaVersion " + meta.getSchemaVersion());
        }
        requireText(errors, "meta.eventId", meta.getEventId());
        requireText(errors, "meta.eventType", meta.getEventType());
        requireText(errors, "meta.source", meta.getSource());
        requireText(errors, "meta.aggregateId", meta.getAggregateId());
        if (meta.getEventTime() == null) {
            errors.add("meta.eventTime is missing");
        }
        if (meta.getVersion() == null) {
            errors.add("meta.version is missing");
        }
        if (envelope.getData() == null) {
            errors.add("data is missing");
        }
        // Key and headers must agree with meta / Key và header phải khớp meta
        String key = record.keyAsString();
        if (key == null || key.isEmpty()) {
            errors.add("record key is empty");
        } else if (meta.getAggregateId() != null && !key.equals(meta.getAggregateId())) {
            errors.add("record key " + key + " != meta.aggregateId " + meta.getAggregateId());
        }
        String headerType = record.header(EventHeaders.EVENT_TYPE);
        if (headerType != null && meta.getEventType() != null && !headerType.equals(meta.getEventType())) {
            errors.add("header eventType " + headerType + " != meta.eventType " + meta.getEventType());
        }
        return errors;
    }

    private static void requireText(List<String> errors, String field, String value) {
        if (value == null || value.isEmpty()) {
            errors.add(field + " is missing");
        }
    }
}
