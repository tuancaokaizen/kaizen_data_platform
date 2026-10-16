package com.kaizen.stream.utils.serialization;

import com.kaizen.stream.model.envelope.EventMeta;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Kafka header names and conversions; headers duplicate meta so consumers can filter without parsing JSON.
 * Tên header Kafka và chuyển đổi; header lặp lại meta để lọc không cần parse JSON.
 */
public final class EventHeaders {

    public static final String EVENT_TYPE = "eventType";
    public static final String SCHEMA_VERSION = "schemaVersion";
    public static final String EVENT_ID = "eventId";
    public static final String SOURCE = "source";

    private EventHeaders() {
    }

    public static Headers fromMeta(EventMeta meta) {
        Headers headers = new RecordHeaders();
        put(headers, EVENT_TYPE, meta.getEventType());
        put(headers, SCHEMA_VERSION, String.valueOf(meta.getSchemaVersion()));
        put(headers, EVENT_ID, meta.getEventId());
        put(headers, SOURCE, meta.getSource());
        return headers;
    }

    public static Headers fromMap(Map<String, String> values) {
        Headers headers = new RecordHeaders();
        values.forEach((k, v) -> put(headers, k, v));
        return headers;
    }

    /** Last value wins for repeated header names. / Header trùng tên lấy giá trị cuối. */
    public static Map<String, String> toMap(Headers headers) {
        Map<String, String> out = new LinkedHashMap<>();
        for (Header header : headers) {
            out.put(header.key(), header.value() == null ? null
                    : new String(header.value(), StandardCharsets.UTF_8));
        }
        return out;
    }

    public static void put(Headers headers, String key, String value) {
        if (value != null) {
            headers.add(key, value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
