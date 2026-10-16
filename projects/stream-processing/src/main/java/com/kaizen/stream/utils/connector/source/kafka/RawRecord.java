package com.kaizen.stream.utils.connector.source.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Kafka record kept byte-for-byte so a bad message can be copied to the DLQ unchanged.
 * Record Kafka giữ nguyên từng byte để message lỗi được chép sang DLQ không đổi.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RawRecord implements Serializable {

    private String topic;
    private int partition;
    private long offset;
    private long timestamp;
    private byte[] key;
    private byte[] value;
    private Map<String, String> headers;

    public String keyAsString() {
        return key == null ? null : new String(key, StandardCharsets.UTF_8);
    }

    public String header(String name) {
        return headers == null ? null : headers.get(name);
    }
}
