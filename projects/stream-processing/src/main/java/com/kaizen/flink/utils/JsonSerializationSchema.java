package com.kaizen.flink.utils;

import org.apache.flink.api.common.serialization.SerializationSchema;

import java.nio.charset.StandardCharsets;

/**
 * Proper SerializationSchema implementation thay cho lambda để tránh
 * type-erasure issues với Flink's generic type inference.
 */
public class JsonSerializationSchema<T> implements SerializationSchema<T> {
    @Override
    public byte[] serialize(T element) {
        return JsonUtils.toJson(element).getBytes(StandardCharsets.UTF_8);
    }
}
