package com.kaizen.stream.model.envelope;

import com.kaizen.stream.utils.event.EventIds;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Message value on every Kafka topic: meta + full aggregate snapshot.
 * Giá trị message trên mọi topic Kafka: meta + snapshot đầy đủ của aggregate.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Envelope<T> implements Serializable {

    private EventMeta meta;
    private T data;

    /**
     * Build a new envelope; eventId is derived so re-emitting the same state gives the same id.
     * Tạo envelope mới; eventId tất định nên phát lại cùng trạng thái cho ra cùng id.
     */
    public static <T> Envelope<T> of(Enum<?> eventType, String source, String aggregateId,
                                     long version, long eventTime, T data) {
        String type = eventType.name();
        EventMeta meta = new EventMeta(
                EventIds.of(aggregateId, type, version),
                type, EventMeta.SCHEMA_VERSION, source, eventTime, aggregateId, version);
        return new Envelope<>(meta, data);
    }

    /** Kafka record key. / Key của Kafka record. */
    @JsonIgnore
    public String key() {
        return meta.getAggregateId();
    }
}
