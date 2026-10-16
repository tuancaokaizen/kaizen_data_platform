package com.kaizen.stream.model.outbox;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * One inventory."Outbox" row = one Kafka message to publish.
 * Một dòng inventory."Outbox" = một message Kafka cần phát.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxRow implements Serializable {
    private long seq;
    private String topic;
    private String aggregateId;
    private String eventType;
    private long version;
    /** Payload JSON as stored in PG. / JSON payload như lưu trong PG. */
    private String payload;
    private long eventTime;
}
