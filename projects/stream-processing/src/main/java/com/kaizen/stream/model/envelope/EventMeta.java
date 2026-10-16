package com.kaizen.stream.model.envelope;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Envelope metadata, contract in kafka_topic_model.md 7.2.
 * Metadata của envelope, hợp đồng ở kafka_topic_model.md mục 7.2.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventMeta implements Serializable {

    public static final int SCHEMA_VERSION = 2;

    private String eventId;
    private String eventType;
    private Integer schemaVersion;
    private String source;
    /** Epoch ms (UTC) when the business event happened. / Thời điểm sự kiện nghiệp vụ. */
    private Long eventTime;
    /** Kafka key: orderId for order/payment topics. / Key Kafka: orderId cho topic order/payment. */
    private String aggregateId;
    /** ModifiedDate (epoch ms) of the snapshot; version for every sink. / Phiên bản dùng cho mọi sink. */
    private Long version;
}
