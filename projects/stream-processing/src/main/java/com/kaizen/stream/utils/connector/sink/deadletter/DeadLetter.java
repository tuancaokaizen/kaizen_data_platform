package com.kaizen.stream.utils.connector.sink.deadletter;

import com.kaizen.stream.utils.connector.source.kafka.RawRecord;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Original record plus why it was rejected (kafka_topic_model.md 7.5).
 * Record gốc kèm lý do bị loại (kafka_topic_model.md mục 7.5).
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeadLetter implements Serializable {

    static final int MAX_MESSAGE_LENGTH = 500;

    private RawRecord record;
    /** Consumer group that rejected it. / Group đã loại message. */
    private String consumer;
    private String errorClass;
    private String errorMessage;
    private long failedAt;

    public static DeadLetter of(RawRecord record, String consumer, Throwable error, long failedAt) {
        String message = error.getMessage() == null ? "" : error.getMessage();
        if (message.length() > MAX_MESSAGE_LENGTH) {
            message = message.substring(0, MAX_MESSAGE_LENGTH);
        }
        return new DeadLetter(record, consumer, error.getClass().getName(), message, failedAt);
    }
}
