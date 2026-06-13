package com.kaizen.flink.sink;

import com.kaizen.flink.config.FlinkConfig;

public final class KafkaSink extends com.kaizen.flink.utils.KafkaSink {

    private KafkaSink() {}

    /** Sink nhận bất kỳ Object T và tự serialize sang JSON bytes. */
    public static <T> org.apache.flink.connector.kafka.sink.KafkaSink<T> buildObjectSink(FlinkConfig config, String topic) {
        return com.kaizen.flink.utils.KafkaSink.buildObjectSink(config, topic);
    }

    /** Sink nhận String (JSON đã serialized). */
    public static org.apache.flink.connector.kafka.sink.KafkaSink<String> buildStringSink(FlinkConfig config, String topic) {
        return com.kaizen.flink.utils.KafkaSink.buildStringSink(config, topic);
    }
}
