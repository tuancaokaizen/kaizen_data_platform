package com.kaizen.flink.sink;

import com.kaizen.flink.config.FlinkConfig;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;

public final class EsSink extends com.kaizen.flink.utils.EsSink {

    private EsSink() {
    }

    /**
     * Dành cho producer jobs: model T → JsonUtils.toJson(T) → ES
     */
    public static <T> ElasticsearchSink<T> build(FlinkConfig config, String indexName) {
        return com.kaizen.flink.utils.EsSink.build(config, indexName);
    }

    /**
     * Dành cho consumer jobs: String JSON (từ Kafka) → ES trực tiếp (không double-serialize)
     */
    public static ElasticsearchSink<String> buildStringIndexSink(FlinkConfig config, String indexName) {
        return com.kaizen.flink.utils.EsSink.buildStringIndexSink(config, indexName);
    }
}
