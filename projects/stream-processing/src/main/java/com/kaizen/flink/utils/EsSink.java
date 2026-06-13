package com.kaizen.flink.utils;

import com.kaizen.flink.config.FlinkConfig;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.net.URI;

public class EsSink {

    protected EsSink() {}

    /** Dành cho producer jobs: model T → JsonUtils.toJson(T) → ES */
    public static <T> ElasticsearchSink<T> build(FlinkConfig config, String indexName) {
        Elasticsearch7SinkBuilder<T> builder = new Elasticsearch7SinkBuilder<T>()
                .setHosts(parseHost(config))
                .setEmitter((element, context, indexer) -> {
                    String json = JsonUtils.toJson(element);
                    indexer.add(buildIndexRequest(indexName, json));
                });
        return builder.build();
    }

    /** Dành cho consumer jobs: String JSON (từ Kafka) → ES trực tiếp (không double-serialize) */
    public static ElasticsearchSink<String> buildStringIndexSink(FlinkConfig config, String indexName) {
        Elasticsearch7SinkBuilder<String> builder = new Elasticsearch7SinkBuilder<String>()
                .setHosts(parseHost(config))
                .setEmitter((json, context, indexer) ->
                        indexer.add(buildIndexRequest(indexName, json)));
        return builder.build();
    }

    private static HttpHost parseHost(FlinkConfig config) {
        String hostsStr = config.getOrDefault("elasticsearch", "hosts", "http://localhost:9200");
        URI uri = URI.create(hostsStr);
        return new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
    }

    private static IndexRequest buildIndexRequest(String indexName, String json) {
        return Requests.indexRequest()
                .index(indexName)
                .source(json, XContentType.JSON);
    }
}
