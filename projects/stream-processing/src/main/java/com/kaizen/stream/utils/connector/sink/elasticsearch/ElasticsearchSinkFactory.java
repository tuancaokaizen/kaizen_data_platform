package com.kaizen.stream.utils.connector.sink.elasticsearch;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.model.envelope.Envelope;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.connector.elasticsearch.sink.FlushBackoffType;
import org.apache.http.HttpHost;

import java.net.URI;
import java.util.Arrays;

/**
 * Elasticsearch sinks. Envelopes are written with {@link VersionedUpsertEmitter}, so replays never fail the job.
 * Sink Elasticsearch. Envelope được ghi bằng {@link VersionedUpsertEmitter} nên đọc lại không làm job lỗi.
 */
public final class ElasticsearchSinkFactory {

    private ElasticsearchSinkFactory() {
    }

    /**
     * One document per aggregate; an older snapshot never replaces a newer one.
     * Mỗi aggregate một document; snapshot cũ không bao giờ thay bản mới hơn.
     */
    public static <T> ElasticsearchSink<Envelope<T>> versionedUpsert(StreamSettings.Elasticsearch es, String index,
                                                                    VersionedUpsertEmitter.DocId<T> docId) {
        return new Elasticsearch7SinkBuilder<Envelope<T>>()
                .setHosts(hosts(es.getHosts()))
                .setEmitter(new VersionedUpsertEmitter<>(index, docId))
                .setBulkFlushMaxActions(es.getBulkMaxActions())
                .setBulkFlushInterval(es.getBulkIntervalMs())
                .setBulkFlushBackoffStrategy(FlushBackoffType.EXPONENTIAL, 3, 1_000L)
                .build();
    }

    static HttpHost[] hosts(String hosts) {
        return Arrays.stream(hosts.split(","))
                .map(String::trim)
                .filter(h -> !h.isEmpty())
                .map(h -> {
                    URI uri = URI.create(h);
                    if (uri.getHost() == null) {
                        throw new IllegalArgumentException("Elasticsearch host must be a URL like http://es:9200: " + h);
                    }
                    return new HttpHost(uri.getHost(), uri.getPort() > 0 ? uri.getPort() : 9200,
                            uri.getScheme() == null ? "http" : uri.getScheme());
                })
                .toArray(HttpHost[]::new);
    }
}
