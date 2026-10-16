package com.kaizen.stream.utils.connector.sink.elasticsearch;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.utils.serialization.Json;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter;
import org.apache.flink.connector.elasticsearch.sink.RequestIndexer;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Turns an envelope into a scripted upsert that keeps the document with the highest meta.version.
 * The Flink ES sink fails the job on any rejected item (e.g. a 409 from external versioning),
 * so stale writes are turned into a no-op inside Elasticsearch instead.
 * Biến envelope thành scripted upsert giữ document có meta.version cao nhất.
 * Sink ES của Flink làm job lỗi khi có item bị từ chối (vd. 409 của external version),
 * nên bản ghi cũ được biến thành no-op ngay trong Elasticsearch.
 */
public final class VersionedUpsertEmitter<T> implements ElasticsearchEmitter<Envelope<T>> {

    private static final long serialVersionUID = 1L;

    static final String SCRIPT =
            "def m = ctx._source.meta;"
                    + " if (m != null && m.version != null && m.version > params.version) { ctx.op = 'noop'; }"
                    + " else { ctx._source.clear(); ctx._source.putAll(params.doc); }";

    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<Map<String, Object>>() {
    };

    /** Elasticsearch _id of an envelope. / _id Elasticsearch của một envelope. */
    @FunctionalInterface
    public interface DocId<T> extends Serializable {
        String of(Envelope<T> envelope);
    }

    private final String index;
    private final DocId<T> docId;

    public VersionedUpsertEmitter(String index, DocId<T> docId) {
        this.index = index;
        this.docId = docId;
    }

    @Override
    public void emit(Envelope<T> envelope, SinkWriter.Context context, RequestIndexer indexer) {
        indexer.add(request(envelope));
    }

    UpdateRequest request(Envelope<T> envelope) {
        String id = docId.of(envelope);
        if (id == null || id.isEmpty()) {
            throw new IllegalStateException("Empty document id for index " + index);
        }
        Map<String, Object> params = new HashMap<>();
        params.put("version", envelope.getMeta().getVersion());
        // Same JSON as on Kafka / Cùng JSON như trên Kafka
        params.put("doc", Json.mapper().convertValue(envelope, MAP));
        return new UpdateRequest(index, id)
                .script(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, SCRIPT, params))
                .scriptedUpsert(true)
                .upsert(Collections.emptyMap())
                .retryOnConflict(3);
    }
}
