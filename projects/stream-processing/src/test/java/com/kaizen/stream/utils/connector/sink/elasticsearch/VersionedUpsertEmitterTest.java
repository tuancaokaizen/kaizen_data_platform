package com.kaizen.stream.utils.connector.sink.elasticsearch;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.testing.Fixtures;
import org.apache.http.HttpHost;
import org.elasticsearch.action.update.UpdateRequest;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VersionedUpsertEmitterTest {

    @Test
    @SuppressWarnings("unchecked")
    void buildsScriptedUpsertCarryingTheWholeEnvelope() {
        Envelope<OrderSnapshot> envelope = Fixtures.orderEnvelope(Fixtures.partiallyReturnedOrder());
        UpdateRequest r = new VersionedUpsertEmitter<OrderSnapshot>("order-bundles", Envelope::key)
                .request(envelope);

        assertEquals("order-bundles", r.index());
        assertEquals(Fixtures.ORDER_ID, r.id());
        assertTrue(r.scriptedUpsert());
        assertEquals(3, r.retryOnConflict());
        assertTrue(r.upsertRequest().sourceAsMap().isEmpty());
        assertEquals(VersionedUpsertEmitter.SCRIPT, r.script().getIdOrCode());

        Map<String, Object> params = r.script().getParams();
        assertEquals(Fixtures.T_RETURN, params.get("version"));
        Map<String, Object> doc = (Map<String, Object>) params.get("doc");
        Map<String, Object> meta = (Map<String, Object>) doc.get("meta");
        Map<String, Object> order = (Map<String, Object>) ((Map<String, Object>) doc.get("data")).get("order");
        assertEquals("ORDER_RETURNED", meta.get("eventType"));
        assertEquals("PartiallyReturned", order.get("status"));
    }

    @Test
    void staleSnapshotsAreNoOps() {
        // The guard must compare stored vs incoming version / Phải so version đã lưu với version mới tới
        assertTrue(VersionedUpsertEmitter.SCRIPT.contains("m.version > params.version"));
        assertTrue(VersionedUpsertEmitter.SCRIPT.contains("ctx.op = 'noop'"));
    }

    @Test
    void rejectsEmptyId() {
        Envelope<OrderSnapshot> envelope = Fixtures.orderEnvelope(Fixtures.placedOrder());
        assertThrows(IllegalStateException.class,
                () -> new VersionedUpsertEmitter<OrderSnapshot>("order-bundles", e -> "").request(envelope));
    }

    @Test
    void parsesHostList() {
        assertArrayEquals(new HttpHost[]{
                        new HttpHost("es-0", 9200, "http"), new HttpHost("es-1", 9201, "https")},
                ElasticsearchSinkFactory.hosts("http://es-0:9200, https://es-1:9201"));
        assertArrayEquals(new HttpHost[]{new HttpHost("es", 9200, "http")}, ElasticsearchSinkFactory.hosts("http://es"));
        assertThrows(IllegalArgumentException.class, () -> ElasticsearchSinkFactory.hosts("es:9200"));
    }
}
