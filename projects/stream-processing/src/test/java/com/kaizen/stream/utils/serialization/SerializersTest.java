package com.kaizen.stream.utils.serialization;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.testing.Fixtures;
import com.kaizen.stream.utils.connector.sink.deadletter.DeadLetter;
import com.kaizen.stream.utils.connector.sink.deadletter.DeadLetterSerializer;
import com.kaizen.stream.utils.connector.sink.kafka.EnvelopeSerializer;
import com.kaizen.stream.utils.connector.source.kafka.RawRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SerializersTest {

    @Test
    void envelopeRecordHasKeyHeadersAndEventTime() {
        Envelope<OrderSnapshot> env = Fixtures.orderEnvelope(Fixtures.placedOrder());
        ProducerRecord<byte[], byte[]> r = new EnvelopeSerializer<OrderSnapshot>("order-bundles")
                .serialize(env, null, null);

        assertEquals("order-bundles", r.topic());
        assertEquals(Fixtures.ORDER_ID, new String(r.key(), StandardCharsets.UTF_8));
        assertEquals(Fixtures.T0, r.timestamp());
        Map<String, String> headers = EventHeaders.toMap(r.headers());
        assertEquals("ORDER_PLACED", headers.get(EventHeaders.EVENT_TYPE));
        assertEquals("2", headers.get(EventHeaders.SCHEMA_VERSION));
        assertEquals(env.getMeta().getEventId(), headers.get(EventHeaders.EVENT_ID));
        assertEquals("flink.simulation-order", headers.get(EventHeaders.SOURCE));
    }

    @Test
    void keylessEnvelopeIsRejected() {
        Envelope<OrderSnapshot> env = Fixtures.orderEnvelope(Fixtures.placedOrder());
        env.getMeta().setAggregateId("");
        assertThrows(IllegalArgumentException.class,
                () -> new EnvelopeSerializer<OrderSnapshot>("order-bundles").serialize(env, null, null));
    }

    @Test
    void deadLetterKeepsOriginalBytesAndAddsErrorHeaders() {
        RawRecord original = Fixtures.record(Fixtures.orderEnvelope(Fixtures.placedOrder()));
        String longMessage = new String(new char[800]).replace('\0', 'x');
        DeadLetter letter = DeadLetter.of(original, "kaizen.simulation-sales-writer.order",
                new InvalidEventException(longMessage), 123L);

        ProducerRecord<byte[], byte[]> r = new DeadLetterSerializer("order-bundles.dlq").serialize(letter, null, null);

        assertEquals("order-bundles.dlq", r.topic());
        assertArrayEquals(original.getKey(), r.key());
        assertArrayEquals(original.getValue(), r.value());
        Map<String, String> h = EventHeaders.toMap(r.headers());
        assertEquals("ORDER_PLACED", h.get(EventHeaders.EVENT_TYPE));
        assertEquals("order-bundles", h.get("dlq.sourceTopic"));
        assertEquals("2", h.get("dlq.partition"));
        assertEquals("41", h.get("dlq.offset"));
        assertEquals("kaizen.simulation-sales-writer.order", h.get("dlq.consumer"));
        assertEquals(InvalidEventException.class.getName(), h.get("dlq.errorClass"));
        assertEquals(500, h.get("dlq.errorMessage").length());
        assertEquals("123", h.get("dlq.failedAt"));
    }
}
