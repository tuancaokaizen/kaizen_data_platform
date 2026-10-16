package com.kaizen.stream.utils.serialization;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.order.OrderStatus;
import com.kaizen.stream.testing.Fixtures;
import com.kaizen.stream.utils.connector.source.kafka.RawRecord;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnvelopeCodecTest {

    @Test
    void roundTripKeepsContractFieldNames() throws Exception {
        Envelope<OrderSnapshot> env = Fixtures.orderEnvelope(Fixtures.partiallyReturnedOrder());
        RawRecord record = Fixtures.record(env);

        JsonNode json = Json.mapper().readTree(record.getValue());
        assertEquals("PartiallyReturned", json.path("data").path("order").path("status").asText());
        assertTrue(json.path("data").path("items").get(0).has("isGift"));
        assertFalse(json.path("data").path("items").get(0).has("gift"));
        assertFalse(json.has("key"));

        Envelope<OrderSnapshot> back = EnvelopeCodec.decode(record, OrderSnapshot.class);
        assertEquals(env, back);
        assertEquals(OrderStatus.PARTIALLY_RETURNED, back.getData().getOrder().getStatus());
    }

    @Test
    void malformedJsonIsInvalidEvent() {
        RawRecord r = Fixtures.record(Fixtures.ORDER_ID, "{not json", new HashMap<>());
        InvalidEventException e = assertThrows(InvalidEventException.class,
                () -> EnvelopeCodec.decode(r, OrderSnapshot.class));
        assertTrue(e.getMessage().startsWith("Malformed JSON"), e.getMessage());
    }

    @Test
    void unknownStatusIsInvalidEvent() throws Exception {
        String json = new String(EnvelopeCodec.encode(Fixtures.orderEnvelope(Fixtures.placedOrder())))
                .replace("\"Completed\"", "\"Done\"");
        RawRecord r = Fixtures.record(Fixtures.ORDER_ID, json, new HashMap<>());
        assertThrows(InvalidEventException.class, () -> EnvelopeCodec.decode(r, OrderSnapshot.class));
    }

    @Test
    void contractBreachesAreListed() {
        Envelope<OrderSnapshot> env = Fixtures.orderEnvelope(Fixtures.placedOrder());
        env.getMeta().setSchemaVersion(1);
        env.getMeta().setVersion(null);
        Map<String, String> headers = new HashMap<>();
        headers.put(EventHeaders.EVENT_TYPE, "ORDER_RETURNED");
        RawRecord r = Fixtures.record("OTHER-KEY", new String(EnvelopeCodec.encode(env)), headers);

        InvalidEventException e = assertThrows(InvalidEventException.class,
                () -> EnvelopeCodec.decode(r, OrderSnapshot.class));
        assertTrue(e.getMessage().contains("unsupported schemaVersion 1"), e.getMessage());
        assertTrue(e.getMessage().contains("meta.version is missing"), e.getMessage());
        assertTrue(e.getMessage().contains("record key OTHER-KEY"), e.getMessage());
        assertTrue(e.getMessage().contains("header eventType ORDER_RETURNED"), e.getMessage());
    }

    @Test
    void emptyValueAndMissingKeyAreRejected() {
        RawRecord empty = Fixtures.record(Fixtures.ORDER_ID, "", new HashMap<>());
        assertThrows(InvalidEventException.class, () -> EnvelopeCodec.decode(empty, OrderSnapshot.class));

        String json = new String(EnvelopeCodec.encode(Fixtures.orderEnvelope(Fixtures.placedOrder())));
        RawRecord keyless = Fixtures.record(null, json, new HashMap<>());
        InvalidEventException e = assertThrows(InvalidEventException.class,
                () -> EnvelopeCodec.decode(keyless, OrderSnapshot.class));
        assertTrue(e.getMessage().contains("record key is empty"));
    }
}
