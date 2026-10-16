package com.kaizen.stream.utils.event;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.envelope.EventMeta;
import com.kaizen.stream.model.order.OrderEventType;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventIdsTest {

    @Test
    void matchesRfc4122Vector() {
        // Python: uuid.uuid5(uuid.NAMESPACE_DNS, "python.org")
        UUID dns = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
        assertEquals("886313e1-3b8a-5372-9b90-0c9aee199e5d", EventIds.uuidV5(dns, "python.org").toString());
    }

    @Test
    void sameStateGivesSameId() {
        String a = EventIds.of("SORD-1", "ORDER_PLACED", 1000L);
        assertEquals(a, EventIds.of("SORD-1", "ORDER_PLACED", 1000L));
        assertNotEquals(a, EventIds.of("SORD-1", "ORDER_PLACED", 1001L));
        assertNotEquals(a, EventIds.of("SORD-1", "ORDER_RETURNED", 1000L));
        assertEquals(5, UUID.fromString(a).version());
    }

    @Test
    void envelopeOfFillsMeta() {
        Envelope<String> e = Envelope.of(OrderEventType.ORDER_PLACED, "flink.test", "SORD-1", 1000L, 999L, "x");
        assertEquals("SORD-1", e.key());
        assertEquals(EventMeta.SCHEMA_VERSION, e.getMeta().getSchemaVersion());
        assertEquals(EventIds.of("SORD-1", "ORDER_PLACED", 1000L), e.getMeta().getEventId());
        assertEquals(999L, e.getMeta().getEventTime());
    }

    @Test
    void filterAcceptsListedTypesAndMissingHeader() {
        EventFilter only = EventFilter.only(OrderEventType.ORDER_RETURNED);
        assertTrue(only.accepts("ORDER_RETURNED"));
        assertFalse(only.accepts("ORDER_PLACED"));
        assertTrue(only.accepts(null));
        assertTrue(EventFilter.all().accepts("ANYTHING"));
    }
}
