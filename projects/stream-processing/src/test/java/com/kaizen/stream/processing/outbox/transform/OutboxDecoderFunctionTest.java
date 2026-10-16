package com.kaizen.stream.processing.outbox.transform;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.inventory.StockMovementEvent;
import com.kaizen.stream.model.logistics.ShipmentEvent;
import com.kaizen.stream.model.outbox.OutboxRow;
import com.kaizen.stream.utils.connector.sink.deadletter.DeadLetter;
import com.kaizen.stream.utils.event.EventIds;
import com.kaizen.stream.utils.serialization.Json;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutboxDecoderFunctionTest {

    private static final long T = 1_790_000_000_000L;

    private OneInputStreamOperatorTestHarness<OutboxRow, Envelope<StockMovementEvent>> harness;

    @BeforeEach
    void open() throws Exception {
        harness = ProcessFunctionTestHarnesses.forProcessFunction(new OutboxDecoderFunction("flink.processing-outbox-relay",
                "kaizen.processing-outbox-relay"));
    }

    @AfterEach
    void close() throws Exception {
        harness.close();
    }

    static StockMovementEvent movement() {
        return StockMovementEvent.builder()
                .movementId("SM-ORDER-L1-SALE-1").seq(42L).locationType("SHOP").locationCode("SHOP0001")
                .productId("PROD0001").lotId("LOT-OPEN-PROD0001").movementType("SALE").qty(-2).onHandAfter(5)
                .unitCost(9_000L).refType("ORDER").refId("O1").refLineId("L1").occurredAt(T)
                .build();
    }

    static OutboxRow inventoryRow(String payload) {
        return new OutboxRow(7, "inventory-movements", "SHOP0001:PROD0001", "SALE", 42, payload, T);
    }

    @Test
    void inventoryRowBecomesADeterministicEnvelope() throws Exception {
        harness.processElement(inventoryRow(Json.mapper().writeValueAsString(movement())), T);

        List<Envelope<StockMovementEvent>> out = harness.extractOutputValues();
        assertEquals(1, out.size());
        Envelope<StockMovementEvent> e = out.get(0);
        assertEquals("SHOP0001:PROD0001", e.key());
        assertEquals(42L, e.getMeta().getVersion());
        assertEquals(EventIds.of("SHOP0001:PROD0001", "SALE", 42), e.getMeta().getEventId());
        assertEquals(movement(), e.getData());
    }

    @Test
    void logisticsRowGoesToItsSideOutput() throws Exception {
        ShipmentEvent planned = ShipmentEvent.builder()
                .shipmentId("SHP-1").eventSeq(1).eventType("SHIPMENT_PLANNED").status("PLANNED").originCode("DC01")
                .plannedDeparture(T).stopCount(2).occurredAt(T).build();
        harness.processElement(new OutboxRow(8, "logistics-events", "SHP-1", "SHIPMENT_PLANNED", 1,
                Json.mapper().writeValueAsString(planned), T), T);

        assertTrue(harness.extractOutputValues().isEmpty());
        Queue<StreamRecord<Envelope<ShipmentEvent>>> side = harness.getSideOutput(OutboxDecoderFunction.LOGISTICS_EVENTS);
        assertEquals(1, side.size());
        assertEquals("SHP-1", side.peek().getValue().key());
        assertEquals(planned, side.peek().getValue().getData());
    }

    @Test
    void badRowsGoToTheDlqOfTheirTopic() throws Exception {
        harness.processElement(inventoryRow("{not json"), T);
        harness.processElement(inventoryRow(Json.mapper().writeValueAsString(movement().toBuilder().qty(3).build())), T);
        harness.processElement(new OutboxRow(9, "logistics-events", "SHP-1", "DEPARTED", 1, "{}", T), T);

        assertTrue(harness.extractOutputValues().isEmpty());
        Queue<StreamRecord<DeadLetter>> inventory = harness.getSideOutput(OutboxDecoderFunction.INVENTORY_DEAD_LETTERS);
        assertEquals(2, inventory.size());
        DeadLetter first = inventory.peek().getValue();
        assertEquals("inventory-movements", first.getRecord().getTopic());
        assertEquals(7L, first.getRecord().getOffset());
        assertEquals("kaizen.processing-outbox-relay", first.getConsumer());
        assertEquals("SHOP0001:PROD0001", new String(first.getRecord().getKey(), StandardCharsets.UTF_8));
        assertEquals(1, harness.getSideOutput(OutboxDecoderFunction.LOGISTICS_DEAD_LETTERS).size());
        assertNull(harness.getSideOutput(OutboxDecoderFunction.LOGISTICS_EVENTS));
    }
}
