package com.kaizen.stream.utils.serialization;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.order.OrderStatus;
import com.kaizen.stream.processing.rule.OrderRules;
import com.kaizen.stream.testing.Fixtures;
import com.kaizen.stream.utils.connector.sink.deadletter.DeadLetter;
import com.kaizen.stream.utils.connector.sink.deadletter.DeadLetterOutput;
import com.kaizen.stream.utils.connector.source.kafka.RawRecord;
import com.kaizen.stream.utils.event.EventValidator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnvelopeParserTest {

    private static final EventValidator<OrderSnapshot> ORDER_RULES =
            (meta, data) -> OrderRules.violations(meta.getEventType(), meta.getVersion(), data);

    private static OneInputStreamOperatorTestHarness<RawRecord, Envelope<OrderSnapshot>> harness() throws Exception {
        return ProcessFunctionTestHarnesses.forProcessFunction(
                new EnvelopeParser<>(OrderSnapshot.class, ORDER_RULES, "kaizen.test.order"));
    }

    @Test
    void validRecordGoesToMainOutput() throws Exception {
        try (OneInputStreamOperatorTestHarness<RawRecord, Envelope<OrderSnapshot>> h = harness()) {
            Envelope<OrderSnapshot> env = Fixtures.orderEnvelope(Fixtures.placedOrder());
            h.processElement(Fixtures.record(env), 0L);

            List<Envelope<OrderSnapshot>> out = h.extractOutputValues();
            assertEquals(1, out.size());
            assertEquals(env, out.get(0));
            assertEquals(null, h.getSideOutput(DeadLetterOutput.TAG));
        }
    }

    @Test
    void badRecordsGoToDeadLettersUnchanged() throws Exception {
        try (OneInputStreamOperatorTestHarness<RawRecord, Envelope<OrderSnapshot>> h = harness()) {
            RawRecord broken = Fixtures.record(Fixtures.ORDER_ID, "{oops", new HashMap<>());
            OrderSnapshot wrongStatus = Fixtures.partiallyReturnedOrder();
            wrongStatus.getOrder().setStatus(OrderStatus.COMPLETED);
            RawRecord invalid = Fixtures.record(Fixtures.orderEnvelope(wrongStatus));

            h.processElement(broken, 0L);
            h.processElement(invalid, 0L);

            assertTrue(h.extractOutputValues().isEmpty());
            ConcurrentLinkedQueue<StreamRecord<DeadLetter>> dead = h.getSideOutput(DeadLetterOutput.TAG);
            assertEquals(2, dead.size());
            DeadLetter first = dead.poll().getValue();
            assertArrayEquals(broken.getValue(), first.getRecord().getValue());
            assertEquals("kaizen.test.order", first.getConsumer());
            DeadLetter second = dead.poll().getValue();
            assertTrue(second.getErrorMessage().contains("returns imply PartiallyReturned"), second.getErrorMessage());
        }
    }
}
