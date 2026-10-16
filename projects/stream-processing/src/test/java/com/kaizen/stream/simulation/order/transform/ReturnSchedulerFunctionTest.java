package com.kaizen.stream.simulation.order.transform;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderEventType;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.processing.rule.OrderRules;
import com.kaizen.stream.simulation.order.OrderTestData;
import com.kaizen.stream.simulation.order.lookup.MasterData;
import com.kaizen.stream.testing.Fixtures;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReturnSchedulerFunctionTest {

    private static final long DAY = 86_400_000L;
    private static final long AT = Fixtures.T0 + 3 * 3_600_000L;
    private static final String SOURCE = "flink.simulation-order";

    private final OrderGenerator generator = OrderTestData.generator();
    private final MasterData master = OrderTestData.master();

    private KeyedOneInputStreamOperatorTestHarness<String, Envelope<OrderSnapshot>, Envelope<OrderSnapshot>> harness;

    @BeforeEach
    void open() throws Exception {
        harness = newHarness();
        harness.open();
        harness.setProcessingTime(AT);
    }

    @AfterEach
    void close() throws Exception {
        harness.close();
    }

    @Test
    void placedOrdersPassThroughAndMostKeepNoState() throws Exception {
        OrderSnapshot keeper = firstOrder(false);

        place(keeper);

        List<Envelope<OrderSnapshot>> out = drain();
        assertEquals(1, out.size());
        assertEquals("ORDER_PLACED", out.get(0).getMeta().getEventType());
        assertEquals(0, harness.numProcessingTimeTimers());
        assertEquals(0, harness.numKeyedStateEntries());
    }

    @Test
    void theReturnIsEmittedWhenItsTimeComes() throws Exception {
        OrderSnapshot returner = firstOrder(true);
        OrderSnapshot expected = generator.planReturn(returner).orElseThrow();
        long due = expected.getOrder().getModifiedDate();

        place(returner);
        drain();
        harness.setProcessingTime(due - 1);
        assertTrue(drain().isEmpty(), "not before the return date");

        harness.setProcessingTime(due);
        List<Envelope<OrderSnapshot>> out = drain();

        assertEquals(1, out.size());
        Envelope<OrderSnapshot> e = out.get(0);
        assertEquals("ORDER_RETURNED", e.getMeta().getEventType());
        assertEquals(SOURCE, e.getMeta().getSource());
        assertEquals(due, e.getMeta().getVersion());
        assertEquals(expected, e.getData());
        assertEquals(List.of(), OrderRules.violations(e.getMeta().getEventType(), e.getMeta().getVersion(), e.getData()));
        assertEquals(0, harness.numProcessingTimeTimers());
    }

    @Test
    void replayedOrdersDoNotReturnTwice() throws Exception {
        OrderSnapshot returner = firstOrder(true);
        place(returner);
        harness.setProcessingTime(AT + 15 * DAY);
        assertEquals(2, drain().size());

        place(returner);
        harness.setProcessingTime(AT + 16 * DAY);

        List<Envelope<OrderSnapshot>> out = drain();
        assertEquals(1, out.size(), "only the replayed ORDER_PLACED");
        assertEquals("ORDER_PLACED", out.get(0).getMeta().getEventType());
    }

    @Test
    void pendingReturnsSurviveARestore() throws Exception {
        OrderSnapshot returner = firstOrder(true);
        place(returner);
        drain();
        OperatorSubtaskState snapshot = harness.snapshot(1L, AT);
        harness.close();

        harness = newHarness();
        harness.setup();
        harness.initializeState(snapshot);
        harness.open();
        harness.setProcessingTime(AT + 15 * DAY);

        List<Envelope<OrderSnapshot>> out = drain();
        assertEquals(1, out.size());
        assertEquals("ORDER_RETURNED", out.get(0).getMeta().getEventType());
    }

    private KeyedOneInputStreamOperatorTestHarness<String, Envelope<OrderSnapshot>, Envelope<OrderSnapshot>> newHarness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new ReturnSchedulerFunction(generator, 16, SOURCE)),
                e -> e.getData().getOrder().getId(),
                Types.STRING);
    }

    /** First generated order that will (or will not) be returned. / Đơn đầu tiên sẽ (hoặc không) bị trả. */
    private OrderSnapshot firstOrder(boolean returned) {
        for (int seq = 0; ; seq++) {
            OrderSnapshot s = generator.place(master, seq, AT, shop -> 1);
            if (generator.planReturn(s).isPresent() == returned) {
                return s;
            }
        }
    }

    private void place(OrderSnapshot s) throws Exception {
        harness.processElement(new StreamRecord<>(
                Envelope.of(OrderEventType.ORDER_PLACED, SOURCE, s.getOrder().getId(), AT, AT, s)));
    }

    @SuppressWarnings("unchecked")
    private List<Envelope<OrderSnapshot>> drain() {
        List<Envelope<OrderSnapshot>> out = harness.getOutput().stream()
                .map(o -> ((StreamRecord<Envelope<OrderSnapshot>>) o).getValue())
                .collect(Collectors.toList());
        harness.getOutput().clear();
        return out;
    }
}
