package com.kaizen.stream.processing.outbox.transform;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutboxLedgerTest {

    @Test
    void rowsAreMarkedOnlyAfterTheirCheckpointCompletes() {
        OutboxLedger l = new OutboxLedger();
        l.emitted(1);
        l.emitted(2);
        assertEquals(Arrays.asList(1L, 2L), l.snapshot(10));
        l.emitted(3);

        // Nothing is delivered for sure yet / Chưa dòng nào chắc chắn đã gửi
        assertTrue(l.toMark().isEmpty());
        assertEquals(Arrays.asList(1L, 2L, 3L), l.inFlight());

        l.checkpointCompleted(10);
        assertEquals(Arrays.asList(1L, 2L), l.toMark());
        l.marked(l.toMark());
        assertEquals(List.of(3L), l.inFlight());
        assertTrue(l.toMark().isEmpty());
    }

    @Test
    void aLaterCheckpointConfirmsAnAbortedOne() {
        OutboxLedger l = new OutboxLedger();
        l.emitted(1);
        l.snapshot(10);
        l.emitted(2);
        l.snapshot(11);

        // Checkpoint 10 was aborted, 11 completes / Checkpoint 10 bị huỷ, 11 hoàn tất
        l.checkpointCompleted(11);
        assertEquals(Arrays.asList(1L, 2L), l.toMark());
    }

    @Test
    void restoredRowsWereDeliveredAndAreNotPolledAgain() {
        OutboxLedger l = new OutboxLedger();
        l.restore(Arrays.asList(7L, 8L));
        assertEquals(Arrays.asList(7L, 8L), l.inFlight());
        assertEquals(Arrays.asList(7L, 8L), l.toMark());
    }

    @Test
    void emittingTheSameRowTwiceIsIgnored() {
        OutboxLedger l = new OutboxLedger();
        l.emitted(5);
        l.emitted(5);
        l.snapshot(1);
        l.checkpointCompleted(1);
        assertEquals(List.of(5L), l.toMark());
    }
}
