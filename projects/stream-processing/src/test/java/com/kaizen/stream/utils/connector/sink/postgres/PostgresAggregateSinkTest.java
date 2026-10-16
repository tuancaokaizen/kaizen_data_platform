package com.kaizen.stream.utils.connector.sink.postgres;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.testing.Fixtures;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresAggregateSinkTest {

    @Test
    void bufferKeepsNewestSnapshotPerKey() {
        Envelope<OrderSnapshot> placed = Fixtures.orderEnvelope(Fixtures.placedOrder());
        Envelope<OrderSnapshot> returned = Fixtures.orderEnvelope(Fixtures.partiallyReturnedOrder());
        LatestByKey<OrderSnapshot> buffer = new LatestByKey<>();

        buffer.add("A", returned);
        buffer.add("A", placed);
        buffer.add("B", placed);
        assertEquals(2, buffer.size());

        List<Envelope<OrderSnapshot>> drained = buffer.drain();
        assertSame(returned, drained.get(0));
        assertSame(placed, drained.get(1));
        assertTrue(buffer.isEmpty());
    }

    @Test
    void equalVersionReplaces() {
        Envelope<OrderSnapshot> first = Fixtures.orderEnvelope(Fixtures.placedOrder());
        Envelope<OrderSnapshot> resend = Fixtures.orderEnvelope(Fixtures.placedOrder());
        LatestByKey<OrderSnapshot> buffer = new LatestByKey<>();
        buffer.add("A", first);
        buffer.add("A", resend);
        assertSame(resend, buffer.drain().get(0));
    }

    @Test
    void onlyConnectionAndResourceErrorsAreRetried() {
        assertTrue(PostgresAggregateSink.isTransient(new SQLException("io", "08006")));
        assertTrue(PostgresAggregateSink.isTransient(new SQLException("deadlock", "40P01")));
        assertTrue(PostgresAggregateSink.isTransient(new SQLException("too many connections", "53300")));
        assertTrue(PostgresAggregateSink.isTransient(new SQLException("admin shutdown", "57P01")));
        assertTrue(PostgresAggregateSink.isTransient(new SQLException("unknown")));
        assertFalse(PostgresAggregateSink.isTransient(new SQLException("check", "23514")));
        assertFalse(PostgresAggregateSink.isTransient(new SQLException("fk", "23503")));
        assertFalse(PostgresAggregateSink.isTransient(new SQLException("bad type", "22P02")));

        SQLException batch = new SQLException("batch aborted", "08006");
        batch.setNextException(new SQLException("check", "23514"));
        assertFalse(PostgresAggregateSink.isTransient(batch));
    }

    @Test
    void sinkNeedsTables() {
        assertThrows(IllegalArgumentException.class,
                () -> new PostgresAggregateSink<OrderSnapshot>("x", null, Envelope::key, List.of()));
    }
}
