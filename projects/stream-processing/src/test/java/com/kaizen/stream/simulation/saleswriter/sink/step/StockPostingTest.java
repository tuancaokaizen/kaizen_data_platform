package com.kaizen.stream.simulation.saleswriter.sink.step;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.testing.Fixtures;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StockPostingTest {

    private final List<String> calls = new ArrayList<>();

    @SuppressWarnings("unchecked")
    private <T> T proxy(Class<T> type) {
        return (T) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{type}, (p, m, args) -> {
            switch (m.getName()) {
                case "createArrayOf":
                    calls.add("array " + args[0] + " " + Arrays.toString((Object[]) args[1]));
                    return proxy(Array.class);
                case "prepareStatement":
                    calls.add("sql " + args[0]);
                    return proxy(PreparedStatement.class);
                case "executeQuery":
                    calls.add("execute");
                    return proxy(ResultSet.class);
                case "next":
                    return true;
                case "free":
                    calls.add("free");
                    return null;
                default:
                    return null;
            }
        });
    }

    @Test
    void postsEachOrderOnceInIdOrder() throws Exception {
        OrderSnapshot a = Fixtures.placedOrder();
        OrderSnapshot b = Fixtures.placedOrder();
        b.getOrder().setId("ORD-0");
        List<Envelope<OrderSnapshot>> batch = List.of(Fixtures.orderEnvelope(a), Fixtures.orderEnvelope(b),
                Fixtures.orderEnvelope(Fixtures.partiallyReturnedOrder()));

        new StockPosting().apply(proxy(Connection.class), batch);

        assertEquals(List.of("array text [ORD-0, " + Fixtures.ORDER_ID + "]", "sql " + StockPosting.SQL, "execute",
                "free"), calls);
    }

    @Test
    void emptyBatchTouchesNothing() throws Exception {
        new StockPosting().apply(proxy(Connection.class), List.of());
        assertTrue(calls.isEmpty());
    }
}
