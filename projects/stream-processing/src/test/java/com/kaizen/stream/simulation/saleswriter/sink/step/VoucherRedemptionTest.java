package com.kaizen.stream.simulation.saleswriter.sink.step;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.testing.Fixtures;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VoucherRedemptionTest {

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
                case "executeUpdate":
                    calls.add("execute");
                    return 1;
                case "free":
                    calls.add("free");
                    return null;
                default:
                    return null;
            }
        });
    }

    @Test
    void redeemsOnlyOrdersWithACode() throws Exception {
        OrderSnapshot withCode = Fixtures.placedOrder();
        withCode.getOrder().setVoucherSeriesCode("SR_V10_0001");
        OrderSnapshot other = Fixtures.placedOrder();
        other.getOrder().setId("ORD-0");
        other.getOrder().setVoucherSeriesCode(null);
        List<Envelope<OrderSnapshot>> batch = List.of(Fixtures.orderEnvelope(other), Fixtures.orderEnvelope(withCode));

        new VoucherRedemption().apply(proxy(Connection.class), batch);

        assertEquals(List.of("array text [" + Fixtures.ORDER_ID + "]", "sql " + VoucherRedemption.SQL, "execute",
                "free"), calls);
    }

    @Test
    void batchWithoutCodesTouchesNothing() throws Exception {
        OrderSnapshot s = Fixtures.placedOrder();
        s.getOrder().setVoucherSeriesCode(null);

        new VoucherRedemption().apply(proxy(Connection.class), List.of(Fixtures.orderEnvelope(s)));

        assertTrue(calls.isEmpty());
    }
}
