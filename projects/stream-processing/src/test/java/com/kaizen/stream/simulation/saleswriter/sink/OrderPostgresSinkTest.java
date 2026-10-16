package com.kaizen.stream.simulation.saleswriter.sink;

import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.simulation.saleswriter.model.OrderTables;
import com.kaizen.stream.testing.Fixtures;
import com.kaizen.stream.utils.connector.sink.postgres.SqlRow;
import com.kaizen.stream.utils.connector.sink.postgres.TableWriter;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static com.kaizen.stream.utils.connector.sink.postgres.SqlRow.timestamptz;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderPostgresSinkTest {

    /** T0 = 2026-10-11 00:00 ICT. */
    private static final LocalDate DAY0 = LocalDate.of(2026, 10, 11);

    private static List<SqlRow> rows(String table, OrderSnapshot s) {
        // TableWriter.rows also checks every row has exactly the table's columns
        // TableWriter.rows còn kiểm tra mỗi dòng có đúng đủ cột của bảng
        for (TableWriter<OrderSnapshot> t : OrderPostgresSink.tables()) {
            if (t.upsert().table().endsWith("\"" + table + "\"")) {
                return t.rows(s);
            }
        }
        throw new AssertionError("No table " + table);
    }

    @Test
    void tablesAreWrittenParentFirst() {
        assertEquals(List.of("\"order\".\"Order\"", "\"order\".\"OrderItem\"", "\"order\".\"Return\"",
                        "\"order\".\"ReturnItem\""),
                OrderPostgresSink.tables().stream().map(t -> t.upsert().table()).collect(java.util.stream.Collectors.toList()));
        assertTrue(OrderTables.ORDER.sql().contains("DO UPDATE"));
        assertTrue(OrderTables.ORDER_ITEM.sql().endsWith("DO NOTHING"));
        assertTrue(OrderTables.RETURN.sql().endsWith("DO NOTHING"));
        assertTrue(OrderTables.RETURN_ITEM.sql().endsWith("DO NOTHING"));
    }

    @Test
    void headerFollowsTheLatestSnapshot() {
        Map<String, Object> o = rows("Order", Fixtures.partiallyReturnedOrder()).get(0).values();
        assertEquals(Fixtures.ORDER_ID, o.get("Id"));
        assertEquals("PartiallyReturned", o.get("Status"));
        assertEquals(DAY0, o.get("OrderDate"));
        assertEquals(305_000L, o.get("TotalPayment"));
        assertEquals(timestamptz(Fixtures.T0), o.get("CreatedDate"));
        assertEquals(timestamptz(Fixtures.T_RETURN), o.get("ModifiedDate"));
    }

    @Test
    void linesKeepCreationTimestamps() {
        List<SqlRow> items = rows("OrderItem", Fixtures.partiallyReturnedOrder());
        assertEquals(3, items.size());
        for (SqlRow r : items) {
            assertEquals(timestamptz(Fixtures.T0), r.values().get("ModifiedDate"));
            assertEquals(DAY0, r.values().get("OrderDate"));
        }
        assertEquals(true, items.get(2).values().get("IsGift"));
        assertEquals(false, items.get(0).values().get("IsGift"));
    }

    @Test
    void returnsUseTheirOwnDateAndTheOrderShop() {
        OrderSnapshot s = Fixtures.partiallyReturnedOrder();
        Map<String, Object> r = rows("Return", s).get(0).values();
        assertEquals("SHOP0001", r.get("ShopCode"));
        assertEquals(DAY0.plusDays(1), r.get("ReturnDate"));
        assertEquals(timestamptz(Fixtures.T_RETURN), r.get("ModifiedDate"));

        Map<String, Object> ri = rows("ReturnItem", s).get(0).values();
        assertEquals(Fixtures.ORDER_ID + "-L1", ri.get("OrderItemId"));
        assertEquals(Fixtures.ORDER_ID, ri.get("OrderId"));
        assertEquals(DAY0.plusDays(1), ri.get("ReturnDate"));
    }

    @Test
    void placedOrderHasNoReturnRows() {
        assertTrue(rows("Return", Fixtures.placedOrder()).isEmpty());
        assertTrue(rows("ReturnItem", Fixtures.placedOrder()).isEmpty());
    }
}
