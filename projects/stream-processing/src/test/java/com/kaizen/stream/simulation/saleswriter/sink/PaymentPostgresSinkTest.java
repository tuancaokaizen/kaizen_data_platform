package com.kaizen.stream.simulation.saleswriter.sink;

import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.model.payment.PaymentType;
import com.kaizen.stream.simulation.saleswriter.model.PaymentTables;
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

class PaymentPostgresSinkTest {

    private static List<SqlRow> rows(String table, PaymentSnapshot s) {
        for (TableWriter<PaymentSnapshot> t : PaymentPostgresSink.tables()) {
            if (t.upsert().table().endsWith("\"" + table + "\"")) {
                return t.rows(s);
            }
        }
        throw new AssertionError("No table " + table);
    }

    private static PaymentSnapshot refund() {
        PaymentSnapshot s = Fixtures.successReceipt();
        s.getPayment().setId("PAY-REFUND-1");
        s.getPayment().setType(PaymentType.REFUND);
        s.getPayment().setReturnId("RET-" + Fixtures.ORDER_ID + "-1");
        return s;
    }

    @Test
    void receiptHeaderAndLink() {
        PaymentSnapshot s = Fixtures.successReceipt();
        Map<String, Object> p = rows("Payment", s).get(0).values();
        assertEquals((short) 1, p.get("PaymentType"));
        assertEquals("SUCCESS", p.get("Status"));
        assertEquals((short) 1, p.get("Attempt"));
        assertEquals(305_000L, p.get("TotalPayment"));
        assertEquals(LocalDate.of(2026, 10, 11), p.get("PaymentDate"));
        assertEquals(null, p.get("ReturnId"));

        Map<String, Object> link = rows("OrderPayment", s).get(0).values();
        assertEquals(s.getPayment().getId(), link.get("Id"));
        assertEquals(s.getPayment().getId(), link.get("PaymentId"));
        assertEquals(Fixtures.ORDER_ID, link.get("OrderId"));
        assertTrue(rows("ReturnPayment", s).isEmpty());
    }

    @Test
    void refundLinksToItsReturn() {
        PaymentSnapshot s = refund();
        assertEquals((short) 2, rows("Payment", s).get(0).values().get("PaymentType"));
        assertTrue(rows("OrderPayment", s).isEmpty());
        Map<String, Object> link = rows("ReturnPayment", s).get(0).values();
        assertEquals("RET-" + Fixtures.ORDER_ID + "-1", link.get("ReturnId"));
        assertEquals(Fixtures.ORDER_ID, link.get("OrderId"));
    }

    @Test
    void transactionsAreStampedWhenThePaymentSucceeded() {
        PaymentSnapshot s = Fixtures.successReceipt();
        List<SqlRow> txns = rows("Transaction", s);
        assertEquals(2, txns.size());
        assertEquals((short) 3, txns.get(1).values().get("PaymentMethodId"));
        assertEquals(timestamptz(s.getPayment().getModifiedDate()), txns.get(0).values().get("CreatedDate"));
        assertEquals(timestamptz(s.getPayment().getModifiedDate()), txns.get(0).values().get("ModifiedDate"));
    }

    @Test
    void onlyHeaderIsVersioned() {
        assertTrue(PaymentTables.PAYMENT.sql().contains("DO UPDATE"));
        assertTrue(PaymentTables.TRANSACTION.sql().endsWith("DO NOTHING"));
        assertTrue(PaymentTables.ORDER_PAYMENT.sql().endsWith("DO NOTHING"));
        assertTrue(PaymentTables.RETURN_PAYMENT.sql().endsWith("DO NOTHING"));
    }
}
