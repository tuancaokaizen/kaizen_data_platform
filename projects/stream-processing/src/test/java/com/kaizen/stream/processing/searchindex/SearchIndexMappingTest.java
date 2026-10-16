package com.kaizen.stream.processing.searchindex;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.payment.PaymentEventType;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.model.payment.PaymentType;
import com.kaizen.stream.testing.Fixtures;
import com.kaizen.stream.utils.serialization.Json;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Mappings use dynamic=false, so a field missing from the mapping is silently not searchable; keep them equal.
 * Mapping dùng dynamic=false nên trường thiếu trong mapping sẽ âm thầm không tìm được; hai bên phải bằng nhau.
 */
class SearchIndexMappingTest {

    private static final String DIR = "../../schema_manager/elasticsearch/";

    @Test
    void orderMappingMatchesEnvelope() throws Exception {
        Set<String> doc = new TreeSet<>();
        leaves(tree(Fixtures.orderEnvelope(Fixtures.placedOrder())), "", doc);
        leaves(tree(Fixtures.orderEnvelope(Fixtures.partiallyReturnedOrder())), "", doc);
        assertEquals(mapped("order-bundles.json"), doc);
    }

    @Test
    void paymentMappingMatchesEnvelope() throws Exception {
        PaymentSnapshot refund = Fixtures.successReceipt();
        refund.getPayment().setType(PaymentType.REFUND);
        refund.getPayment().setReturnId("RET-1");
        Set<String> doc = new TreeSet<>();
        leaves(tree(payment(Fixtures.successReceipt())), "", doc);
        leaves(tree(payment(refund)), "", doc);
        assertEquals(mapped("payment-bundles.json"), doc);
    }

    private static Envelope<PaymentSnapshot> payment(PaymentSnapshot s) {
        return Envelope.of(PaymentEventType.of(s.getPayment().getType(), s.getPayment().getStatus()),
                "flink.simulation-payment", s.getPayment().getOrderId(), s.getPayment().getModifiedDate(),
                s.getPayment().getModifiedDate(), s);
    }

    private static JsonNode tree(Envelope<?> envelope) {
        return Json.mapper().valueToTree(envelope);
    }

    /** Dotted paths of non-null leaves; arrays are flattened. / Đường dẫn các lá khác null; mảng được làm phẳng. */
    private static void leaves(JsonNode node, String path, Set<String> out) {
        if (node.isArray()) {
            node.forEach(n -> leaves(n, path, out));
        } else if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> it = node.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> f = it.next();
                leaves(f.getValue(), path.isEmpty() ? f.getKey() : path + "." + f.getKey(), out);
            }
        } else if (!node.isNull()) {
            out.add(path);
        }
    }

    private static Set<String> mapped(String file) throws Exception {
        JsonNode mappings = Json.mapper().readTree(Paths.get(DIR + file).toFile()).get("mappings");
        assertEquals(false, mappings.get("dynamic").asBoolean(true));
        Set<String> out = new TreeSet<>();
        properties(mappings.get("properties"), "", out);
        return out;
    }

    private static void properties(JsonNode props, String path, Set<String> out) {
        Iterator<Map.Entry<String, JsonNode>> it = props.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> f = it.next();
            String p = path.isEmpty() ? f.getKey() : path + "." + f.getKey();
            if (f.getValue().has("properties")) {
                properties(f.getValue().get("properties"), p, out);
            } else {
                out.add(p);
            }
        }
    }
}
