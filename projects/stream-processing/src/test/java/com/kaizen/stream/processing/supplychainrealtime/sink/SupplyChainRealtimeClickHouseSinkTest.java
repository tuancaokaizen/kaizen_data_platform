package com.kaizen.stream.processing.supplychainrealtime.sink;

import com.kaizen.stream.model.realtime.RealtimeInventoryPositionRow;
import com.kaizen.stream.model.realtime.RealtimeShipmentStatusRow;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Writers and tables must agree on columns, or inserts break at runtime.
 * Writer và bảng phải cùng danh sách cột, nếu không insert sẽ lỗi lúc chạy.
 */
class SupplyChainRealtimeClickHouseSinkTest {

    private static final String DDL = "../../dags/schema_manager/clickhouse/schema_clickhouse.sql";

    @Test
    void columnsMatchTheDdl() throws Exception {
        assertEquals(RealtimeInventoryPositionRow.COLUMNS, columns(RealtimeInventoryPositionRow.TABLE));
        assertEquals(RealtimeShipmentStatusRow.COLUMNS, columns(RealtimeShipmentStatusRow.TABLE));
    }

    @Test
    void bindsOneParameterPerColumn() throws Exception {
        TreeSet<Integer> inventory = new TreeSet<>();
        RealtimeInventoryPositionClickHouseSink.bind(recorder(inventory), RealtimeInventoryPositionRow.builder().locationType("SHOP")
                .locationCode("S1").productId("P1").businessDate(LocalDate.of(2026, 10, 12)).lastMovementType("SALE")
                .build());
        assertEquals(RealtimeInventoryPositionRow.COLUMNS.size(), inventory.size());
        assertEquals(RealtimeInventoryPositionRow.COLUMNS.size(), inventory.last());

        TreeSet<Integer> shipment = new TreeSet<>();
        RealtimeShipmentStatusClickHouseSink.bind(recorder(shipment), RealtimeShipmentStatusRow.builder()
                .shipmentDate(LocalDate.of(2026, 10, 12)).shipmentId("SHP-1").originCode("DC01").status("PLANNED")
                .lastEventType("SHIPMENT_PLANNED").build());
        assertEquals(RealtimeShipmentStatusRow.COLUMNS.size(), shipment.size());
        assertEquals(RealtimeShipmentStatusRow.COLUMNS.size(), shipment.last());
    }

    private PreparedStatement recorder(TreeSet<Integer> indexes) {
        return (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{PreparedStatement.class}, (proxy, method, args) -> {
                    if (method.getName().startsWith("set")) {
                        indexes.add((Integer) args[0]);
                    }
                    return null;
                });
    }

    private static List<String> columns(String table) throws Exception {
        String sql = new String(Files.readAllBytes(Paths.get(DDL)), StandardCharsets.UTF_8);
        Matcher m = Pattern.compile("CREATE TABLE IF NOT EXISTS " + Pattern.quote(table) + " \\((.*?)\\n\\) ENGINE",
                Pattern.DOTALL).matcher(sql);
        assertTrue(m.find(), table + " not found");
        List<String> out = new ArrayList<>();
        for (String line : m.group(1).split("\n")) {
            String t = line.trim();
            if (!t.isEmpty() && !t.startsWith("--")) {
                out.add(t.split("\\s+")[0]);
            }
        }
        return out;
    }
}
