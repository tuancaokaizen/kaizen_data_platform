package com.kaizen.stream.processing.businessrealtime.sink;

import com.kaizen.stream.model.realtime.RealtimeBusinessByOrderRow;
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
 * The writer, the hot table and the cold stub must agree on columns, or inserts / cold migration break at runtime.
 * Writer, bảng hot và stub cold phải cùng danh sách cột, nếu không insert / migrate cold sẽ lỗi lúc chạy.
 */
class RealtimeBusinessClickHouseSinkTest {

    private static final String DIR = "../../schema_manager/clickhouse/";

    @Test
    void columnsMatchTheHotTableDdl() throws Exception {
        assertEquals(RealtimeBusinessByOrderRow.COLUMNS, columns("schema_clickhouse.sql", "report.realtime_business_by_order"));
    }

    @Test
    void coldStubsHaveTheSameColumns() throws Exception {
        assertEquals(RealtimeBusinessByOrderRow.COLUMNS, columns("schema_clickhouse.sql", "serving.realtime_business_by_order"));
        assertEquals(RealtimeBusinessByOrderRow.COLUMNS, columns("schema_clickhouse_views.sql", "serving.realtime_business_by_order"));
    }

    @Test
    void bindsOneParameterPerColumn() throws Exception {
        TreeSet<Integer> indexes = new TreeSet<>();
        PreparedStatement ps = (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{PreparedStatement.class}, (proxy, method, args) -> {
                    if (method.getName().startsWith("set")) {
                        indexes.add((Integer) args[0]);
                    }
                    return null;
                });
        RealtimeBusinessByOrderRow row = RealtimeBusinessByOrderRow.builder().reportDate(LocalDate.of(2026, 10, 11)).orderId("O1").returnId("")
                .documentType(RealtimeBusinessByOrderRow.SALE).shopCode("S1").shopName("").orderStatus("Completed")
                .paymentMethodSummary("").build();

        RealtimeBusinessClickHouseSink.bind(ps, row);

        assertEquals(RealtimeBusinessByOrderRow.COLUMNS.size(), indexes.size());
        assertEquals(1, indexes.first());
        assertEquals(RealtimeBusinessByOrderRow.COLUMNS.size(), indexes.last());
    }

    private static List<String> columns(String file, String table) throws Exception {
        String sql = new String(Files.readAllBytes(Paths.get(DIR + file)), StandardCharsets.UTF_8);
        Matcher m = Pattern.compile("CREATE TABLE IF NOT EXISTS " + Pattern.quote(table) + " \\((.*?)\\n\\) ENGINE",
                Pattern.DOTALL).matcher(sql);
        assertTrue(m.find(), table + " not found in " + file);
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
