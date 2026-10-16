package com.kaizen.stream.simulation.tool;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.simulation.order.OrderTestData;
import com.kaizen.stream.simulation.order.lookup.MasterData;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpeningStockPlannerTest {

    private static final long AT = OrderTestData.YEAR_START + 40L * 86_400_000L;

    @Test
    void stockScalesWithTheCoveredDays() {
        MasterData master = OrderTestData.master();
        StreamSettings.Order profile = OrderTestData.profile(Map.of("order.orders_per_day", "300"));

        Map<String, long[]> sample = OpeningStockPlanner.plan(master, profile, 5, 5, AT);
        Map<String, long[]> doubled = OpeningStockPlanner.plan(master, profile, 10, 5, AT);

        assertEquals(sample.keySet(), doubled.keySet());
        for (Map.Entry<String, long[]> e : sample.entrySet()) {
            assertTrue(e.getValue()[0] > 0, e.getKey());
            assertEquals(2 * e.getValue()[0], doubled.get(e.getKey())[0], e.getKey());
            assertEquals(e.getValue()[1], doubled.get(e.getKey())[1]);
        }
        // Positions cover several shops and products / Vị trí phủ nhiều shop và sản phẩm
        assertTrue(sample.keySet().stream().map(k -> k.split("\\|")[0]).distinct().count() > 10);
    }

    @Test
    void roundsUpSoRareProductsStillGetStock() {
        Map<String, long[]> plan = OpeningStockPlanner.plan(OrderTestData.master(),
                OrderTestData.profile(Map.of("order.orders_per_day", "50")), 1, 100, AT);
        assertTrue(plan.values().stream().allMatch(v -> v[0] >= 1));
    }
}
