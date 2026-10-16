package com.kaizen.stream.simulation.order;

import com.kaizen.stream.config.RawSettings;
import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.simulation.order.lookup.MasterData;
import com.kaizen.stream.simulation.order.transform.ConfigDemandModel;
import com.kaizen.stream.simulation.order.transform.OrderGenerator;
import com.kaizen.stream.utils.time.IctTime;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Master data shaped like the v1 generator (50 shops, products, vouchers, gifts) and the default profile.
 * Master data giống bộ sinh v1 (50 shop, sản phẩm, voucher, quà tặng) và hồ sơ mặc định.
 */
public final class OrderTestData {

    public static final long YEAR_START = IctTime.startOfDay(LocalDate.of(2026, 1, 1));
    static final long YEAR_END = IctTime.startOfDay(LocalDate.of(2026, 12, 31));
    static final String[] UNITS = {"Hộp", "Chai", "Vỉ", "Viên"};
    static final int SERIES_PER_VOUCHER = 2_000;

    private OrderTestData() {
    }

    public static StreamSettings.Order profile() {
        return profile(Map.of());
    }

    public static StreamSettings.Order profile(Map<String, String> overrides) {
        Map<String, String> values = new HashMap<>(overrides);
        return new StreamSettings(new RawSettings(values)).order();
    }

    public static OrderGenerator generator() {
        StreamSettings.Order p = profile();
        return new OrderGenerator(p, new ConfigDemandModel(p));
    }

    public static MasterData master() {
        return master(Map.of());
    }

    /**
     * Master data where PG already shows the given codes as used, closed at the mapped order time.
     * Master data trong đó PG đã thấy các mã cho trước là đã dùng, đóng tại thời điểm đơn tương ứng.
     */
    public static MasterData master(Map<String, Long> closedAt) {
        List<String> shops = new ArrayList<>();
        Map<String, List<String>> employees = new HashMap<>();
        for (int s = 1; s <= 50; s++) {
            String code = String.format("SHOP%04d", s);
            shops.add(code);
            employees.put(code, List.of(String.format("EMP%04d", 2 * s - 1), String.format("EMP%04d", 2 * s)));
        }
        List<MasterData.Product> products = new ArrayList<>();
        for (int i = 1; i <= 40; i++) {
            long stock = 5_000L + 3_700L * i;
            products.add(new MasterData.Product(String.format("PROD%04d", i), UNITS[i % UNITS.length],
                    stock * 6 / 5, stock));
        }
        // Gift-only products have no retail price / Hàng chỉ để tặng không có giá bán
        for (int i = 41; i <= 44; i++) {
            products.add(new MasterData.Product(String.format("PROD%04d", i), "Hộp", 0, 3_000L));
        }
        List<String> customers = new ArrayList<>();
        for (int c = 1; c <= 1_000; c++) {
            customers.add(String.format("CUST%06d", c));
        }
        List<MasterData.Voucher> vouchers = List.of(
                voucher("VCH0001", 20_000, 100_000, closedAt),
                voucher("VCH0002", 50_000, 200_000, closedAt),
                voucher("VCH0003", 100_000, 500_000, closedAt));
        List<MasterData.GiftOffer> gifts = List.of(
                new MasterData.GiftOffer("PROMO0011", "PROD0041", YEAR_START, YEAR_END),
                new MasterData.GiftOffer("PROMO0012", "PROD0042", YEAR_START, YEAR_END));
        return MasterData.of(shops, employees, products, customers, vouchers, gifts);
    }

    private static MasterData.Voucher voucher(String code, long amount, long minOrderValue, Map<String, Long> closedAt) {
        List<MasterData.Series> series = new ArrayList<>();
        for (int n = 1; n <= SERIES_PER_VOUCHER; n++) {
            String s = String.format("SR_%s_%04d", code, n);
            series.add(new MasterData.Series(s, YEAR_START, Long.MAX_VALUE, closedAt.getOrDefault(s, Long.MAX_VALUE)));
        }
        return new MasterData.Voucher(code, amount, minOrderValue, YEAR_START, YEAR_END, series);
    }
}
