package com.kaizen.stream.simulation.tool;

import com.kaizen.stream.config.RawSettings;
import com.kaizen.stream.config.SettingsLoader;
import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.model.order.OrderLine;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.simulation.order.lookup.MasterData;
import com.kaizen.stream.simulation.order.lookup.PostgresMasterDataSource;
import com.kaizen.stream.simulation.order.transform.ConfigDemandModel;
import com.kaizen.stream.simulation.order.transform.DemandModel;
import com.kaizen.stream.simulation.order.transform.OrderGenerator;
import com.kaizen.stream.utils.time.IctTime;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Opening stock per shop × product = expected demand of the next N days under the live order profile,
 * measured by running the real OrderGenerator (gifts included). Prints CSV shop,product,qty,unit_cost.
 * Usage: KAIZEN_POSTGRES_PASSWORD=… java -cp … OpeningStockPlanner --postgres.host … [--opening.days 60]
 * Tồn đầu kỳ mỗi shop × sản phẩm = nhu cầu kỳ vọng của N ngày tới theo hồ sơ đơn live, đo bằng cách chạy
 * chính OrderGenerator (gồm cả quà). In CSV shop,product,qty,unit_cost.
 */
public final class OpeningStockPlanner {

    /** Far from live sequence numbers, only the statistics matter. / Xa dãy số live, chỉ cần thống kê. */
    static final long SEQ_BASE = 9_000_000_000L;

    private OpeningStockPlanner() {
    }

    public static void main(String[] args) throws Exception {
        StreamSettings settings = SettingsLoader.load(args);
        RawSettings raw = settings.raw();
        int days = raw.getInt("opening", "days", 60);
        int sampleDays = raw.getInt("opening", "sample_days", 120);
        StreamSettings.Order profile = settings.order();

        MasterData master = new PostgresMasterDataSource(settings.postgres()).load();
        Map<String, long[]> plan = plan(master, profile, days, sampleDays, System.currentTimeMillis());

        PrintStream out = new PrintStream(System.out, false, StandardCharsets.UTF_8.name());
        for (Map.Entry<String, long[]> e : plan.entrySet()) {
            String[] key = e.getKey().split("\\|", 2);
            out.println(key[0] + "," + key[1] + "," + e.getValue()[0] + "," + e.getValue()[1]);
        }
        out.flush();
        System.err.println("Planned " + plan.size() + " positions for " + days + " days from "
                + sampleDays + " sampled days");
    }

    /**
     * shop|product → {qty, unitCost}; qty rounded up so every product seen in the sample gets stock.
     * shop|product → {qty, giá vốn}; qty làm tròn lên để mọi sản phẩm có trong mẫu đều có tồn.
     */
    static Map<String, long[]> plan(MasterData master, StreamSettings.Order profile, int days, int sampleDays,
                                    long at) {
        ConfigDemandModel demand = new ConfigDemandModel(profile);
        OrderGenerator generator = new OrderGenerator(profile, demand);
        long orders = Math.round(profile.getOrdersPerDay() * sampleDays);
        long dayStart = IctTime.startOfDay(IctTime.businessDate(at));
        long[] hourEnds = hourQuotas(demand, dayStart, orders);
        Map<String, Integer> shopSeq = new HashMap<>();
        Map<String, long[]> sold = new TreeMap<>();
        int hour = 0;
        for (long i = 0; i < orders; i++) {
            // Spread over today's hours by rate: which shops are open depends on the hour, gifts on the day
            // Rải theo giờ trong ngày theo tốc độ: shop mở cửa tùy giờ, quà tùy ngày
            while (i >= hourEnds[hour]) {
                hour++;
            }
            long when = dayStart + hour * DemandModel.HOUR_MS + DemandModel.HOUR_MS / 2;
            OrderSnapshot s = generator.place(master, SEQ_BASE + i, when, shop -> shopSeq.merge(shop, 1, Integer::sum));
            String shop = s.getOrder().getShopCode();
            for (OrderLine l : s.getItems()) {
                long[] v = sold.computeIfAbsent(shop + "|" + l.getProductId(), k -> new long[2]);
                v[0] += l.getQuantity();
                v[1] = l.getUnitCost();
            }
        }
        // Weekday factors average 1, so sampled days scale linearly / Hệ số thứ trung bình 1 nên nhân tuyến tính
        for (long[] v : sold.values()) {
            v[0] = (long) Math.ceil(v[0] * (double) days / sampleDays);
        }
        return sold;
    }

    /**
     * Cumulative order count at the end of each hour of the day, proportional to the demand rate of that hour.
     * Số đơn cộng dồn tới cuối mỗi giờ trong ngày, tỷ lệ với tốc độ nhu cầu của giờ đó.
     */
    static long[] hourQuotas(DemandModel demand, long dayStart, long orders) {
        double[] rate = new double[24];
        double total = 0;
        for (int h = 0; h < 24; h++) {
            rate[h] = demand.rate(dayStart + h * DemandModel.HOUR_MS);
            total += rate[h];
        }
        long[] ends = new long[24];
        double acc = 0;
        for (int h = 0; h < 24; h++) {
            acc += rate[h];
            ends[h] = Math.round(orders * acc / total);
        }
        // Last hour closes the day exactly / Giờ cuối khép đúng tổng số đơn
        ends[23] = orders;
        return ends;
    }
}
