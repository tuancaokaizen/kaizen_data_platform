package com.kaizen.stream.simulation.order.transform;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.utils.time.IctTime;
import java.time.Instant;
import java.time.ZonedDateTime;

/**
 * Stand-in until DemandPlan exists: orders/day × weekday factor × hourly curve, shops weighted by tier
 * (supply_chain_forecast_optimization.md 3.1, section 8).
 * Mô hình tạm khi chưa có DemandPlan: đơn/ngày × hệ số thứ × đường cong giờ, shop có trọng số theo hạng.
 */
public final class ConfigDemandModel implements DemandModel {

    private static final long serialVersionUID = 1L;

    /** W(dow) Monday..Sunday, mean 1 (3.1). / Hệ số thứ T2..CN, trung bình 1. */
    static final double[] WEEKDAY = {0.93, 0.90, 0.92, 0.95, 1.03, 1.17, 1.10};

    /**
     * Relative orders per ICT hour: day 07–22 peaks 10–11h and 18–20h; a thin night trickle (~4.6% of the day).
     * Đơn theo giờ: ngày 07–22h, đỉnh 10–11h và 18–20h; ban đêm lác đác (~4,6% cả ngày).
     */
    static final double[] HOUR_WEIGHTS = {
            0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 1.5,
            3, 5, 7, 10, 10, 7, 5, 5, 5, 6, 7, 9, 10, 9, 5,
            1.5, 0.8};

    /** Night hours [22, 07): only tier-A (24/7) shops sell. / Giờ đêm [22, 07): chỉ shop hạng A (24/7) bán. */
    static final int NIGHT_FROM = 22;
    static final int NIGHT_TO = 7;

    private final StreamSettings.Order profile;
    private final double[] hourShare;

    public ConfigDemandModel(StreamSettings.Order profile) {
        this.profile = profile;
        double sum = 0;
        for (double w : HOUR_WEIGHTS) {
            sum += w;
        }
        hourShare = new double[24];
        for (int h = 0; h < 24; h++) {
            hourShare[h] = HOUR_WEIGHTS[h] / sum;
        }
    }

    @Override
    public double rate(long epochMs) {
        ZonedDateTime t = Instant.ofEpochMilli(epochMs).atZone(IctTime.ZONE);
        double perHour = profile.getOrdersPerDay() * WEEKDAY[t.getDayOfWeek().getValue() - 1] * hourShare[t.getHour()];
        return perHour / HOUR_MS;
    }

    @Override
    public double shopWeight(int rank, long epochMs) {
        if (rank < profile.getTierACount()) {
            return profile.getTierAWeight();
        }
        if (isNight(Instant.ofEpochMilli(epochMs).atZone(IctTime.ZONE).getHour())) {
            return 0;
        }
        return rank < profile.getTierACount() + profile.getTierBCount() ? profile.getTierBWeight() : profile.getTierCWeight();
    }

    static boolean isNight(int hour) {
        return hour >= NIGHT_FROM || hour < NIGHT_TO;
    }
}
