package com.kaizen.stream.simulation.order.transform;

import com.kaizen.stream.simulation.order.OrderTestData;
import com.kaizen.stream.utils.time.IctTime;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigDemandModelTest {

    private static final long HOUR = DemandModel.HOUR_MS;
    /** A Monday. / Một ngày thứ Hai. */
    private static final LocalDate MONDAY = LocalDate.of(2026, 10, 12);

    private final ConfigDemandModel model = new ConfigDemandModel(OrderTestData.profile());

    @Test
    void nightIsAThinTrickle() {
        long day = IctTime.startOfDay(MONDAY);
        double night = 0;
        for (int h : new int[]{0, 1, 2, 3, 4, 5, 6, 22, 23}) {
            double perHour = model.rate(day + h * HOUR) * HOUR;
            assertTrue(perHour > 0, "hour " + h);
            assertTrue(perHour < model.rate(day + 7 * HOUR) * HOUR, "hour " + h + " below the 07h opening");
            night += perHour;
        }
        assertEquals(0.046, night / dailyExpected(MONDAY), 0.002);
        assertTrue(model.rate(day + 10 * HOUR) > model.rate(day + 14 * HOUR), "morning peak");
        assertTrue(model.rate(day + 19 * HOUR) > model.rate(day + 15 * HOUR), "evening peak");
    }

    @Test
    void dailyTotalsFollowWeekdayFactorsAndAverageOrdersPerDay() {
        double week = 0;
        for (int d = 0; d < 7; d++) {
            double day = dailyExpected(MONDAY.plusDays(d));
            assertEquals(7_000 * ConfigDemandModel.WEEKDAY[d], day, 1e-6, MONDAY.plusDays(d).toString());
            week += day;
        }
        assertEquals(7 * 7_000, week, 1e-6);
    }

    @Test
    void poissonArrivalsMatchTheRateWithFewAtNight() {
        SplittableRandom rnd = new SplittableRandom(1);
        long start = IctTime.startOfDay(MONDAY);
        long end = IctTime.startOfDay(MONDAY.plusDays(7));
        int count = 0;
        int night = 0;
        long t = start;
        while (true) {
            t = DemandModel.nextArrival(model, t, -Math.log(1 - rnd.nextDouble()));
            if (t >= end) {
                break;
            }
            int hour = java.time.Instant.ofEpochMilli(t).atZone(IctTime.ZONE).getHour();
            if (ConfigDemandModel.isNight(hour)) {
                night++;
            }
            count++;
        }
        assertEquals(49_000, count, 49_000 * 0.02);
        assertEquals(49_000 * 0.046, night, 49_000 * 0.046 * 0.1);
    }

    @Test
    void tiersFollowShopRankByDay() {
        long noon = IctTime.startOfDay(MONDAY) + 12 * HOUR;
        assertEquals(230.0, model.shopWeight(0, noon));
        assertEquals(230.0, model.shopWeight(9, noon));
        assertEquals(150.0, model.shopWeight(10, noon));
        assertEquals(150.0, model.shopWeight(29, noon));
        assertEquals(90.0, model.shopWeight(30, noon));
    }

    @Test
    void onlyTierASellsAtNight() {
        long day = IctTime.startOfDay(MONDAY);
        for (int h : new int[]{0, 3, 6, 22, 23}) {
            assertEquals(230.0, model.shopWeight(9, day + h * HOUR), "hour " + h);
            assertEquals(0.0, model.shopWeight(10, day + h * HOUR), "hour " + h);
            assertEquals(0.0, model.shopWeight(30, day + h * HOUR), "hour " + h);
        }
        assertEquals(150.0, model.shopWeight(10, day + 7 * HOUR));
    }

    private double dailyExpected(LocalDate date) {
        long start = IctTime.startOfDay(date);
        double sum = 0;
        for (int h = 0; h < 24; h++) {
            sum += model.rate(start + h * HOUR) * HOUR;
        }
        return sum;
    }
}
