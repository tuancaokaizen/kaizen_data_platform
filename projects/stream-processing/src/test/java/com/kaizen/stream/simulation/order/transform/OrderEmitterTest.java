package com.kaizen.stream.simulation.order.transform;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.simulation.order.OrderTestData;
import com.kaizen.stream.simulation.order.lookup.MasterData;
import com.kaizen.stream.simulation.order.transform.state.OrderCursor;
import com.kaizen.stream.utils.time.IctTime;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderEmitterTest {

    private static final long HOUR = DemandModel.HOUR_MS;
    private static final LocalDate MONDAY = LocalDate.of(2026, 10, 12);
    private static final long DAY_START = IctTime.startOfDay(MONDAY);

    private final MasterData master = OrderTestData.master();
    private final OrderEmitter emitter = new OrderEmitter(OrderTestData.generator(), 6 * HOUR, "flink.simulation-order");

    @Test
    void beforeOpeningHoursOnlyTierAShopsTrickle() {
        List<Envelope<OrderSnapshot>> night = run(emitter.start(null, DAY_START), DAY_START + 7 * HOUR - 1);

        // 0–7h holds 2.7 / 108 of the day, ~165 orders / 0–7h chiếm 2,7 / 108 của ngày, ~165 đơn
        assertTrue(night.size() > 110 && night.size() < 220, "night orders " + night.size());
        for (Envelope<OrderSnapshot> e : night) {
            assertTrue(isTierA(e.getData().getOrder().getShopCode()), e.getData().getOrder().getId());
        }
    }

    @Test
    void aDayOfOrdersHasTheExpectedVolumeAndConsecutiveShopCounters() {
        List<Envelope<OrderSnapshot>> out = run(emitter.start(null, DAY_START), DAY_START + 24 * HOUR);

        assertEquals(7_000 * 0.93, out.size(), 7_000 * 0.93 * 0.04);
        Set<String> ids = new HashSet<>();
        Map<String, Integer> lastPerShop = new HashMap<>();
        long previous = 0;
        for (Envelope<OrderSnapshot> e : out) {
            String id = e.getData().getOrder().getId();
            assertTrue(ids.add(id), "unique id " + id);
            assertEquals("ORDER_PLACED", e.getMeta().getEventType());
            assertEquals("flink.simulation-order", e.getMeta().getSource());
            assertTrue(e.getMeta().getEventTime() > previous, "event times increase");
            previous = e.getMeta().getEventTime();
            int hour = Instant.ofEpochMilli(previous).atZone(IctTime.ZONE).getHour();
            // SORD-yyyyMMdd-SHOPxxxx-nnnnn counts 1, 2, 3… per shop / Đếm 1, 2, 3… theo shop
            String shop = e.getData().getOrder().getShopCode();
            assertTrue(!ConfigDemandModel.isNight(hour) || isTierA(shop), id);
            int n = Integer.parseInt(id.substring(id.lastIndexOf('-') + 1));
            assertEquals(lastPerShop.getOrDefault(shop, 0) + 1, n, id);
            lastPerShop.put(shop, n);
        }
    }

    @Test
    void restoringTheCursorReplaysExactlyTheSameEvents() {
        OrderCursor c = emitter.start(null, DAY_START);
        run(c, DAY_START + 12 * HOUR);
        OrderCursor checkpoint = c.copy();
        List<String> first = eventIds(run(c, DAY_START + 18 * HOUR));

        OrderCursor restored = emitter.start(checkpoint, DAY_START + 12 * HOUR + 30_000);
        List<String> replay = eventIds(run(restored, DAY_START + 18 * HOUR));

        assertTrue(first.size() > 100);
        assertEquals(first, replay);
    }

    @Test
    void everyVoucherCodeIsUsedAtMostOnce() {
        List<Envelope<OrderSnapshot>> out = run(emitter.start(null, DAY_START), DAY_START + 2 * 24 * HOUR);

        List<String> codes = voucherCodes(out);
        assertTrue(codes.size() > 500, "vouchers " + codes.size());
        assertEquals(codes.size(), new HashSet<>(codes).size());
    }

    @Test
    void replayAfterTheSinkMarkedCodesUsedPicksTheSameCodes() {
        OrderCursor c = emitter.start(null, DAY_START);
        List<Envelope<OrderSnapshot>> before = run(c, DAY_START + 12 * HOUR);
        OrderCursor checkpoint = c.copy();
        List<Envelope<OrderSnapshot>> first = run(c, DAY_START + 18 * HOUR);

        // PG now shows every code of both runs closed at its order time / PG giờ thấy mọi mã đã đóng tại giờ đơn
        Map<String, Long> closedAt = new HashMap<>();
        for (List<Envelope<OrderSnapshot>> events : List.of(before, first)) {
            for (Envelope<OrderSnapshot> e : events) {
                if (e.getData().getOrder().getVoucherSeriesCode() != null) {
                    closedAt.put(e.getData().getOrder().getVoucherSeriesCode(), e.getMeta().getEventTime());
                }
            }
        }
        MasterData written = OrderTestData.master(closedAt);
        OrderCursor restored = emitter.start(checkpoint, DAY_START + 12 * HOUR + 30_000);
        emitter.forgetSettledSeries(restored, written);
        List<Envelope<OrderSnapshot>> replay = run(restored, written, DAY_START + 18 * HOUR);

        assertTrue(restored.getUsedSeries().containsAll(voucherCodes(first)));
        assertEquals(eventIds(first), eventIds(replay));
        assertEquals(voucherCodes(first), voucherCodes(replay));
        assertTrue(voucherCodes(first).size() > 50);
    }

    @Test
    void codesThatMasterDataShowsClosedAreForgotten() {
        OrderCursor c = emitter.start(null, DAY_START);
        List<Envelope<OrderSnapshot>> out = run(c, DAY_START + 12 * HOUR);
        Map<String, Long> closedAt = new HashMap<>();
        out.forEach(e -> {
            if (e.getData().getOrder().getVoucherSeriesCode() != null) {
                closedAt.put(e.getData().getOrder().getVoucherSeriesCode(), e.getMeta().getEventTime());
            }
        });
        assertEquals(closedAt.keySet(), c.getUsedSeries());

        // Not written yet → kept; written → forgotten / Chưa ghi → giữ; đã ghi → bỏ
        emitter.forgetSettledSeries(c, master);
        assertEquals(closedAt.keySet(), c.getUsedSeries());
        emitter.forgetSettledSeries(c, OrderTestData.master(closedAt));
        assertTrue(c.getUsedSeries().isEmpty());
    }

    @Test
    void cursorIsAFlinkPojoSoAddedFieldsRestoreFromOlderState() {
        assertTrue(TypeInformation.of(OrderCursor.class) instanceof PojoTypeInfo);
    }

    @Test
    void stateWithoutUsedSeriesIsRestored() {
        OrderCursor old = new OrderCursor(5, DAY_START + 9 * HOUR, "2026-10-12", new HashMap<>(), null);

        OrderCursor c = emitter.start(old, DAY_START + 9 * HOUR);

        assertTrue(c.getUsedSeries().isEmpty());
        run(c, DAY_START + 12 * HOUR);
    }

    @Test
    void longOutagesAreOnlyCaughtUpForTheLastSixHours() {
        OrderCursor c = emitter.start(null, DAY_START);
        run(c, DAY_START + 9 * HOUR);
        long now = DAY_START + 2 * 24 * HOUR + 20 * HOUR;

        OrderCursor resumed = emitter.start(c.copy(), now);

        assertTrue(resumed.getNextAt() >= now - 6 * HOUR);
        assertEquals(c.getSeq(), resumed.getSeq(), "sequence numbers continue");
        List<Envelope<OrderSnapshot>> caughtUp = run(resumed, now);
        assertTrue(caughtUp.stream().allMatch(e -> e.getMeta().getEventTime() >= now - 6 * HOUR));
    }

    @Test
    void shortOutagesAreCaughtUpCompletely() {
        OrderCursor c = emitter.start(null, DAY_START);
        run(c, DAY_START + 9 * HOUR);
        long before = c.getNextAt();

        OrderCursor resumed = emitter.start(c.copy(), DAY_START + 12 * HOUR);

        assertEquals(before, resumed.getNextAt());
    }

    @Test
    void shopCountersRestartEveryBusinessDay() {
        OrderCursor c = emitter.start(null, DAY_START);
        List<Envelope<OrderSnapshot>> out = run(c, DAY_START + 2 * 24 * HOUR);

        List<String> tuesday = out.stream().map(e -> e.getData().getOrder().getId())
                .filter(id -> id.startsWith("SORD-20261013-")).collect(Collectors.toList());
        assertTrue(tuesday.stream().anyMatch(id -> id.endsWith("-00001")));
        assertEquals("2026-10-13", c.getDay());
    }

    @Test
    void aFreshStartContinuesTodaysNumbersInsteadOfReusingIds() {
        Map<String, Integer> usedToday = new HashMap<>();
        for (String shop : master.getShopCodes()) {
            usedToday.put(shop, 40);
        }
        OrderCursor c = emitter.start(null, DAY_START + 12 * HOUR, usedToday);
        List<Envelope<OrderSnapshot>> out = run(c, DAY_START + 14 * HOUR);

        assertTrue(!out.isEmpty());
        for (Envelope<OrderSnapshot> e : out) {
            String id = e.getData().getOrder().getId();
            assertTrue(Integer.parseInt(id.substring(id.lastIndexOf('-') + 1)) > 40, id);
        }
    }

    @Test
    void numbersSeededForTodayRestartOnTheNextBusinessDay() {
        OrderCursor c = emitter.start(null, DAY_START + 23 * HOUR, Map.of("SHOP0001", 500));
        List<Envelope<OrderSnapshot>> out = run(c, DAY_START + 36 * HOUR);

        List<String> tuesday = out.stream().map(e -> e.getData().getOrder().getId())
                .filter(id -> id.startsWith("SORD-20261013-SHOP0001-")).collect(Collectors.toList());
        assertEquals("SORD-20261013-SHOP0001-00001", tuesday.get(0));
    }

    @Test
    void oneCallEmitsAtMostTheBatchSize() {
        OrderCursor c = emitter.start(null, DAY_START);

        assertEquals(10, emitter.emitDue(c, master, DAY_START + 12 * HOUR, 10, e -> { }));
        assertEquals(10, c.getSeq());
    }

    /** The test master's first 10 codes are tier A. / 10 mã đầu của master test là hạng A. */
    private static boolean isTierA(String shop) {
        return shop.compareTo("SHOP0010") <= 0;
    }

    private List<Envelope<OrderSnapshot>> run(OrderCursor c, long until) {
        return run(c, master, until);
    }

    private List<Envelope<OrderSnapshot>> run(OrderCursor c, MasterData m, long until) {
        List<Envelope<OrderSnapshot>> out = new ArrayList<>();
        emitter.emitDue(c, m, until, Integer.MAX_VALUE, out::add);
        return out;
    }

    private static List<String> voucherCodes(List<Envelope<OrderSnapshot>> events) {
        return events.stream().map(e -> e.getData().getOrder().getVoucherSeriesCode())
                .filter(java.util.Objects::nonNull).collect(Collectors.toList());
    }

    private static List<String> eventIds(List<Envelope<OrderSnapshot>> events) {
        return events.stream().map(e -> e.getMeta().getEventId()).collect(Collectors.toList());
    }
}
