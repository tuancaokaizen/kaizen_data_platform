package com.kaizen.stream.simulation.order.transform;

import com.kaizen.stream.model.order.OrderEventType;
import com.kaizen.stream.model.order.OrderHeader;
import com.kaizen.stream.model.order.OrderLine;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.order.OrderStatus;
import com.kaizen.stream.model.order.ReturnDocument;
import com.kaizen.stream.model.order.ReturnLine;
import com.kaizen.stream.processing.rule.OrderRules;
import com.kaizen.stream.simulation.order.OrderTestData;
import com.kaizen.stream.simulation.order.lookup.MasterData;
import com.kaizen.stream.testing.Fixtures;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderGeneratorTest {

    private static final int ORDERS = 20_000;
    private static final long DAY = 86_400_000L;
    /** Mid-morning ICT on Fixtures' day. / Giữa buổi sáng theo giờ VN. */
    private static final long AT = Fixtures.T0 + 3 * 3_600_000L;

    private final MasterData master = OrderTestData.master();
    private final OrderGenerator generator = OrderTestData.generator();

    @Test
    void sameSeedAndSequenceGiveTheSameOrder() {
        OrderSnapshot a = generator.place(master, 42, AT, shop -> 7);
        OrderSnapshot b = OrderTestData.generator().place(master, 42, AT, shop -> 7);

        assertEquals(a, b);
        assertEquals(generator.planReturn(a), generator.planReturn(b));
        assertEquals(generator.nextArrival(AT, 42), OrderTestData.generator().nextArrival(AT, 42));
    }

    @Test
    void anotherSeedGivesOtherOrders() {
        OrderGenerator other = new OrderGenerator(OrderTestData.profile(Map.of("order.seed", "7")),
                new ConfigDemandModel(OrderTestData.profile()));

        assertFalse(generator.place(master, 42, AT, shop -> 1).equals(other.place(master, 42, AT, shop -> 1)));
    }

    @Test
    void everyOrderSatisfiesTheRulesAndLooksLikeAPharmacyBasket() {
        int walkIns = 0;
        int vouchers = 0;
        int gifts = 0;
        long saleLines = 0;
        for (OrderSnapshot s : orders()) {
            OrderHeader o = s.getOrder();
            assertEquals(List.of(), OrderRules.violations(OrderEventType.ORDER_PLACED.name(), AT, s), o.getId());
            assertEquals(OrderStatus.COMPLETED, o.getStatus());
            Set<String> products = new HashSet<>();
            int sale = 0;
            for (OrderLine l : s.getItems()) {
                assertTrue(products.add(l.getProductId()), "one line per product");
                assertTrue(l.getUnitCost() > 0);
                if (l.getGift()) {
                    gifts++;
                    assertEquals(0L, l.getDiscountAmount());
                } else {
                    sale++;
                }
            }
            assertTrue(sale >= 1 && sale <= 6);
            saleLines += sale;
            walkIns += o.getCustomerId() == null ? 1 : 0;
            if (o.getTotalDiscount() > 0) {
                vouchers++;
                assertNotNull(o.getVoucherSeriesCode());
            } else {
                assertEquals(null, o.getVoucherSeriesCode());
            }
            assertTrue(master.employeesOf(o.getShopCode()).contains(o.getEmployeeSaleId()));
        }
        assertEquals(0.20, walkIns / (double) ORDERS, 0.015, "walk-in share");
        // Vouchers need min order value, so slightly under voucher_rate / Voucher cần giá trị tối thiểu nên thấp hơn chút
        assertTrue(vouchers / (double) ORDERS > 0.06 && vouchers / (double) ORDERS <= 0.105, "voucher share " + vouchers);
        assertEquals(0.30, gifts / (double) ORDERS, 0.015, "gift share");
        assertEquals(2.4, saleLines / (double) ORDERS, 0.08, "mean sale lines");
    }

    @Test
    void shopsFollowTheirTierWeights() {
        Map<String, Integer> perShop = new HashMap<>();
        for (OrderSnapshot s : orders()) {
            perShop.merge(s.getOrder().getShopCode(), 1, Integer::sum);
        }
        int tierA = 0;
        for (int s = 1; s <= 10; s++) {
            tierA += perShop.getOrDefault(String.format("SHOP%04d", s), 0);
        }
        assertEquals(50, perShop.size());
        assertEquals(2_300.0 / 7_100, tierA / (double) ORDERS, 0.015, "tier A share");
    }

    @Test
    void popularProductsSellMoreThanTheTail() {
        Map<String, Integer> sold = new HashMap<>();
        for (OrderSnapshot s : orders()) {
            s.getItems().stream().filter(l -> !l.getGift()).forEach(l -> sold.merge(l.getProductId(), 1, Integer::sum));
        }
        List<MasterData.Product> ranked = master.getProducts();
        int head = sold.getOrDefault(ranked.get(0).getId(), 0);
        int tail = sold.getOrDefault(ranked.get(ranked.size() - 1).getId(), 0);
        assertTrue(head > 3 * tail, head + " vs " + tail);
        assertFalse(sold.containsKey("PROD0041"), "gift-only products are never sold");
    }

    @Test
    void orderIdsCarryDayShopAndShopCounter() {
        OrderSnapshot s = generator.place(master, 1, AT, shop -> 42);

        assertTrue(s.getOrder().getId().matches("SORD-20261011-SHOP\\d{4}-00042"), s.getOrder().getId());
        assertEquals(s.getOrder().getId() + "-L1", s.getItems().get(0).getId());
    }

    @Test
    void returnsFollowTheProfileAndStayValid() {
        int returns = 0;
        int full = 0;
        long units = 0;
        long restocked = 0;
        for (OrderSnapshot placed : orders()) {
            Optional<OrderSnapshot> planned = generator.planReturn(placed);
            if (planned.isEmpty()) {
                continue;
            }
            returns++;
            OrderSnapshot s = planned.get();
            ReturnDocument r = s.getReturns().get(0);
            assertEquals(List.of(), OrderRules.violations(OrderEventType.ORDER_RETURNED.name(),
                    s.getOrder().getModifiedDate(), s), s.getOrder().getId());
            long delay = r.getCreatedDate() - placed.getOrder().getTransactionDate();
            assertTrue(delay >= DAY && delay <= 14 * DAY, "delay " + delay);
            assertEquals("RET-" + placed.getOrder().getId() + "-1", r.getId());
            assertTrue(List.of(OrderGenerator.RETURN_REASONS).contains(r.getReason()));
            assertEquals(placed.getItems(), s.getItems(), "sold lines are unchanged");
            if (s.getOrder().getStatus() == OrderStatus.RETURNED) {
                full++;
            }
            for (ReturnLine l : r.getItems()) {
                units += l.getQuantity();
                restocked += l.getRestockQty();
            }
        }
        assertEquals(0.04, returns / (double) ORDERS, 0.006, "return rate");
        // Partial returns of single-unit orders also end up Returned / Trả một phần đơn 1 đơn vị cũng thành Returned
        assertTrue(full / (double) returns > 0.45 && full / (double) returns < 0.75, "full share " + full);
        assertEquals(0.70, restocked / (double) units, 0.04, "restock share");
    }

    @Test
    void takenVoucherCodesAreNeverReused() {
        // Leave two codes of VCH0001 free / Chỉ chừa hai mã của VCH0001
        Set<String> taken = new HashSet<>();
        for (MasterData.Voucher v : master.getVouchers()) {
            v.getSeries().forEach(s -> taken.add(s.getCode()));
        }
        taken.removeAll(List.of("SR_VCH0001_0007", "SR_VCH0001_0420"));

        List<String> used = new ArrayList<>();
        for (int i = 0; i < 2_000; i++) {
            OrderSnapshot s = generator.place(master, i, AT, shop -> 1, taken);
            String series = s.getOrder().getVoucherSeriesCode();
            if (series != null) {
                assertTrue(taken.add(series), "reused " + series);
                used.add(series);
                assertEquals(Math.min(20_000L, s.getOrder().getTotalBill()), s.getOrder().getTotalDiscount());
            } else {
                assertEquals(0L, s.getOrder().getTotalDiscount());
            }
        }
        assertEquals(Set.of("SR_VCH0001_0007", "SR_VCH0001_0420"), new HashSet<>(used));
    }

    @Test
    void aCodeIsUsableFromCreationUntilItExpiresOrCloses() {
        MasterData.Series used = new MasterData.Series("S1", 100, 1_000, 500);
        assertFalse(used.availableAt(99));
        assertTrue(used.availableAt(100));
        assertTrue(used.availableAt(500), "the order that used it can be replayed");
        assertFalse(used.availableAt(501));

        MasterData.Series open = new MasterData.Series("S2", 100, 1_000, Long.MAX_VALUE);
        assertTrue(open.availableAt(999));
        assertFalse(open.availableAt(1_000), "expired");
    }

    @Test
    void masterDataWithoutShopsOrProductsIsRejected() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> MasterData.of(List.of(), Map.of(), List.of(), List.of(), List.of(), List.of()));
        assertTrue(e.getMessage().contains("Load master data"));
    }

    private List<OrderSnapshot> orders() {
        // Unique ids like the real source: returns are seeded by order id / Id không trùng như source thật
        Map<String, Integer> counters = new HashMap<>();
        List<OrderSnapshot> out = new ArrayList<>(ORDERS);
        for (int i = 0; i < ORDERS; i++) {
            out.add(generator.place(master, i, AT, shop -> counters.merge(shop, 1, Integer::sum)));
        }
        return out;
    }
}
