package com.kaizen.flink.simulate;

import com.kaizen.flink.model.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sinh OrderBundle với business rules:
 *
 * 1. THROUGHPUT: được gọi mỗi 100ms → 10 bundles/giây → 100 transactions/10s.
 *
 * 2. VOUCHER:
 *    - Mỗi khi khách hàng hoàn thành đủ 10 đơn hàng → tự động cấp 1 voucher hoạt động tại orderDate.
 *    - Voucher được áp dụng tự động vào lần mua tiếp theo của cùng khách hàng nếu đạt thời gian hiệu lực và MinOrderValue.
 *
 * 3. GIFT:
 *    - Nếu có chương trình khuyến mãi GIFT đang hoạt động tại orderDate, đơn hàng sẽ tự động nhận được sản phẩm tặng kèm (UnitPrice = 0).
 *
 * 4. RETURN (7-day window):
 *    - 25% xác suất có return sau khi đơn COMPLETED.
 *    - Return date phải trong vòng 7 ngày kể từ ngày mua (transactionDate).
 *    - Có thể trả FULL (tất cả items) hoặc PARTIAL (1..N items được chọn ngẫu nhiên).
 *    - Mỗi item có thể trả PARTIAL quantity (1..max qty).
 */
public final class OrderSimulator {

    private static final long SEVEN_DAYS_MS      = 7L * 24 * 3600 * 1000;
    private static final long THIRTY_DAYS_MS     = 30L * 24 * 3600 * 1000;
    private static final int  VOUCHER_THRESHOLD  = 10;      // đơn hàng cần để nhận voucher
    private static final int  RETURN_PROBABILITY = 4;       // 1/4 = 25%

    /** Đếm số lần mua của từng customer. */
    private static final ConcurrentHashMap<String, AtomicInteger> ORDER_COUNT =
            new ConcurrentHashMap<>();

    /** Voucher đang chờ áp dụng cho customer (chỉ giữ 1 voucher/customer). */
    private static final ConcurrentHashMap<String, String> PENDING_VOUCHER =
            new ConcurrentHashMap<>();

    private OrderSimulator() {}

    public static OrderBundle generate() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long now = System.currentTimeMillis();

        // Đơn hàng xảy ra trong quá khứ (1 phút → 30 ngày trước)
        // để có window cho return (return phải ≤ now)
        long orderDate = now - rnd.nextLong(60_000L, THIRTY_DAYS_MS);

        // ── Customer + Voucher logic ──────────────────────────────────────────
        String customerId  = IdPool.pickPareto(IdPool.CUSTOMER_IDS);
        int orderCount     = ORDER_COUNT.computeIfAbsent(customerId, k -> new AtomicInteger(0))
                                        .incrementAndGet();

        // Lấy voucher đang chờ của customer này
        String pendingVoucherCode = PENDING_VOUCHER.get(customerId);
        IdPool.VoucherRef appliedVoucher = null;
        if (pendingVoucherCode != null) {
            for (IdPool.VoucherRef v : IdPool.VOUCHERS) {
                if (v.code.equals(pendingVoucherCode)) {
                    appliedVoucher = v;
                    break;
                }
            }
        }

        // Sau mỗi 10 đơn → cấp voucher mới thích hợp (phải hoạt động tại orderDate) cho lần sau
        if (orderCount % VOUCHER_THRESHOLD == 0) {
            List<IdPool.VoucherRef> activeVouchers = new ArrayList<>();
            for (IdPool.VoucherRef v : IdPool.VOUCHERS) {
                if (v.startDate <= orderDate && orderDate <= v.endDate) {
                    activeVouchers.add(v);
                }
            }
            if (!activeVouchers.isEmpty()) {
                IdPool.VoucherRef chosen = activeVouchers.get(rnd.nextInt(activeVouchers.size()));
                PENDING_VOUCHER.put(customerId, chosen.code);
            }
        }

        // ── OrderItems (1-5 sản phẩm) ────────────────────────────────────────
        String orderId = "ORD-" + uuid8();
        int itemCount  = rnd.nextInt(1, 6);
        List<OrderItem> items = new ArrayList<>(itemCount);
        int totalBill = 0;

        for (int i = 0; i < itemCount; i++) {
            String itemId = "OI-" + uuid8();
            String prodId = IdPool.pick(IdPool.PRODUCT_IDS);
            int qty       = rnd.nextInt(1, 10);
            int unitPrice = rnd.nextInt(5_000, 500_000);
            totalBill    += qty * unitPrice;
            items.add(new OrderItem(itemId, orderId, prodId, qty, unitPrice, orderDate, orderDate));
        }

        // ── Gift Promotion Logic (Xoay tua quà tặng trong thời gian hiệu lực) ──
        List<IdPool.GiftRef> applicableGifts = new ArrayList<>();
        for (IdPool.PromotionRef p : IdPool.PROMOTIONS) {
            if ("GIFT".equals(p.type) && p.startDate <= orderDate && orderDate <= p.endDate) {
                for (IdPool.GiftRef g : IdPool.GIFTS) {
                    if (g.promotionCode.equals(p.code)) {
                        applicableGifts.add(g);
                    }
                }
            }
        }
        if (!applicableGifts.isEmpty()) {
            // Chọn ngẫu nhiên quà tặng từ các chương trình quà tặng đang hiệu lực
            IdPool.GiftRef chosenGift = applicableGifts.get(rnd.nextInt(applicableGifts.size()));
            String giftItemId = "OI-" + uuid8();
            // Thêm sản phẩm tặng với UnitPrice = 0
            items.add(new OrderItem(giftItemId, orderId, chosenGift.productId, 1, 0, orderDate, orderDate));
        }

        // ── Discount: voucher (cần ràng buộc hạn dùng + min order value) ─────
        int voucherDiscount = 0;
        String appliedVoucherSeriesCode = null;
        if (appliedVoucher != null) {
            if (orderDate > appliedVoucher.endDate) {
                // Voucher đã hết hạn, xóa khỏi danh sách chờ
                PENDING_VOUCHER.remove(customerId);
            } else if (orderDate >= appliedVoucher.startDate && totalBill >= appliedVoucher.minOrderValue) {
                // Voucher hợp lệ và đạt giá trị đơn hàng tối thiểu
                voucherDiscount = appliedVoucher.amount;
                // Phát sinh VoucherSeriesCode (từ 1 đến 50) dựa trên Voucher Code của promotion đang áp dụng
                appliedVoucherSeriesCode = "SR_" + appliedVoucher.code + "_" + String.format("%04d", rnd.nextInt(1, 51));
                PENDING_VOUCHER.remove(customerId); // Sử dụng thành công
            }
        }
        
        int normalDiscount  = (int) (totalBill * rnd.nextDouble(0, 0.05));
        int totalDiscount   = voucherDiscount + normalDiscount;
        int totalPayment    = Math.max(0, totalBill - totalDiscount);

        String status       = "Completed";
        long modifiedDate   = orderDate;
        String shopCode     = IdPool.pickPareto(IdPool.SHOP_CODES);
        String empId        = IdPool.pick(IdPool.EMPLOYEE_IDS);

        // ── Return: 25% xác suất, chỉ với đơn Completed, trong vòng 7 ngày ───────
        Return returnRecord  = null;
        List<ReturnItem> returnItems = null;

        if (rnd.nextInt(RETURN_PROBABILITY) == 0) {
            long timeSinceOrder = now - orderDate;  // ms đã qua từ ngày mua
            long maxReturnDelay = Math.min(SEVEN_DAYS_MS, timeSinceOrder);

            if (maxReturnDelay > 60_000) { // cần ít nhất 1 phút để có return hợp lệ
                long returnDate = orderDate + rnd.nextLong(60_000L, maxReturnDelay);
                String retId    = "RET-" + uuid8();

                // Chọn FULL hoặc PARTIAL
                boolean fullReturn = rnd.nextBoolean();
                int numItemsToReturn = fullReturn ? items.size() : rnd.nextInt(1, items.size() + 1);

                // Shuffle để lấy ngẫu nhiên subset
                List<OrderItem> shuffled = new ArrayList<>(items);
                Collections.shuffle(shuffled, new Random(rnd.nextLong()));
                List<OrderItem> selectedItems = shuffled.subList(0, numItemsToReturn);

                int returnAmount = 0;
                returnItems = new ArrayList<>();

                for (OrderItem src : selectedItems) {
                    int maxQty   = src.getQuantity() != null ? src.getQuantity() : 1;
                    int retQty   = fullReturn ? maxQty : rnd.nextInt(1, maxQty + 1); // partial qty
                    int retPrice = src.getUnitPrice() != null ? src.getUnitPrice() : 0;
                    int itemAmt  = retQty * retPrice;
                    returnAmount += itemAmt;

                    returnItems.add(new ReturnItem(
                            "RI-" + uuid8(), retId, src.getProductId(),
                            retQty, retPrice, src.getId(), orderId, itemAmt,
                            returnDate, returnDate));
                }

                returnRecord = new Return(retId, orderId, returnAmount, returnDate, returnDate);
                modifiedDate = returnDate;
                if (fullReturn) {
                    status = "Returned";
                } else {
                    status = "Completed";
                }
            }
        }

        Order order = new Order(orderId, customerId, shopCode, orderDate,
                totalBill, totalDiscount, totalPayment, empId, status,
                appliedVoucherSeriesCode, orderDate, modifiedDate);

        return new OrderBundle(order, items, returnRecord, returnItems);
    }

    private static String uuid8() {
        return UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }
}
