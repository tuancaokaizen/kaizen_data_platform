package com.kaizen.flink.simulate;

import com.kaizen.flink.model.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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
 *    - Return được giả lập trễ (gửi sự kiện trả hàng kèm đầy đủ thông tin bán hàng sau sự kiện bán hàng 15 giây).
 */
public final class OrderSimulator {

    private static final int  VOUCHER_THRESHOLD  = 10;      // đơn hàng cần để nhận voucher
    private static final int  RETURN_PROBABILITY = 4;       // 1/4 = 25%

    /** Đếm số lần mua của từng customer. */
    private static final ConcurrentHashMap<String, AtomicInteger> ORDER_COUNT =
            new ConcurrentHashMap<>();

    /** Voucher đang chờ áp dụng cho customer (chỉ giữ 1 voucher/customer). */
    private static final ConcurrentHashMap<String, String> PENDING_VOUCHER =
            new ConcurrentHashMap<>();

    // Cấu trúc lưu trữ đơn hàng sẽ bị trả trong tương lai
    private static final class PendingReturn {
        final Order order;
        final List<OrderItem> items;
        final Return returnRecord;
        final List<ReturnItem> returnItems;
        final long scheduledTime;

        PendingReturn(Order order, List<OrderItem> items, Return returnRecord, List<ReturnItem> returnItems, long scheduledTime) {
            this.order = order;
            this.items = items;
            this.returnRecord = returnRecord;
            this.returnItems = returnItems;
            this.scheduledTime = scheduledTime;
        }
    }

    private static final Queue<PendingReturn> PENDING_RETURNS = new ConcurrentLinkedQueue<>();

    private OrderSimulator() {}

    public static OrderBundle generate() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long now = System.currentTimeMillis();

        // 1. Kiểm tra xem có đơn hàng nào chờ trả hàng đã đến giờ phát sự kiện không
        PendingReturn readyReturn = null;
        for (PendingReturn pr : PENDING_RETURNS) {
            if (now >= pr.scheduledTime) {
                readyReturn = pr;
                break;
            }
        }

        if (readyReturn != null) {
            PENDING_RETURNS.remove(readyReturn);
            
            // Đưa mã return thực tế vào hàng chờ động để PaymentSimulator kết nối thanh toán (hoàn tiền)
            IdPool.GENERATED_RETURNS.add(new IdPool.ReturnRef(
                readyReturn.returnRecord.getId(),
                readyReturn.order.getId(),
                readyReturn.returnRecord.getReturnAmount() != null ? readyReturn.returnRecord.getReturnAmount() : 0
            ));
            if (IdPool.GENERATED_RETURNS.size() > 1000) {
                IdPool.GENERATED_RETURNS.poll();
            }

            // Trả về gói dữ liệu chứa cả thông tin bán hàng gốc và thông tin trả hàng đầy đủ
            return new OrderBundle(readyReturn.order, readyReturn.items, readyReturn.returnRecord, readyReturn.returnItems);
        }

        // 2. Nếu không có đơn nào đến giờ trả, sinh một đơn hàng mới (Realtime)
        long orderDate = now - rnd.nextLong(1000L, 5000L);

        // ── Customer + Voucher logic ──────────────────────────────────────────
        String customerId  = IdPool.pickPareto(IdPool.CUSTOMER_IDS);
        int orderCount     = ORDER_COUNT.computeIfAbsent(customerId, k -> new AtomicInteger(0))
                                        .incrementAndGet();

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
        String orderId = "ORD-" + uuid12();
        int itemCount  = rnd.nextInt(1, 6);
        List<OrderItem> items = new ArrayList<>(itemCount);
        int totalBill = 0;

        for (int i = 0; i < itemCount; i++) {
            String itemId = "OI-" + uuid12();
            IdPool.ProductRef prod = IdPool.PRODUCTS.get(rnd.nextInt(IdPool.PRODUCTS.size()));
            int qty       = rnd.nextInt(1, 10);
            int unitPrice = prod.retailPrice;
            totalBill    += qty * unitPrice;
            items.add(new OrderItem(itemId, orderId, prod.id, qty, unitPrice, orderDate, orderDate));
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
            IdPool.GiftRef chosenGift = applicableGifts.get(rnd.nextInt(applicableGifts.size()));
            String giftItemId = "OI-" + uuid12();
            items.add(new OrderItem(giftItemId, orderId, chosenGift.productId, 1, 0, orderDate, orderDate));
        }

        // ── Discount: voucher ────────────────────────────────────────────────
        // Lấy voucher đang chờ của customer này hoặc áp dụng ngẫu nhiên 30%
        String pendingVoucherCode = PENDING_VOUCHER.get(customerId);
        IdPool.VoucherRef appliedVoucher = null;
        if (pendingVoucherCode != null) {
            for (IdPool.VoucherRef v : IdPool.VOUCHERS) {
                if (v.code.equals(pendingVoucherCode)) {
                    appliedVoucher = v;
                    break;
                }
            }
        } else {
            // Nếu không có voucher pending, cho 30% cơ hội áp dụng trực tiếp 1 voucher đang hoạt động thỏa mãn minOrderValue
            if (rnd.nextDouble() < 0.3) {
                List<IdPool.VoucherRef> activeVouchers = new ArrayList<>();
                for (IdPool.VoucherRef v : IdPool.VOUCHERS) {
                    if (v.startDate <= orderDate && orderDate <= v.endDate && totalBill >= v.minOrderValue) {
                        activeVouchers.add(v);
                    }
                }
                if (!activeVouchers.isEmpty()) {
                    appliedVoucher = activeVouchers.get(rnd.nextInt(activeVouchers.size()));
                }
            }
        }

        int voucherDiscount = 0;
        String appliedVoucherSeriesCode = null;
        if (appliedVoucher != null) {
            if (orderDate > appliedVoucher.endDate) {
                PENDING_VOUCHER.remove(customerId);
            } else if (orderDate >= appliedVoucher.startDate && totalBill >= appliedVoucher.minOrderValue) {
                voucherDiscount = appliedVoucher.amount;
                appliedVoucherSeriesCode = "SR_" + appliedVoucher.code + "_" + String.format("%04d", rnd.nextInt(1, 51));
                PENDING_VOUCHER.remove(customerId);
            }
        }
        
        int totalDiscount   = voucherDiscount;
        int totalPayment    = Math.max(0, totalBill - totalDiscount);

        String shopCode     = IdPool.pickPareto(IdPool.SHOP_CODES);
        String empId        = IdPool.pick(IdPool.EMPLOYEE_IDS);

        // ── Quyết định xem đơn này có bị trả lại sau này hay không (25% xác suất) ──
        boolean willBeReturned = rnd.nextInt(RETURN_PROBABILITY) == 0;

        if (willBeReturned) {
            // Lọc danh sách sản phẩm có thể trả (có giá trị > 0) để tránh trả quà tặng 0đ
            List<OrderItem> returnableItems = new ArrayList<>();
            for (OrderItem item : items) {
                if (item.getUnitPrice() != null && item.getUnitPrice() > 0) {
                    returnableItems.add(item);
                }
            }

            if (!returnableItems.isEmpty()) {
                boolean fullReturn = rnd.nextBoolean();
                int numItemsToReturn = fullReturn ? returnableItems.size() : rnd.nextInt(1, returnableItems.size() + 1);

                List<OrderItem> shuffled = new ArrayList<>(returnableItems);
                Collections.shuffle(shuffled, new Random(rnd.nextLong()));
                List<OrderItem> selectedItems = shuffled.subList(0, numItemsToReturn);

                int returnAmount = 0;
                List<ReturnItem> returnItems = new ArrayList<>();
                long returnDate = orderDate + rnd.nextLong(1000L, 5000L); // ngày trả cách ngày bán 1-5 giây
                String retId = "RET-" + uuid12();

                boolean isZeroReturn = totalPayment <= totalDiscount;
                double discountRatio = (double) totalDiscount / totalBill;

                for (OrderItem src : selectedItems) {
                    int maxQty   = src.getQuantity() != null ? src.getQuantity() : 1;
                    int retQty   = fullReturn ? maxQty : rnd.nextInt(1, maxQty + 1);
                    int salePrice = src.getUnitPrice() != null ? src.getUnitPrice() : 0;
                    
                    int itemAmt;
                    if (isZeroReturn) {
                        itemAmt = 0;
                    } else {
                        int discountedPrice = (int) Math.max(0, salePrice - (salePrice * discountRatio));
                        itemAmt = retQty * discountedPrice;
                    }
                    returnAmount += itemAmt;

                    returnItems.add(new ReturnItem(
                            "RI-" + uuid12(), retId, src.getProductId(),
                            retQty, salePrice, src.getId(), orderId, itemAmt,
                            returnDate, returnDate));
                }

                Return returnRecord = new Return(retId, orderId, returnAmount, returnDate, returnDate);
                String finalStatus = fullReturn ? "Returned" : "Completed";

                // Tạo đối tượng Order cập nhật với trạng thái khi trả hàng và modifiedDate mới
                Order returnedOrder = new Order(orderId, customerId, shopCode, orderDate,
                        totalBill, totalDiscount, totalPayment, empId, finalStatus,
                        appliedVoucherSeriesCode, orderDate, returnDate);

                // Lưu vào hàng chờ để phát sự kiện trả hàng (mang theo data bán hàng gốc) sau 15 giây
                PENDING_RETURNS.add(new PendingReturn(returnedOrder, items, returnRecord, returnItems, now + 15000L));
            }
        }

        // Tạo đơn hàng bán hàng ban đầu với trạng thái Completed
        Order initialOrder = new Order(orderId, customerId, shopCode, orderDate,
                totalBill, totalDiscount, totalPayment, empId, "Completed",
                appliedVoucherSeriesCode, orderDate, orderDate);

        // Trả về gói chỉ chứa đơn hàng bán hàng ban đầu (chưa có Return)
        return new OrderBundle(initialOrder, items, null, null);
    }

    private static String uuid12() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
    }
}
