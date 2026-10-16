package com.kaizen.stream.simulation.order.transform;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.model.order.OrderHeader;
import com.kaizen.stream.model.order.OrderLine;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.order.OrderStatus;
import com.kaizen.stream.model.order.ReturnDocument;
import com.kaizen.stream.model.order.ReturnLine;
import com.kaizen.stream.simulation.order.lookup.MasterData;
import com.kaizen.stream.utils.money.Money;
import com.kaizen.stream.utils.time.IctTime;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;

/**
 * Builds orders and their returns from the seed, a sequence number and master data only: same inputs →
 * same order, so replays after a restore re-create identical events. Every output satisfies OrderRules.
 * Dựng đơn và phiếu trả chỉ từ seed, số thứ tự và master data: cùng đầu vào → cùng đơn, nên phát lại sau
 * restore tạo ra sự kiện giống hệt. Mọi đầu ra đều thoả OrderRules.
 */
public final class OrderGenerator implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter DAY = DateTimeFormatter.BASIC_ISO_DATE;
    /** Zipf exponent of product popularity (3.3). / Số mũ Zipf của độ phổ biến sản phẩm. */
    static final double ZIPF_EXPONENT = 0.8;
    /** Regulars: this share of customers places 80% of member orders. / Khách quen chiếm 80% đơn thành viên. */
    static final double REGULAR_SHARE = 0.2;
    static final double REGULAR_ORDERS = 0.8;
    static final String[] RETURN_REASONS = {"CHANGED_MIND", "WRONG_ITEM", "DEFECTIVE", "ADVERSE_REACTION"};

    private final StreamSettings.Order profile;
    private final DemandModel demand;

    public OrderGenerator(StreamSettings.Order profile, DemandModel demand) {
        this.profile = profile;
        this.demand = demand;
    }

    /** Per-shop, per-day order counter used in order ids. / Bộ đếm đơn theo shop, theo ngày dùng trong id đơn. */
    @FunctionalInterface
    public interface ShopSequence {
        int next(String shopCode);
    }

    /** Next arrival after {@code from} for sequence number {@code seq}. / Thời điểm đơn kế tiếp sau {@code from}. */
    public long nextArrival(long from, long seq) {
        SplittableRandom rnd = random(profile.getSeed() + "|arrival|" + seq);
        return DemandModel.nextArrival(demand, from, -Math.log(1 - rnd.nextDouble()));
    }

    /**
     * The order with sequence number {@code seq} placed at {@code at}, when no voucher code has been handed out yet.
     * Đơn có số thứ tự {@code seq}, đặt lúc {@code at}, khi chưa phát ra mã voucher nào.
     */
    public OrderSnapshot place(MasterData m, long seq, long at, ShopSequence shopSequence) {
        return place(m, seq, at, shopSequence, Collections.emptySet());
    }

    /**
     * The order with sequence number {@code seq} placed at {@code at}; voucher codes in {@code takenSeries} are
     * not reused. The caller records the code of the returned order.
     * Đơn có số thứ tự {@code seq}, đặt lúc {@code at}; mã voucher trong {@code takenSeries} không dùng lại.
     * Bên gọi ghi nhận mã của đơn trả về.
     */
    public OrderSnapshot place(MasterData m, long seq, long at, ShopSequence shopSequence, Set<String> takenSeries) {
        SplittableRandom rnd = random(profile.getSeed() + "|order|" + seq);
        String shop = pickShop(m, rnd, at);
        String orderId = String.format("SORD-%s-%s-%05d",
                IctTime.businessDate(at).format(DAY), shop, shopSequence.next(shop));
        List<String> staff = m.employeesOf(shop);
        String employee = staff.get(rnd.nextInt(staff.size()));
        String customer = pickCustomer(m, rnd);

        List<OrderLine> lines = saleLines(m, rnd, orderId);
        long bill = 0;
        long[] gross = new long[lines.size()];
        for (int i = 0; i < lines.size(); i++) {
            gross[i] = lines.get(i).getQuantity() * lines.get(i).getUnitPrice();
            bill += gross[i];
        }
        addGift(m, rnd, at, orderId, lines);

        // Voucher discount is spread over sale lines by gross amount; gift lines get none
        // Chiết khấu voucher chia cho dòng bán theo thành tiền; dòng quà không nhận
        String series = null;
        long discount = 0;
        if (rnd.nextDouble() < profile.getVoucherRate()) {
            // Vouchers with no free code left are not eligible / Voucher hết mã trống thì không đủ điều kiện
            List<MasterData.Voucher> eligible = new ArrayList<>();
            List<List<String>> free = new ArrayList<>();
            for (MasterData.Voucher v : m.getVouchers()) {
                List<String> codes = v.activeAt(at) && v.getMinOrderValue() <= bill && v.getAmount() > 0
                        ? v.freeSeries(at, takenSeries) : Collections.emptyList();
                if (!codes.isEmpty()) {
                    eligible.add(v);
                    free.add(codes);
                }
            }
            if (!eligible.isEmpty()) {
                int k = rnd.nextInt(eligible.size());
                series = free.get(k).get(rnd.nextInt(free.get(k).size()));
                discount = Math.min(eligible.get(k).getAmount(), bill);
            }
        }
        long[] parts = Money.allocate(discount, gross);
        for (int i = 0; i < lines.size(); i++) {
            OrderLine l = lines.get(i);
            long d = i < parts.length ? parts[i] : 0;
            l.setDiscountAmount(d);
            l.setLineAmount(l.getQuantity() * l.getUnitPrice() - d);
        }

        OrderHeader header = OrderHeader.builder()
                .id(orderId).shopCode(shop).customerId(customer).employeeSaleId(employee)
                .transactionDate(at).totalBill(bill).totalDiscount(discount).totalPayment(bill - discount)
                .voucherSeriesCode(series).status(OrderStatus.COMPLETED)
                .createdDate(at).modifiedDate(at)
                .build();
        return OrderSnapshot.builder().order(header).items(lines).returns(new ArrayList<>()).build();
    }

    /**
     * The return this order will get, already applied to the snapshot (status, modifiedDate); empty for most orders.
     * Phiếu trả đơn này sẽ có, đã áp vào snapshot (status, modifiedDate); phần lớn đơn không có.
     */
    public Optional<OrderSnapshot> planReturn(OrderSnapshot placed) {
        OrderHeader o = placed.getOrder();
        SplittableRandom rnd = random(profile.getSeed() + "|return|" + o.getId());
        if (rnd.nextDouble() >= profile.getReturnRate()) {
            return Optional.empty();
        }
        long at = o.getTransactionDate() + between(rnd, profile.getReturnDelayMinMs(), profile.getReturnDelayMaxMs());
        String returnId = "RET-" + o.getId() + "-1";
        boolean full = rnd.nextDouble() < profile.getReturnFullRate();

        List<OrderLine> taken = new ArrayList<>();
        if (full) {
            taken.addAll(placed.getItems());
        } else {
            // Partial returns take each paid line with probability ½ (at least one); gifts stay with the customer
            // Trả một phần: mỗi dòng có tiền lấy với xác suất ½ (ít nhất một dòng); quà tặng khách giữ lại
            List<OrderLine> paid = new ArrayList<>();
            for (OrderLine l : placed.getItems()) {
                if (!l.getGift() && l.getLineAmount() > 0) {
                    paid.add(l);
                }
            }
            for (OrderLine l : paid) {
                if (rnd.nextBoolean()) {
                    taken.add(l);
                }
            }
            if (taken.isEmpty() && !paid.isEmpty()) {
                taken.add(paid.get(rnd.nextInt(paid.size())));
            }
        }

        List<ReturnLine> lines = new ArrayList<>();
        long amount = 0;
        for (OrderLine sold : taken) {
            int qty = full ? sold.getQuantity() : 1 + rnd.nextInt(sold.getQuantity());
            long lineAmount = qty == sold.getQuantity()
                    ? sold.getLineAmount()
                    : sold.getLineAmount() * qty / sold.getQuantity();
            int restock = 0;
            for (int u = 0; u < qty; u++) {
                restock += rnd.nextDouble() < profile.getRestockRate() ? 1 : 0;
            }
            lines.add(ReturnLine.builder()
                    .id(returnId + "-L" + (lines.size() + 1)).orderItemId(sold.getId()).productId(sold.getProductId())
                    .quantity(qty).restockQty(restock).damageQty(qty - restock)
                    .unitPrice(sold.getUnitPrice()).unitCost(sold.getUnitCost()).lineAmount(lineAmount)
                    .build());
            amount += lineAmount;
        }
        // Nothing paid to give back → no return / Không có tiền để hoàn → không có phiếu trả
        if (amount <= 0) {
            return Optional.empty();
        }
        ReturnDocument ret = ReturnDocument.builder()
                .id(returnId).createdDate(at).reason(RETURN_REASONS[rnd.nextInt(RETURN_REASONS.length)])
                .returnAmount(amount).items(lines)
                .build();
        List<ReturnDocument> returns = new ArrayList<>();
        returns.add(ret);
        OrderHeader returned = o.toBuilder()
                .status(OrderStatus.of(o.getTotalPayment(), returns.size(), amount))
                .modifiedDate(at)
                .build();
        return Optional.of(placed.toBuilder().order(returned).returns(returns).build());
    }

    private String pickShop(MasterData m, SplittableRandom rnd, long at) {
        List<String> shops = m.getShopCodes();
        double[] w = new double[shops.size()];
        for (int i = 0; i < w.length; i++) {
            w[i] = demand.shopWeight(i, at);
        }
        return shops.get(pick(rnd, w));
    }

    private String pickCustomer(MasterData m, SplittableRandom rnd) {
        List<String> ids = m.getCustomerIds();
        if (ids.isEmpty() || rnd.nextDouble() < profile.getWalkInRate()) {
            return null;
        }
        // Pareto 80/20: most member orders come from the regulars / Phần lớn đơn thành viên đến từ khách quen
        int regulars = Math.max(1, (int) (ids.size() * REGULAR_SHARE));
        if (rnd.nextDouble() < REGULAR_ORDERS || regulars >= ids.size()) {
            return ids.get(rnd.nextInt(Math.min(regulars, ids.size())));
        }
        return ids.get(regulars + rnd.nextInt(ids.size() - regulars));
    }

    /** 1 + Poisson(extra) distinct products, Zipf by popularity rank. / 1 + Poisson(extra) sản phẩm khác nhau, Zipf. */
    private List<OrderLine> saleLines(MasterData m, SplittableRandom rnd, String orderId) {
        List<MasterData.Product> products = m.getProducts();
        int wanted = Math.min(Math.min(1 + poisson(rnd, profile.getExtraLinesMean()), profile.getMaxLines()),
                products.size());
        double[] w = new double[products.size()];
        for (int i = 0; i < w.length; i++) {
            w[i] = Math.pow(i + 1, -ZIPF_EXPONENT);
        }
        Set<Integer> picked = new HashSet<>();
        List<OrderLine> lines = new ArrayList<>();
        while (lines.size() < wanted) {
            int i = pick(rnd, w);
            if (!picked.add(i)) {
                continue;
            }
            // Drawn without replacement / Lấy không hoàn lại
            w[i] = 0;
            MasterData.Product p = products.get(i);
            lines.add(OrderLine.builder()
                    .id(orderId + "-L" + (lines.size() + 1)).lineNo(lines.size() + 1).productId(p.getId())
                    .quantity(1 + poisson(rnd, unitsMean(p.getUnit())))
                    .unitPrice(p.getRetailPrice()).unitCost(p.getStockPrice()).gift(false)
                    .build());
        }
        return lines;
    }

    private void addGift(MasterData m, SplittableRandom rnd, long at, String orderId, List<OrderLine> lines) {
        List<MasterData.GiftOffer> active = new ArrayList<>();
        for (MasterData.GiftOffer g : m.getGifts()) {
            if (g.activeAt(at) && m.getProductsById().containsKey(g.getProductId())) {
                active.add(g);
            }
        }
        if (active.isEmpty() || rnd.nextDouble() >= profile.getGiftRate()) {
            return;
        }
        MasterData.Product p = m.getProductsById().get(active.get(rnd.nextInt(active.size())).getProductId());
        int lineNo = lines.size() + 1;
        lines.add(OrderLine.builder()
                .id(orderId + "-L" + lineNo).lineNo(lineNo).productId(p.getId()).quantity(1)
                .unitPrice(0L).unitCost(p.getStockPrice()).gift(true)
                .build());
    }

    /** λ_unit by packaging (3.3). / λ theo quy cách đóng gói. */
    static double unitsMean(String unit) {
        if (unit == null) {
            return 0.5;
        }
        switch (unit) {
            case "Hộp":
                return 0.4;
            case "Chai":
                return 0.3;
            case "Vỉ":
                return 1.2;
            case "Viên":
                return 4.0;
            default:
                return 0.5;
        }
    }

    /** Knuth's method; fine for the small means used here. / Phương pháp Knuth, đủ tốt với kỳ vọng nhỏ. */
    static int poisson(SplittableRandom rnd, double mean) {
        if (mean <= 0) {
            return 0;
        }
        double limit = Math.exp(-mean);
        double p = 1;
        int k = 0;
        do {
            k++;
            p *= rnd.nextDouble();
        } while (p > limit);
        return k - 1;
    }

    private static int pick(SplittableRandom rnd, double[] weights) {
        double sum = 0;
        for (double w : weights) {
            sum += w;
        }
        double u = rnd.nextDouble() * sum;
        int last = -1;
        for (int i = 0; i < weights.length; i++) {
            if (weights[i] <= 0) {
                continue;
            }
            last = i;
            u -= weights[i];
            if (u < 0) {
                return i;
            }
        }
        // Rounding left u ≥ 0: take the last positive weight / Sai số làm tròn: lấy trọng số dương cuối
        if (last < 0) {
            throw new IllegalStateException("No positive weight to pick from");
        }
        return last;
    }

    private static long between(SplittableRandom rnd, long min, long max) {
        return min == max ? min : rnd.nextLong(min, max + 1);
    }

    /** Stable seed per name (name-based UUID). / Seed cố định theo tên. */
    private static SplittableRandom random(String name) {
        UUID u = UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8));
        return new SplittableRandom(u.getMostSignificantBits() ^ u.getLeastSignificantBits());
    }
}
