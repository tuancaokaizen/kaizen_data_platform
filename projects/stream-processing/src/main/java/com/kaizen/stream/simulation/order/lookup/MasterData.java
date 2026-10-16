package com.kaizen.stream.simulation.order.lookup;

import lombok.Value;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Immutable master-data snapshot the order generator draws from. Lists are in a fixed order so the same
 * rows always give the same orders.
 * Ảnh chụp master data bất biến cho bộ sinh đơn. Danh sách có thứ tự cố định để cùng dữ liệu luôn cho cùng đơn.
 */
@Value
public class MasterData implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Shop codes ascending; rank decides the A/B/C tier. / Mã shop tăng dần; thứ hạng quyết định hạng A/B/C. */
    List<String> shopCodes;
    /** Active employees per shop, ids ascending. / Nhân viên đang làm theo shop, id tăng dần. */
    Map<String, List<String>> employeesByShop;
    /** Sellable products, most popular first. / Sản phẩm bán được, phổ biến nhất đứng đầu. */
    List<Product> products;
    /** Every product incl. gift-only ones, for cost lookup. / Mọi sản phẩm kể cả hàng tặng, để tra giá vốn. */
    Map<String, Product> productsById;
    /** Customer ids ascending; the first 20% are the regulars. / Id khách tăng dần; 20% đầu là khách quen. */
    List<String> customerIds;
    List<Voucher> vouchers;
    List<GiftOffer> gifts;

    /**
     * Normalises ordering and checks the minimum needed to build an order.
     * Chuẩn hoá thứ tự và kiểm tra tối thiểu để dựng được một đơn.
     */
    public static MasterData of(List<String> shopCodes, Map<String, List<String>> employeesByShop,
                                List<Product> allProducts, List<String> customerIds,
                                List<Voucher> vouchers, List<GiftOffer> gifts) {
        List<String> shops = sorted(shopCodes);
        Map<String, List<String>> employees = new TreeMap<>();
        employeesByShop.forEach((shop, ids) -> employees.put(shop, sorted(ids)));

        Map<String, Product> byId = new HashMap<>();
        List<Product> sellable = new ArrayList<>();
        for (Product p : allProducts) {
            byId.put(p.getId(), p);
            if (p.getRetailPrice() > 0) {
                sellable.add(p);
            }
        }
        // Popularity rank is a stable hash of the id, not the id order itself
        // Thứ hạng phổ biến là hash cố định của id, không theo thứ tự id
        sellable.sort(Comparator.comparingLong((Product p) -> popularityKey(p.getId())).thenComparing(Product::getId));

        List<Voucher> vs = new ArrayList<>(vouchers);
        vs.sort(Comparator.comparing(Voucher::getCode));
        List<GiftOffer> gs = new ArrayList<>(gifts);
        gs.sort(Comparator.comparing(GiftOffer::getPromotionCode).thenComparing(GiftOffer::getProductId));

        MasterData m = new MasterData(Collections.unmodifiableList(shops), Collections.unmodifiableMap(employees),
                Collections.unmodifiableList(sellable), Collections.unmodifiableMap(byId),
                Collections.unmodifiableList(sorted(customerIds)), Collections.unmodifiableList(vs),
                Collections.unmodifiableList(gs));
        m.check();
        return m;
    }

    /** Employees of the shop, or every employee when the shop has none. / Nhân viên của shop, không có thì lấy tất cả. */
    public List<String> employeesOf(String shopCode) {
        List<String> own = employeesByShop.get(shopCode);
        if (own != null && !own.isEmpty()) {
            return own;
        }
        List<String> all = new ArrayList<>();
        employeesByShop.values().forEach(all::addAll);
        return all;
    }

    private void check() {
        // Without these an order cannot be built; customers may be empty (walk-ins only)
        // Thiếu các nhóm này thì không dựng được đơn; khách hàng có thể rỗng (chỉ khách vãng lai)
        if (shopCodes.isEmpty() || products.isEmpty() || employeesByShop.values().stream().allMatch(List::isEmpty)) {
            throw new IllegalStateException("Master data is missing: shops=" + shopCodes.size()
                    + ", sellable products=" + products.size() + ", employees="
                    + employeesByShop.values().stream().mapToInt(List::size).sum()
                    + ". Load master data into PG first");
        }
    }

    private static List<String> sorted(List<String> in) {
        List<String> out = new ArrayList<>(in);
        Collections.sort(out);
        return out;
    }

    private static long popularityKey(String id) {
        return UUID.nameUUIDFromBytes(("popularity|" + id).getBytes(StandardCharsets.UTF_8)).getMostSignificantBits();
    }

    @Value
    public static class Product implements Serializable {
        String id;
        /** Hộp, Chai, Vỉ, Viên… */
        String unit;
        long retailPrice;
        long stockPrice;
    }

    @Value
    public static class Voucher implements Serializable {
        String code;
        long amount;
        long minOrderValue;
        long startDate;
        long endDate;
        /** Series codes, code ascending. / Các mã series, tăng dần theo mã. */
        List<Series> series;

        public boolean activeAt(long t) {
            return startDate <= t && t <= endDate;
        }

        /**
         * Codes usable at {@code t} that the stream has not handed out yet, code ascending.
         * Các mã dùng được tại {@code t} mà stream chưa phát ra, tăng dần theo mã.
         */
        public List<String> freeSeries(long t, Set<String> taken) {
            List<String> out = new ArrayList<>();
            for (Series s : series) {
                if (s.availableAt(t) && !taken.contains(s.getCode())) {
                    out.add(s.getCode());
                }
            }
            return out;
        }
    }

    /**
     * A single-use voucher code. closedAt is when it stopped being usable (used, expired…), Long.MAX_VALUE while
     * still ACTIVE in PG. Deciding by time rather than by status keeps a replay after restore identical even though
     * the first run has already marked its codes USED.
     * Một mã voucher dùng một lần. closedAt là lúc mã hết dùng được (đã dùng, hết hạn…), Long.MAX_VALUE khi còn
     * ACTIVE trong PG. Xét theo thời gian thay vì trạng thái giúp phát lại sau restore giống hệt dù lần chạy đầu đã
     * đánh dấu mã USED.
     */
    @Value
    public static class Series implements Serializable {
        String code;
        long createdAt;
        long expiresAt;
        long closedAt;

        public boolean availableAt(long t) {
            return createdAt <= t && t < expiresAt && t <= closedAt;
        }
    }

    @Value
    public static class GiftOffer implements Serializable {
        String promotionCode;
        String productId;
        long startDate;
        long endDate;

        public boolean activeAt(long t) {
            return startDate <= t && t <= endDate;
        }
    }
}
