package com.kaizen.flink.simulate;

import com.kaizen.flink.config.FlinkConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Pool ID dùng chung giữa các Simulator để đảm bảo Referential Integrity.
 * Load dữ liệu động từ PostgreSQL thay vì sinh ngẫu nhiên tĩnh.
 */
public final class IdPool {

    // ── Master Data IDs (Loaded dynamically) ──────────────────────────────────
    public static List<String> GEOGRAPHY_IDS   = Collections.emptyList();
    public static List<String> PRODUCT_IDS     = Collections.emptyList();
    public static List<String> SHOP_CODES      = Collections.emptyList();
    public static List<String> EMPLOYEE_IDS    = Collections.emptyList();
    public static List<String> CUSTOMER_IDS    = Collections.emptyList();
    public static List<String> ORDER_IDS       = Collections.emptyList();
    
    // Đối tượng tham chiếu kèm thời gian hiệu lực
    public static List<VoucherRef> VOUCHERS     = Collections.emptyList();
    public static List<PromotionRef> PROMOTIONS = Collections.emptyList();
    public static List<GiftRef> GIFTS           = Collections.emptyList();

    // ── Reference classes for time constraints & values ─────────────────────
    public static final class VoucherRef {
        public final String code;
        public final long startDate;
        public final long endDate;
        public final int amount;
        public final int minOrderValue;

        public VoucherRef(String code, long startDate, long endDate, int amount, int minOrderValue) {
            this.code = code;
            this.startDate = startDate;
            this.endDate = endDate;
            this.amount = amount;
            this.minOrderValue = minOrderValue;
        }
    }

    public static final class PromotionRef {
        public final String code;
        public final String type; // "VOUCHER" | "GIFT"
        public final long startDate;
        public final long endDate;

        public PromotionRef(String code, String type, long startDate, long endDate) {
            this.code = code;
            this.type = type;
            this.startDate = startDate;
            this.endDate = endDate;
        }
    }

    public static final class GiftRef {
        public final String id;
        public final String productId;
        public final String promotionCode;
        public final int quantity;

        public GiftRef(String id, String productId, String promotionCode, int quantity) {
            this.id = id;
            this.productId = productId;
            this.promotionCode = promotionCode;
            this.quantity = quantity;
        }
    }

    // ── Reference data (smallint keys cho PaymentType / PaymentMethod) ───────
    /** 1=Tiền mặt, 2=Thẻ ngân hàng, 3=Ví điện tử */
    public static final short[] PAYMENT_TYPE_IDS    = {1, 2, 3};
    public static final short[] PAYMENT_METHOD_IDS  = {1, 2, 3};

    public static final String[] PAYMENT_TYPE_NAMES   =
            {"Thanh toán tiền mặt", "Thanh toán thẻ", "Thanh toán ví điện tử"};
    public static final String[] PAYMENT_METHOD_NAMES =
            {"Tiền mặt", "Thẻ ngân hàng", "Ví điện tử (MoMo/ZaloPay)"};

    private static boolean initialized = false;

    private IdPool() {}

    public static synchronized void init(FlinkConfig config) {
        if (initialized) {
            return;
        }

        try (java.sql.Connection conn = com.kaizen.flink.utils.PostgresSink.getConnection(config)) {
            GEOGRAPHY_IDS   = queryIds(conn, "SELECT \"Id\" FROM public.\"Geography\"");
            PRODUCT_IDS     = queryIds(conn, "SELECT \"Id\" FROM product.\"Product\" WHERE \"Status\"=true AND \"IsPromotion\"=false");
            SHOP_CODES      = queryIds(conn, "SELECT \"Code\" FROM shop.\"Shop\"");
            EMPLOYEE_IDS    = queryIds(conn, "SELECT \"Id\" FROM shop.\"Employee\" WHERE \"Status\"=true");
            CUSTOMER_IDS    = queryIds(conn, "SELECT \"Id\" FROM customer.\"Customer\"");
            ORDER_IDS       = queryIds(conn, "SELECT \"Id\" FROM \"order\".\"Order\" LIMIT 5000");

            // Load Vouchers
            List<VoucherRef> vouchers = new ArrayList<>();
            try (java.sql.Statement stmt = conn.createStatement();
                 java.sql.ResultSet rs = stmt.executeQuery("SELECT \"Code\", \"StartDate\", \"EndDate\", \"VoucherAmount\", \"MinOrderValue\" FROM promotion.\"Voucher\" WHERE \"Status\"=true")) {
                while (rs.next()) {
                    vouchers.add(new VoucherRef(
                        rs.getString(1),
                        rs.getTimestamp(2).getTime(),
                        rs.getTimestamp(3).getTime(),
                        rs.getInt(4),
                        rs.getInt(5)
                    ));
                }
            }
            VOUCHERS = Collections.unmodifiableList(vouchers);

            // Load Promotions
            List<PromotionRef> promotions = new ArrayList<>();
            try (java.sql.Statement stmt = conn.createStatement();
                 java.sql.ResultSet rs = stmt.executeQuery("SELECT \"Code\", \"Type\", \"StartDate\", \"EndDate\" FROM promotion.\"Promotion\" WHERE \"Status\"=true")) {
                while (rs.next()) {
                    promotions.add(new PromotionRef(
                        rs.getString(1),
                        rs.getString(2),
                        rs.getTimestamp(3).getTime(),
                        rs.getTimestamp(4).getTime()
                    ));
                }
            }
            PROMOTIONS = Collections.unmodifiableList(promotions);

            // Load Gifts
            List<GiftRef> gifts = new ArrayList<>();
            try (java.sql.Statement stmt = conn.createStatement();
                 java.sql.ResultSet rs = stmt.executeQuery("SELECT \"Id\", \"ProductId\", \"PromotionCode\", \"PromotionQuantity\" FROM promotion.\"Gift\" WHERE \"Status\"=true")) {
                while (rs.next()) {
                    gifts.add(new GiftRef(
                        rs.getString(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getInt(4)
                    ));
                }
            }
            GIFTS = Collections.unmodifiableList(gifts);

            if (GEOGRAPHY_IDS.isEmpty()) {
                throw new IllegalStateException("public.\"Geography\" table is empty. Generate master data first!");
            }
            if (PRODUCT_IDS.isEmpty()) {
                throw new IllegalStateException("product.\"Product\" table is empty. Generate master data first!");
            }
            if (SHOP_CODES.isEmpty()) {
                throw new IllegalStateException("shop.\"Shop\" table is empty. Generate master data first!");
            }
            if (EMPLOYEE_IDS.isEmpty()) {
                throw new IllegalStateException("shop.\"Employee\" table is empty. Generate master data first!");
            }
            if (CUSTOMER_IDS.isEmpty()) {
                throw new IllegalStateException("customer.\"Customer\" table is empty. Generate master data first!");
            }

            initialized = true;
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize IdPool from PostgreSQL", e);
        }
    }

    private static List<String> queryIds(java.sql.Connection conn, String sql) throws java.sql.SQLException {
        List<String> list = new ArrayList<>();
        try (java.sql.Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                list.add(rs.getString(1));
            }
        }
        return Collections.unmodifiableList(list);
    }

    public static String pick(List<String> pool) {
        if (pool == null || pool.isEmpty()) {
            throw new IllegalStateException("IdPool is not initialized or database table is empty! Please call IdPool.init(config) first.");
        }
        return pool.get(ThreadLocalRandom.current().nextInt(pool.size()));
    }

    public static String pickPareto(List<String> pool) {
        if (pool == null || pool.isEmpty()) {
            throw new IllegalStateException("IdPool is not initialized or database table is empty! Please call IdPool.init(config) first.");
        }
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        int size = pool.size();
        // 80% probability to pick from the first 20% of items (the hot set)
        if (rnd.nextInt(10) < 8) {
            int limit = Math.max(1, (int) (size * 0.2));
            return pool.get(rnd.nextInt(limit));
        } else {
            int limit = Math.max(1, (int) (size * 0.2));
            if (size > limit) {
                return pool.get(limit + rnd.nextInt(size - limit));
            } else {
                return pool.get(rnd.nextInt(size));
            }
        }
    }

    public static short pickShort(short[] arr) {
        return arr[ThreadLocalRandom.current().nextInt(arr.length)];
    }
}
