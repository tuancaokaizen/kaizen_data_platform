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
    public static List<String> GEOGRAPHY_IDS = Collections.emptyList();
    public static List<String> PRODUCT_IDS = Collections.emptyList();
    public static List<ProductRef> PRODUCTS = Collections.emptyList();
    public static List<String> SHOP_CODES = Collections.emptyList();
    public static List<String> EMPLOYEE_IDS = Collections.emptyList();
    public static List<String> CUSTOMER_IDS = Collections.emptyList();
    public static List<String> ORDER_IDS = Collections.emptyList();

    // Đối tượng tham chiếu kèm thời gian hiệu lực
    public static List<VoucherRef> VOUCHERS = Collections.emptyList();
    public static List<PromotionRef> PROMOTIONS = Collections.emptyList();
    public static List<GiftRef> GIFTS = Collections.emptyList();

    public static List<ReturnRef> RETURNS = Collections.emptyList();
    public static final java.util.Queue<ReturnRef> GENERATED_RETURNS = new java.util.concurrent.ConcurrentLinkedQueue<>();

    // In-memory caches to reduce PostgreSQL connection and query overhead during stream generation
    private static final java.util.Queue<ReturnRef> UNPAID_RETURNS_CACHE = new java.util.concurrent.ConcurrentLinkedQueue<>();
    private static final java.util.Queue<UnpaidOrderRef> UNPAID_ORDERS_CACHE = new java.util.concurrent.ConcurrentLinkedQueue<>();

    // ── Reference classes for time constraints & values ─────────────────────
    public static final class ReturnRef {
        public final String returnId;
        public final String orderId;
        public final int returnAmount;

        public ReturnRef(String returnId, String orderId, int returnAmount) {
            this.returnId = returnId;
            this.orderId = orderId;
            this.returnAmount = returnAmount;
        }
    }

    public static final class ProductRef {
        public final String id;
        public final int retailPrice;

        public ProductRef(String id, int retailPrice) {
            this.id = id;
            this.retailPrice = retailPrice;
        }
    }

    public static final class UnpaidOrderRef {
        public final String orderId;
        public final int totalPayment;

        public UnpaidOrderRef(String orderId, int totalPayment) {
            this.orderId = orderId;
            this.totalPayment = totalPayment;
        }
    }

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
    /**
     * 1=Phiếu thu, 2=Phiếu chi
     */
    public static final short[] PAYMENT_TYPE_IDS = {1, 2};
    public static final short[] PAYMENT_METHOD_IDS = {1, 2, 3};

    public static final String[] PAYMENT_TYPE_NAMES =
            {"Phiếu thu", "Phiếu chi"};
    public static final String[] PAYMENT_METHOD_NAMES =
            {"Tiền mặt", "Thẻ ngân hàng", "Ví điện tử (MoMo/ZaloPay)"};

    private static boolean initialized = false;

    private IdPool() {
    }

    public static synchronized void init(FlinkConfig config) {
        if (initialized) {
            return;
        }

        try (java.sql.Connection conn = com.kaizen.flink.utils.PostgresSink.getConnection(config)) {
            GEOGRAPHY_IDS = queryIds(conn, "SELECT \"Id\" FROM public.\"Geography\"");

            // Load Products with prices
            List<ProductRef> products = new ArrayList<>();
            List<String> productIds = new ArrayList<>();
            try (java.sql.Statement stmt = conn.createStatement();
                 java.sql.ResultSet rs = stmt.executeQuery("SELECT \"Id\", \"RetailPrice\" FROM product.\"Product\" WHERE \"Status\"=true AND \"IsPromotion\"=false")) {
                while (rs.next()) {
                    String id = rs.getString(1);
                    int price = rs.getInt(2);
                    products.add(new ProductRef(id, price));
                    productIds.add(id);
                }
            }
            PRODUCTS = Collections.unmodifiableList(products);
            PRODUCT_IDS = Collections.unmodifiableList(productIds);

            SHOP_CODES = queryIds(conn, "SELECT \"Code\" FROM shop.\"Shop\"");
            EMPLOYEE_IDS = queryIds(conn, "SELECT \"Id\" FROM shop.\"Employee\" WHERE \"Status\"=true");
            CUSTOMER_IDS = queryIds(conn, "SELECT \"Id\" FROM customer.\"Customer\"");
            ORDER_IDS = queryIds(conn, "SELECT \"Id\" FROM \"order\".\"Order\" LIMIT 5000");

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

            // Load Returns
            List<ReturnRef> returns = new ArrayList<>();
            try (java.sql.Statement stmt = conn.createStatement();
                 java.sql.ResultSet rs = stmt.executeQuery("SELECT \"Id\", \"OrderId\", \"ReturnAmount\" FROM \"order\".\"Return\" LIMIT 1000")) {
                while (rs.next()) {
                    returns.add(new ReturnRef(
                            rs.getString(1),
                            rs.getString(2),
                            rs.getInt(3)
                    ));
                }
            }
            RETURNS = Collections.unmodifiableList(returns);

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

    private static long lastReturnsReloadTime = 0;

    public static synchronized void reloadReturnsIfNecessary(FlinkConfig config) {
        long now = System.currentTimeMillis();
        if (now - lastReturnsReloadTime < 10000L) {
            return;
        }
        lastReturnsReloadTime = now;
        try (java.sql.Connection conn = com.kaizen.flink.utils.PostgresSink.getConnection(config)) {
            List<ReturnRef> returns = new ArrayList<>();
            try (java.sql.Statement stmt = conn.createStatement();
                 java.sql.ResultSet rs = stmt.executeQuery("SELECT \"Id\", \"OrderId\", \"ReturnAmount\" FROM \"order\".\"Return\" LIMIT 1000")) {
                while (rs.next()) {
                    returns.add(new ReturnRef(
                            rs.getString(1),
                            rs.getString(2),
                            rs.getInt(3)
                    ));
                }
            }
            RETURNS = Collections.unmodifiableList(returns);
        } catch (Exception e) {
            System.err.println("Failed to reload returns from Postgres: " + e.getMessage());
        }
    }

    private static long lastOrdersReloadTime = 0;

    public static synchronized void reloadOrdersIfNecessary(FlinkConfig config) {
        long now = System.currentTimeMillis();
        if (now - lastOrdersReloadTime < 10000L) {
            return;
        }
        lastOrdersReloadTime = now;
        try (java.sql.Connection conn = com.kaizen.flink.utils.PostgresSink.getConnection(config)) {
            ORDER_IDS = queryIds(conn, "SELECT \"Id\" FROM \"order\".\"Order\" LIMIT 5000");
        } catch (Exception e) {
            System.err.println("Failed to reload order IDs from Postgres: " + e.getMessage());
        }
    }

    public static synchronized List<ReturnRef> fetchUnpaidReturns(FlinkConfig config) {
        List<ReturnRef> list = new ArrayList<>();
        if (!UNPAID_RETURNS_CACHE.isEmpty()) {
            list.add(UNPAID_RETURNS_CACHE.poll());
            return list;
        }

        String sql = "SELECT r.\"Id\", r.\"OrderId\", r.\"ReturnAmount\" " +
                "FROM \"order\".\"Return\" r " +
                "WHERE NOT EXISTS (" +
                "  SELECT 1 FROM payment.\"ReturnPayment\" rp " +
                "  JOIN payment.\"Payment\" p ON rp.\"PaymentId\" = p.\"Id\" " +
                "  WHERE rp.\"ReturnId\" = r.\"Id\" AND rp.\"OrderId\" = r.\"OrderId\" AND p.\"Status\" = 'SUCCESS'" +
                ") " +
                "LIMIT 50";
        try (java.sql.Connection conn = com.kaizen.flink.utils.PostgresSink.getConnection(config);
             java.sql.Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                UNPAID_RETURNS_CACHE.add(new ReturnRef(
                        rs.getString(1),
                        rs.getString(2),
                        rs.getInt(3)
                ));
            }
        } catch (Exception e) {
            System.err.println("Failed to fetch unpaid returns: " + e.getMessage());
        }

        if (!UNPAID_RETURNS_CACHE.isEmpty()) {
            list.add(UNPAID_RETURNS_CACHE.poll());
        }
        return list;
    }

    public static synchronized List<UnpaidOrderRef> fetchUnpaidOrders(FlinkConfig config) {
        List<UnpaidOrderRef> list = new ArrayList<>();
        if (!UNPAID_ORDERS_CACHE.isEmpty()) {
            list.add(UNPAID_ORDERS_CACHE.poll());
            return list;
        }

        String sql = "SELECT o.\"Id\", o.\"TotalPayment\" " +
                "FROM \"order\".\"Order\" o " +
                "WHERE NOT EXISTS (" +
                "  SELECT 1 FROM payment.\"OrderPayment\" op " +
                "  JOIN payment.\"Payment\" p ON op.\"PaymentId\" = p.\"Id\" " +
                "  WHERE op.\"OrderId\" = o.\"Id\" AND p.\"Status\" = 'SUCCESS'" +
                ") " +
                "LIMIT 50";
        try (java.sql.Connection conn = com.kaizen.flink.utils.PostgresSink.getConnection(config);
             java.sql.Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                UNPAID_ORDERS_CACHE.add(new UnpaidOrderRef(
                        rs.getString(1),
                        rs.getInt(2)
                ));
            }
        } catch (Exception e) {
            System.err.println("Failed to fetch unpaid orders: " + e.getMessage());
        }

        if (!UNPAID_ORDERS_CACHE.isEmpty()) {
            list.add(UNPAID_ORDERS_CACHE.poll());
        }
        return list;
    }
}
