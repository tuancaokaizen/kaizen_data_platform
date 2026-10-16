package com.kaizen.stream.simulation.order.lookup;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.utils.connection.JdbcConnections;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads master data from the information_system schemas (shop, product, customer, promotion).
 * Đọc master data từ các schema của information_system (shop, product, customer, promotion).
 */
public class PostgresMasterDataSource implements MasterDataSource {

    private static final long serialVersionUID = 1L;

    static final String SHOPS = "SELECT \"Code\" FROM shop.\"Shop\"";
    static final String EMPLOYEES = "SELECT \"Id\", \"ShopCode\" FROM shop.\"Employee\" WHERE \"Status\"";
    static final String PRODUCTS = "SELECT \"Id\", \"Unit\", \"RetailPrice\", \"StockPrice\" FROM product.\"Product\""
            + " WHERE \"Status\"";
    static final String CUSTOMERS = "SELECT \"Id\" FROM customer.\"Customer\"";
    /**
     * ACTIVE codes plus codes closed within the last day (longer than max_catchup_ms), so a replay still sees the
     * codes its first run used. Codes closed earlier are simply absent, which also means unusable.
     * Mã ACTIVE cùng mã đã đóng trong một ngày qua (dài hơn max_catchup_ms), để phát lại vẫn thấy các mã lần chạy
     * đầu đã dùng. Mã đóng lâu hơn thì không nạp, cũng tức là không dùng được.
     */
    static final String VOUCHERS = "SELECT v.\"Code\", v.\"VoucherAmount\", v.\"MinOrderValue\","
            + " v.\"StartDate\", v.\"EndDate\", s.\"SeriesCode\", s.\"CreatedDate\","
            + " s.\"CreatedDate\" + make_interval(days => s.\"ExpiryDay\"),"
            + " CASE WHEN s.\"Status\" <> 'ACTIVE' THEN coalesce(s.\"ModifiedDate\", s.\"CreatedDate\") END"
            + " FROM promotion.\"Voucher\" v"
            + " JOIN promotion.\"Promotion\" p ON p.\"Code\" = v.\"PromotionCode\" AND p.\"Status\""
            + " JOIN promotion.\"VoucherSeries\" s ON s.\"VoucherCode\" = v.\"Code\""
            + " WHERE v.\"Status\""
            + " AND (s.\"Status\" = 'ACTIVE' OR s.\"ModifiedDate\" >= now() - interval '1 day')"
            + " AND (s.\"ExpiryDay\" IS NULL"
            + " OR s.\"CreatedDate\" + make_interval(days => s.\"ExpiryDay\") >= now() - interval '1 day')";
    static final String GIFTS = "SELECT p.\"Code\", g.\"ProductId\", p.\"StartDate\", p.\"EndDate\""
            + " FROM promotion.\"Gift\" g"
            + " JOIN promotion.\"Promotion\" p ON p.\"Code\" = g.\"PromotionCode\" AND p.\"Status\" AND p.\"Type\" = 'GIFT'"
            + " WHERE g.\"Status\"";

    private final StreamSettings.Postgres pg;

    public PostgresMasterDataSource(StreamSettings.Postgres pg) {
        this.pg = pg;
    }

    @Override
    public MasterData load() throws SQLException {
        try (Connection c = JdbcConnections.openPostgres(pg, "kaizen-flink-simulation-order")) {
            // Cursor-based fetch keeps 500k customers from loading in one go
            // Fetch theo cursor để 500k khách hàng không nạp một lần
            c.setAutoCommit(false);
            c.setReadOnly(true);
            try (Statement st = c.createStatement()) {
                st.setFetchSize(10_000);
                MasterData m = MasterData.of(strings(st, SHOPS), employees(st), products(st), strings(st, CUSTOMERS),
                        vouchers(st), gifts(st));
                c.commit();
                return m;
            }
        }
    }

    private static List<String> strings(Statement st, String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                out.add(rs.getString(1));
            }
        }
        return out;
    }

    private static Map<String, List<String>> employees(Statement st) throws SQLException {
        Map<String, List<String>> out = new HashMap<>();
        try (ResultSet rs = st.executeQuery(EMPLOYEES)) {
            while (rs.next()) {
                out.computeIfAbsent(rs.getString(2), k -> new ArrayList<>()).add(rs.getString(1));
            }
        }
        return out;
    }

    private static List<MasterData.Product> products(Statement st) throws SQLException {
        List<MasterData.Product> out = new ArrayList<>();
        try (ResultSet rs = st.executeQuery(PRODUCTS)) {
            while (rs.next()) {
                out.add(new MasterData.Product(rs.getString(1), rs.getString(2), rs.getLong(3), rs.getLong(4)));
            }
        }
        return out;
    }

    private static List<MasterData.Voucher> vouchers(Statement st) throws SQLException {
        // One row per series; group them under their voucher / Mỗi series một dòng; gom theo voucher
        Map<String, MasterData.Voucher> heads = new HashMap<>();
        Map<String, List<MasterData.Series>> series = new HashMap<>();
        try (ResultSet rs = st.executeQuery(VOUCHERS)) {
            while (rs.next()) {
                String code = rs.getString(1);
                if (!heads.containsKey(code)) {
                    heads.put(code, new MasterData.Voucher(code, rs.getLong(2), rs.getLong(3),
                            rs.getTimestamp(4).getTime(), rs.getTimestamp(5).getTime(), new ArrayList<>()));
                }
                series.computeIfAbsent(code, k -> new ArrayList<>()).add(new MasterData.Series(rs.getString(6),
                        rs.getTimestamp(7).getTime(), millisOr(rs.getTimestamp(8)), millisOr(rs.getTimestamp(9))));
            }
        }
        List<MasterData.Voucher> out = new ArrayList<>();
        for (MasterData.Voucher v : heads.values()) {
            List<MasterData.Series> codes = series.get(v.getCode());
            codes.sort(Comparator.comparing(MasterData.Series::getCode));
            out.add(new MasterData.Voucher(v.getCode(), v.getAmount(), v.getMinOrderValue(), v.getStartDate(),
                    v.getEndDate(), codes));
        }
        return out;
    }

    /** NULL (no expiry, still open) → Long.MAX_VALUE. / NULL (không hết hạn, còn mở) → Long.MAX_VALUE. */
    private static long millisOr(Timestamp ts) {
        return ts == null ? Long.MAX_VALUE : ts.getTime();
    }

    private static List<MasterData.GiftOffer> gifts(Statement st) throws SQLException {
        List<MasterData.GiftOffer> out = new ArrayList<>();
        try (ResultSet rs = st.executeQuery(GIFTS)) {
            while (rs.next()) {
                out.add(new MasterData.GiftOffer(rs.getString(1), rs.getString(2),
                        rs.getTimestamp(3).getTime(), rs.getTimestamp(4).getTime()));
            }
        }
        return out;
    }
}
