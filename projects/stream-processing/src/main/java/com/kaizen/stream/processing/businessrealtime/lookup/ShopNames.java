package com.kaizen.stream.processing.businessrealtime.lookup;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.utils.connection.JdbcConnections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Shop code → name from shop."Shop", reloaded at most once per refresh interval.
 * A failed reload keeps the previous names; an unknown code gives "" and the report view shows the code.
 * Mã → tên cửa hàng từ shop."Shop", nạp lại tối đa một lần mỗi chu kỳ.
 * Nạp lỗi thì giữ tên cũ; mã chưa biết trả "" và view báo cáo hiển thị mã.
 */
public class ShopNames implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ShopNames.class);
    private static final String QUERY = "SELECT \"Code\", \"Name\" FROM shop.\"Shop\"";

    private final StreamSettings.Postgres pg;
    private final long refreshMs;

    private transient Map<String, String> names;
    private transient long loadedAt;

    public ShopNames(StreamSettings.Postgres pg, long refreshMs) {
        this.pg = pg;
        this.refreshMs = refreshMs;
    }

    public String nameOf(String shopCode, long nowMs) {
        if (names == null || nowMs - loadedAt >= refreshMs) {
            reload(nowMs);
        }
        String name = names.get(shopCode);
        return name == null ? "" : name;
    }

    private void reload(long nowMs) {
        // Set first so a down PG is retried once per interval, not once per record
        // Đặt trước để PG lỗi chỉ thử lại mỗi chu kỳ, không phải mỗi record
        loadedAt = nowMs;
        if (names == null) {
            names = new HashMap<>();
        }
        try {
            names = load();
        } catch (SQLException e) {
            LOG.warn("Loading shop names failed, keeping {} cached names: {}", names.size(), e.getMessage());
        }
    }

    protected Map<String, String> load() throws SQLException {
        try (Connection c = JdbcConnections.openPostgres(pg, "kaizen-flink-processing-business-realtime")) {
            Map<String, String> out = new HashMap<>();
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(QUERY)) {
                while (rs.next()) {
                    out.put(rs.getString(1), rs.getString(2) == null ? "" : rs.getString(2));
                }
            }
            return out;
        }
    }
}
