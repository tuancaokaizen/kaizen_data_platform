package com.kaizen.stream.simulation.order.lookup;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.utils.connection.JdbcConnections;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

/**
 * Reads the highest number per shop from order ids SORD-yyyyMMdd-SHOPxxxx-nnnnn of one business day.
 * Đọc số lớn nhất theo shop từ id đơn SORD-yyyyMMdd-SHOPxxxx-nnnnn của một ngày nghiệp vụ.
 */
public class PostgresOrderNumberSource implements OrderNumberSource {

    private static final long serialVersionUID = 1L;

    static final String HIGHEST_PER_SHOP = "SELECT split_part(\"Id\", '-', 3), max(split_part(\"Id\", '-', 4)::int)"
            + " FROM \"order\".\"Order\""
            + " WHERE \"OrderDate\" = ? AND \"Id\" LIKE 'SORD-%'"
            + " GROUP BY 1";

    private final StreamSettings.Postgres pg;

    public PostgresOrderNumberSource(StreamSettings.Postgres pg) {
        this.pg = pg;
    }

    @Override
    public Map<String, Integer> highestPerShop(LocalDate businessDate) throws SQLException {
        Map<String, Integer> out = new HashMap<>();
        try (Connection c = JdbcConnections.openPostgres(pg, "kaizen-flink-simulation-order");
             PreparedStatement st = c.prepareStatement(HIGHEST_PER_SHOP)) {
            st.setDate(1, Date.valueOf(businessDate));
            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    out.put(rs.getString(1), rs.getInt(2));
                }
            }
        }
        return out;
    }
}
