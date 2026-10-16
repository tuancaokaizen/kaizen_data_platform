package com.kaizen.stream.simulation.saleswriter.sink.step;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.simulation.saleswriter.model.SalesKeys;
import com.kaizen.stream.utils.connector.sink.postgres.PostgresAggregateSink;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.TreeSet;

/**
 * Posts SALE / RETURN_RESTOCK stock movements for the orders of a batch, in the transaction that upserted them,
 * so an order and its stock change commit together. The PG function skips lines already in the ledger
 * and orders before inventory."PostingCutover", so replays and old orders post nothing.
 * Ghi biến động kho SALE / RETURN_RESTOCK cho các đơn của batch, trong transaction đã upsert chúng, nên đơn
 * và thay đổi tồn commit cùng nhau. Hàm PG bỏ qua dòng đã có trong sổ cái và đơn trước inventory."PostingCutover",
 * nên đọc lại hay đơn cũ đều không ghi gì.
 */
public final class StockPosting implements PostgresAggregateSink.BatchStep<OrderSnapshot> {

    private static final long serialVersionUID = 1L;

    static final String SQL = "SELECT inventory.post_sales_stock(?)";

    @Override
    public void apply(Connection connection, List<Envelope<OrderSnapshot>> batch) throws SQLException {
        // Sorted ids: the function locks positions in its own order, this only keeps runs repeatable
        // Id đã sắp xếp: hàm tự khoá vị trí theo thứ tự riêng, việc này chỉ để mỗi lần chạy giống nhau
        TreeSet<String> ids = new TreeSet<>();
        for (Envelope<OrderSnapshot> e : batch) {
            ids.add(SalesKeys.orderId(e));
        }
        if (ids.isEmpty()) {
            return;
        }
        Array array = connection.createArrayOf("text", ids.toArray());
        try (PreparedStatement ps = connection.prepareStatement(SQL)) {
            ps.setArray(1, array);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
            }
        } finally {
            array.free();
        }
    }
}
