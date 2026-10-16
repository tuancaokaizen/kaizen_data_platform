package com.kaizen.stream.simulation.saleswriter.sink.step;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.simulation.saleswriter.model.SalesKeys;
import com.kaizen.stream.utils.connector.sink.postgres.PostgresAggregateSink;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.TreeSet;

/**
 * Marks the voucher series codes of a batch's orders USED, in the transaction that upserted the orders. The code
 * is taken from the stored order row and ModifiedDate becomes the order time, which the order generator reads
 * back as the moment the code stopped being available. Codes already USED are left alone, so replays change
 * nothing.
 * Đánh dấu USED cho mã series voucher của các đơn trong batch, cùng transaction đã upsert đơn. Mã lấy từ dòng đơn
 * đã lưu và ModifiedDate thành thời điểm đặt đơn, bộ sinh đơn đọc lại làm mốc mã hết khả dụng. Mã đã USED thì
 * giữ nguyên, nên đọc lại không đổi gì.
 */
public final class VoucherRedemption implements PostgresAggregateSink.BatchStep<OrderSnapshot> {

    private static final long serialVersionUID = 1L;

    static final String SQL = "UPDATE promotion.\"VoucherSeries\" s"
            + " SET \"Status\" = 'USED', \"ModifiedDate\" = o.\"TransactionDate\""
            + " FROM \"order\".\"Order\" o"
            + " WHERE o.\"Id\" = ANY(?) AND s.\"SeriesCode\" = o.\"VoucherSeriesCode\" AND s.\"Status\" = 'ACTIVE'";

    @Override
    public void apply(Connection connection, List<Envelope<OrderSnapshot>> batch) throws SQLException {
        // Only orders that carry a code; sorted so each run touches rows in the same order
        // Chỉ đơn có mã; sắp xếp để mỗi lần chạy chạm các dòng theo cùng thứ tự
        TreeSet<String> ids = new TreeSet<>();
        for (Envelope<OrderSnapshot> e : batch) {
            if (e.getData().getOrder().getVoucherSeriesCode() != null) {
                ids.add(SalesKeys.orderId(e));
            }
        }
        if (ids.isEmpty()) {
            return;
        }
        Array array = connection.createArrayOf("text", ids.toArray());
        try (PreparedStatement ps = connection.prepareStatement(SQL)) {
            ps.setArray(1, array);
            ps.executeUpdate();
        } finally {
            array.free();
        }
    }
}
