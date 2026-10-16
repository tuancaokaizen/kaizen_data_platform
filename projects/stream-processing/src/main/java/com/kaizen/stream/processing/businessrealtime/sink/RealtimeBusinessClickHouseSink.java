package com.kaizen.stream.processing.businessrealtime.sink;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.model.realtime.RealtimeBusinessByOrderRow;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.job.OperatorNames;
import com.kaizen.stream.utils.connector.sink.clickhouse.ClickHouseSinkFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * RealtimeBusinessByOrderRow → report.realtime_business_by_order. Plain inserts: ReplacingMergeTree(Version) keeps the newest row per grain.
 * RealtimeBusinessByOrderRow → report.realtime_business_by_order. Chỉ insert: ReplacingMergeTree(Version) giữ dòng mới nhất của mỗi grain.
 */
public final class RealtimeBusinessClickHouseSink {

    private RealtimeBusinessClickHouseSink() {
    }

    /**
     * sink.clickhouse.realtime_business_by_order at the input parallelism, so rows of one order keep emit order.
     * Cùng parallelism với đầu vào để dòng của một đơn được insert đúng thứ tự phát.
     */
    public static void write(DataStream<RealtimeBusinessByOrderRow> rows, JobContext context) {
        StreamSettings settings = context.settings();
        context.sink(rows, sink(settings.clickhouse(), settings.businessRealtime()),
                OperatorNames.CLICKHOUSE, ClickHouseSinkFactory.tableName(RealtimeBusinessByOrderRow.TABLE));
    }

    static SinkFunction<RealtimeBusinessByOrderRow> sink(StreamSettings.ClickHouse clickhouse,
                                                         StreamSettings.BusinessRealtime businessRealtime) {
        return ClickHouseSinkFactory.insert(clickhouse, RealtimeBusinessByOrderRow.TABLE, RealtimeBusinessByOrderRow.COLUMNS,
                RealtimeBusinessClickHouseSink::bind, businessRealtime.getBatchSize(), businessRealtime.getBatchIntervalMs());
    }

    static void bind(PreparedStatement ps, RealtimeBusinessByOrderRow r) throws SQLException {
        int i = 1;
        ps.setString(i++, r.getReportDate().toString());
        ps.setString(i++, r.getOrderId());
        ps.setString(i++, r.getReturnId());
        ps.setString(i++, r.getDocumentType());
        ps.setString(i++, r.getShopCode());
        ps.setString(i++, r.getShopName());
        ps.setString(i++, r.getOrderStatus());
        ps.setLong(i++, r.getSalesAmount());
        ps.setLong(i++, r.getReturnAmount());
        ps.setLong(i++, r.getNetRevenue());
        ps.setLong(i++, r.getTotalReceipts());
        ps.setLong(i++, r.getTotalExpenditures());
        ps.setLong(i++, r.getNetCashFlow());
        ps.setString(i++, r.getPaymentMethodSummary());
        ps.setLong(i++, r.getTotalOrderAmount());
        ps.setLong(i++, r.getTotalReturnAmount());
        ps.setLong(i++, r.getReconciliationDiff());
        ps.setInt(i++, r.getPendingOrder());
        ps.setLong(i++, r.getVersion());
        ps.setTimestamp(i, new Timestamp(r.getUpdatedAtMs()));
    }
}
