package com.kaizen.stream.processing.supplychainrealtime.sink;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.model.realtime.RealtimeInventoryPositionRow;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.job.OperatorNames;
import com.kaizen.stream.utils.connector.sink.clickhouse.ClickHouseSinkFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import static com.kaizen.stream.utils.connector.sink.clickhouse.ClickHouseValues.setNullableTimestamp;

/**
 * RealtimeInventoryPositionRow → report.realtime_inventory_position (sink.clickhouse.realtime_inventory_position).
 * Plain inserts: ReplacingMergeTree(Version) keeps the newest row per grain.
 * RealtimeInventoryPositionRow → report.realtime_inventory_position. Chỉ insert: ReplacingMergeTree(Version) giữ dòng mới nhất.
 */
public final class RealtimeInventoryPositionClickHouseSink {

    private RealtimeInventoryPositionClickHouseSink() {
    }

    /** Same parallelism as the input, so rows of one key keep emit order. / Cùng parallelism để giữ thứ tự theo key. */
    public static void write(DataStream<RealtimeInventoryPositionRow> rows, JobContext context) {
        StreamSettings settings = context.settings();
        context.sink(rows, sink(settings.clickhouse(), settings.supplyChainRealtime()),
                OperatorNames.CLICKHOUSE, ClickHouseSinkFactory.tableName(RealtimeInventoryPositionRow.TABLE));
    }

    static SinkFunction<RealtimeInventoryPositionRow> sink(StreamSettings.ClickHouse clickhouse,
                                                           StreamSettings.SupplyChainRealtime settings) {
        return ClickHouseSinkFactory.insert(clickhouse, RealtimeInventoryPositionRow.TABLE, RealtimeInventoryPositionRow.COLUMNS,
                RealtimeInventoryPositionClickHouseSink::bind, settings.getBatchSize(), settings.getBatchIntervalMs());
    }

    static void bind(PreparedStatement statement, RealtimeInventoryPositionRow row) throws SQLException {
        int i = 1;
        statement.setString(i++, row.getLocationType());
        statement.setString(i++, row.getLocationCode());
        statement.setString(i++, row.getProductId());
        statement.setLong(i++, row.getOnHandQty());
        statement.setString(i++, row.getBusinessDate().toString());
        statement.setLong(i++, row.getSoldQty());
        statement.setLong(i++, row.getReturnedQty());
        statement.setLong(i++, row.getReceivedQty());
        statement.setLong(i++, row.getTransferInQty());
        statement.setLong(i++, row.getTransferOutQty());
        statement.setLong(i++, row.getExpiredQty());
        statement.setLong(i++, row.getDamagedQty());
        statement.setLong(i++, row.getAdjustedQty());
        statement.setLong(i++, row.getOversellQty());
        statement.setInt(i++, row.getMovementCount());
        statement.setInt(i++, row.isStockout() ? 1 : 0);
        setNullableTimestamp(statement, i++, row.getStockoutSince());
        statement.setString(i++, row.getLastMovementType());
        statement.setTimestamp(i++, new Timestamp(row.getLastMovementAt()));
        statement.setLong(i++, row.getVersion());
        statement.setTimestamp(i, new Timestamp(row.getUpdatedAtMs()));
    }
}
