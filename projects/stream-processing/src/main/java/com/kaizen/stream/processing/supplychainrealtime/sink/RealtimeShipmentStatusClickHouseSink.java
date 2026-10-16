package com.kaizen.stream.processing.supplychainrealtime.sink;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.model.realtime.RealtimeShipmentStatusRow;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.job.OperatorNames;
import com.kaizen.stream.utils.connector.sink.clickhouse.ClickHouseSinkFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import static com.kaizen.stream.utils.connector.sink.clickhouse.ClickHouseValues.emptyIfNull;
import static com.kaizen.stream.utils.connector.sink.clickhouse.ClickHouseValues.setNullableTimestamp;

/**
 * RealtimeShipmentStatusRow → report.realtime_shipment_status (sink.clickhouse.realtime_shipment_status).
 * Plain inserts: ReplacingMergeTree(Version) keeps the newest row per grain.
 * RealtimeShipmentStatusRow → report.realtime_shipment_status. Chỉ insert: ReplacingMergeTree(Version) giữ dòng mới nhất.
 */
public final class RealtimeShipmentStatusClickHouseSink {

    private RealtimeShipmentStatusClickHouseSink() {
    }

    /** Same parallelism as the input, so rows of one trip keep emit order. / Cùng parallelism để giữ thứ tự theo chuyến. */
    public static void write(DataStream<RealtimeShipmentStatusRow> rows, JobContext context) {
        StreamSettings settings = context.settings();
        context.sink(rows, sink(settings.clickhouse(), settings.supplyChainRealtime()),
                OperatorNames.CLICKHOUSE, ClickHouseSinkFactory.tableName(RealtimeShipmentStatusRow.TABLE));
    }

    static SinkFunction<RealtimeShipmentStatusRow> sink(StreamSettings.ClickHouse clickhouse,
                                                        StreamSettings.SupplyChainRealtime settings) {
        return ClickHouseSinkFactory.insert(clickhouse, RealtimeShipmentStatusRow.TABLE, RealtimeShipmentStatusRow.COLUMNS,
                RealtimeShipmentStatusClickHouseSink::bind, settings.getBatchSize(), settings.getBatchIntervalMs());
    }

    static void bind(PreparedStatement statement, RealtimeShipmentStatusRow row) throws SQLException {
        int i = 1;
        statement.setString(i++, row.getShipmentDate().toString());
        statement.setString(i++, row.getShipmentId());
        statement.setString(i++, row.getOriginCode());
        statement.setString(i++, emptyIfNull(row.getCarrierCode()));
        statement.setString(i++, emptyIfNull(row.getVehicleId()));
        statement.setString(i++, row.getStatus());
        setNullableTimestamp(statement, i++, row.getPlannedDeparture());
        setNullableTimestamp(statement, i++, row.getActualDeparture());
        statement.setInt(i++, row.getStopCount());
        statement.setInt(i++, row.getStopsDelivered());
        statement.setInt(i++, row.getStopsPartial());
        statement.setInt(i++, row.getStopsFailed());
        statement.setInt(i++, row.getStopsDelayed());
        statement.setInt(i++, row.getLastStopSeq());
        statement.setString(i++, emptyIfNull(row.getLastStopShop()));
        statement.setInt(i++, row.getMaxDelayMinutes());
        statement.setDouble(i++, row.getLoadKg());
        statement.setDouble(i++, row.getLoadM3());
        statement.setLong(i++, row.getCostVnd());
        statement.setString(i++, row.getLastEventType());
        statement.setString(i++, emptyIfNull(row.getLastReasonCode()));
        statement.setTimestamp(i++, new Timestamp(row.getLastEventAt()));
        statement.setLong(i++, row.getVersion());
        statement.setTimestamp(i, new Timestamp(row.getUpdatedAtMs()));
    }
}
