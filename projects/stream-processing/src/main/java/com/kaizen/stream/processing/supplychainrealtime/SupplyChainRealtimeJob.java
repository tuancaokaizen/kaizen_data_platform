package com.kaizen.stream.processing.supplychainrealtime;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.inventory.StockMovementEvent;
import com.kaizen.stream.model.logistics.ShipmentEvent;
import com.kaizen.stream.model.realtime.RealtimeInventoryPositionRow;
import com.kaizen.stream.model.realtime.RealtimeShipmentStatusRow;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.job.JobRunner;
import com.kaizen.stream.processing.job.StreamJob;
import com.kaizen.stream.processing.supplychainrealtime.sink.RealtimeInventoryPositionClickHouseSink;
import com.kaizen.stream.processing.supplychainrealtime.sink.RealtimeShipmentStatusClickHouseSink;
import com.kaizen.stream.processing.supplychainrealtime.source.InventoryMovementsKafkaSource;
import com.kaizen.stream.processing.supplychainrealtime.source.LogisticsEventsKafkaSource;
import com.kaizen.stream.processing.supplychainrealtime.transform.InventoryRealtimeFunction;
import com.kaizen.stream.processing.supplychainrealtime.transform.ShipmentRealtimeFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * inventory-movements → report.realtime_inventory_position (stock per position now), logistics-events →
 * report.realtime_shipment_status (status per trip now).
 * inventory-movements → report.realtime_inventory_position (tồn hiện tại theo vị trí), logistics-events →
 * report.realtime_shipment_status (trạng thái hiện tại theo chuyến).
 *
 * <pre>
 * source.kafka.inventory-movements → transform.inventory-realtime → sink.clickhouse.realtime_inventory_position
 * source.kafka.logistics-events    → transform.shipment-realtime  → sink.clickhouse.realtime_shipment_status
 * </pre>
 */
public final class SupplyChainRealtimeJob implements StreamJob {

    public static void main(String[] args) throws Exception {
        JobRunner.run(new SupplyChainRealtimeJob(), args);
    }

    @Override
    public String group() {
        return PROCESSING;
    }

    @Override
    public String name() {
        return "supply-chain-realtime";
    }

    @Override
    public void build(JobContext context) {
        // Sources / Nguồn dữ liệu
        DataStream<Envelope<StockMovementEvent>> movements = InventoryMovementsKafkaSource.read(context);
        DataStream<Envelope<ShipmentEvent>> shipments = LogisticsEventsKafkaSource.read(context);

        // Transform / Biến đổi
        DataStream<RealtimeInventoryPositionRow> positions = InventoryRealtimeFunction.apply(movements, context);
        DataStream<RealtimeShipmentStatusRow> trips = ShipmentRealtimeFunction.apply(shipments, context);

        // Sinks / Đích ghi
        RealtimeInventoryPositionClickHouseSink.write(positions, context);
        RealtimeShipmentStatusClickHouseSink.write(trips, context);
    }
}
