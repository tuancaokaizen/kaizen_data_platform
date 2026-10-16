package com.kaizen.stream.processing.outbox;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.inventory.StockMovementEvent;
import com.kaizen.stream.model.outbox.OutboxRow;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.job.JobRunner;
import com.kaizen.stream.processing.job.StreamJob;
import com.kaizen.stream.processing.outbox.sink.InventoryMovementsKafkaSink;
import com.kaizen.stream.processing.outbox.sink.LogisticsEventsKafkaSink;
import com.kaizen.stream.processing.outbox.source.OutboxPostgresSource;
import com.kaizen.stream.processing.outbox.transform.OutboxDecoderFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * inventory."Outbox" (Postgres, written by inventory.apply_movement and the day-close job) → Kafka
 * inventory-movements and logistics-events. Everything runs at parallelism 1 so each key keeps the Seq order.
 * inventory."Outbox" (Postgres, do inventory.apply_movement và job chốt sổ ghi) → Kafka inventory-movements và
 * logistics-events. Mọi operator chạy parallelism 1 để mỗi key giữ đúng thứ tự Seq.
 *
 * <pre>
 * source.postgres.outbox → transform.outbox-decoder
 *   → sink.kafka.inventory-movements + sink.deadletter.inventory-movements
 *   → sink.kafka.logistics-events    + sink.deadletter.logistics-events
 * </pre>
 */
public final class OutboxRelayJob implements StreamJob {

    public static void main(String[] args) throws Exception {
        JobRunner.run(new OutboxRelayJob(), args);
    }

    @Override
    public String group() {
        return PROCESSING;
    }

    @Override
    public String name() {
        return "outbox-relay";
    }

    @Override
    public void build(JobContext context) {
        // Sources / Nguồn dữ liệu
        DataStream<OutboxRow> rows = OutboxPostgresSource.read(context);

        // Transform / Biến đổi
        SingleOutputStreamOperator<Envelope<StockMovementEvent>> decoded = OutboxDecoderFunction.apply(rows, context);

        // Sinks / Đích ghi
        InventoryMovementsKafkaSink.write(decoded, context);
        LogisticsEventsKafkaSink.write(decoded, context);
    }
}
