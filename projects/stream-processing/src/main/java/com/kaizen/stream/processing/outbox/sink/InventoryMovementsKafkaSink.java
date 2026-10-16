package com.kaizen.stream.processing.outbox.sink;

import com.kaizen.stream.config.Topic;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.inventory.StockMovementEvent;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.outbox.transform.OutboxDecoderFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * Decoded stock movements → Kafka inventory-movements (sink.kafka.inventory-movements); rows that failed decoding
 * → inventory-movements.dlq (sink.deadletter.inventory-movements).
 * Biến động kho đã giải mã → Kafka inventory-movements; dòng giải mã lỗi → inventory-movements.dlq.
 */
public final class InventoryMovementsKafkaSink {

    private InventoryMovementsKafkaSink() {
    }

    public static void write(SingleOutputStreamOperator<Envelope<StockMovementEvent>> decoded, JobContext context) {
        context.sinkToKafka(decoded, Topic.INVENTORY_MOVEMENTS);
        context.sinkDeadLetters(decoded.getSideOutput(OutboxDecoderFunction.INVENTORY_DEAD_LETTERS),
                Topic.INVENTORY_MOVEMENTS);
    }
}
