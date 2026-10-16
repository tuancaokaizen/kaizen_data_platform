package com.kaizen.stream.processing.supplychainrealtime.source;

import com.kaizen.stream.config.Topic;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.inventory.StockMovementEvent;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.rule.InventoryRules;
import com.kaizen.stream.utils.event.EventFilter;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Kafka inventory-movements (source.kafka.inventory-movements): every movement; events breaking InventoryRules go to
 * dead letter.
 * Kafka inventory-movements: mọi biến động kho; sự kiện vi phạm InventoryRules sang dead letter.
 */
public final class InventoryMovementsKafkaSource {

    private InventoryMovementsKafkaSource() {
    }

    public static DataStream<Envelope<StockMovementEvent>> read(JobContext context) {
        return context.source(Topic.INVENTORY_MOVEMENTS, StockMovementEvent.class, EventFilter.all(),
                (meta, event) -> InventoryRules.violations(meta.getEventType(), meta.getVersion(),
                        meta.getAggregateId(), event));
    }
}
