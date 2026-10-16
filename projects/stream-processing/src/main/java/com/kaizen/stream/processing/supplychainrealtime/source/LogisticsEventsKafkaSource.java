package com.kaizen.stream.processing.supplychainrealtime.source;

import com.kaizen.stream.config.Topic;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.logistics.ShipmentEvent;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.rule.LogisticsRules;
import com.kaizen.stream.utils.event.EventFilter;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Kafka logistics-events (source.kafka.logistics-events): every shipment event; events breaking LogisticsRules go to
 * dead letter.
 * Kafka logistics-events: mọi sự kiện chuyến xe; sự kiện vi phạm LogisticsRules sang dead letter.
 */
public final class LogisticsEventsKafkaSource {

    private LogisticsEventsKafkaSource() {
    }

    public static DataStream<Envelope<ShipmentEvent>> read(JobContext context) {
        return context.source(Topic.LOGISTICS_EVENTS, ShipmentEvent.class, EventFilter.all(),
                (meta, event) -> LogisticsRules.violations(meta.getEventType(), meta.getVersion(),
                        meta.getAggregateId(), event));
    }
}
