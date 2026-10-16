package com.kaizen.stream.processing.outbox.sink;

import com.kaizen.stream.config.Topic;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.inventory.StockMovementEvent;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.outbox.transform.OutboxDecoderFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * Decoded shipment events (decoder side output) → Kafka logistics-events (sink.kafka.logistics-events); rows that
 * failed decoding → logistics-events.dlq (sink.deadletter.logistics-events).
 * Sự kiện chuyến xe đã giải mã (side output) → Kafka logistics-events; dòng giải mã lỗi → logistics-events.dlq.
 */
public final class LogisticsEventsKafkaSink {

    private LogisticsEventsKafkaSink() {
    }

    public static void write(SingleOutputStreamOperator<Envelope<StockMovementEvent>> decoded, JobContext context) {
        context.sinkToKafka(decoded.getSideOutput(OutboxDecoderFunction.LOGISTICS_EVENTS), Topic.LOGISTICS_EVENTS);
        context.sinkDeadLetters(decoded.getSideOutput(OutboxDecoderFunction.LOGISTICS_DEAD_LETTERS),
                Topic.LOGISTICS_EVENTS);
    }
}
