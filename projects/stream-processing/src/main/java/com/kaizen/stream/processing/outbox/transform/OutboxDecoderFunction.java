package com.kaizen.stream.processing.outbox.transform;

import com.kaizen.stream.config.Topic;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.inventory.MovementType;
import com.kaizen.stream.model.inventory.StockMovementEvent;
import com.kaizen.stream.model.logistics.ShipmentEvent;
import com.kaizen.stream.model.logistics.ShipmentEventType;
import com.kaizen.stream.model.outbox.OutboxRow;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.rule.InventoryRules;
import com.kaizen.stream.processing.rule.LogisticsRules;
import com.kaizen.stream.utils.connector.sink.deadletter.DeadLetter;
import com.kaizen.stream.utils.connector.source.kafka.RawRecord;
import com.kaizen.stream.utils.serialization.EventHeaders;
import com.kaizen.stream.utils.serialization.InvalidEventException;
import com.kaizen.stream.utils.serialization.Json;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

/**
 * OutboxRow → Envelope on the main output (inventory-movements) or the LOGISTICS_EVENTS side output; rows breaking
 * the payload rules go to the dead letter tag of their topic, so a bad row never blocks the relay.
 * OutboxRow → Envelope ở output chính (inventory-movements) hoặc side output LOGISTICS_EVENTS; dòng vi phạm quy tắc
 * payload sang tag dead letter của topic đó, để một dòng lỗi không chặn relay.
 */
public class OutboxDecoderFunction extends ProcessFunction<OutboxRow, Envelope<StockMovementEvent>>
        implements ResultTypeQueryable<Envelope<StockMovementEvent>> {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static final OutputTag<Envelope<ShipmentEvent>> LOGISTICS_EVENTS = new OutputTag<>("logistics-events",
            (TypeInformation<Envelope<ShipmentEvent>>) (TypeInformation) TypeInformation.of(Envelope.class));
    public static final OutputTag<DeadLetter> INVENTORY_DEAD_LETTERS =
            new OutputTag<>("inventory-dead-letters", TypeInformation.of(DeadLetter.class));
    public static final OutputTag<DeadLetter> LOGISTICS_DEAD_LETTERS =
            new OutputTag<>("logistics-dead-letters", TypeInformation.of(DeadLetter.class));

    private final String source;
    /** Shown as the rejecting consumer in dead letter headers. / Hiện là consumer loại message trong header dead letter. */
    private final String consumer;

    public OutboxDecoderFunction(String source, String consumer) {
        this.source = source;
        this.consumer = consumer;
    }

    /**
     * transform.outbox-decoder at parallelism 1, so each key keeps the Seq order of its rows.
     * transform.outbox-decoder chạy parallelism 1 để mỗi key giữ đúng thứ tự Seq của các dòng.
     */
    public static SingleOutputStreamOperator<Envelope<StockMovementEvent>> apply(DataStream<OutboxRow> rows,
                                                                                JobContext context) {
        // Same "kaizen.<job>" prefix as Kafka consumer groups / Cùng tiền tố "kaizen.<job>" với consumer group Kafka
        OutboxDecoderFunction decoder = new OutboxDecoderFunction(context.eventSource(), "kaizen." + context.jobName());
        return context.transform(rows.process(decoder), "outbox-decoder", 1);
    }

    @Override
    public void processElement(OutboxRow row, Context ctx, Collector<Envelope<StockMovementEvent>> out) {
        boolean inventory = Topic.INVENTORY_MOVEMENTS.topicName().equals(row.getTopic());
        try {
            if (inventory) {
                out.collect(inventory(row));
            } else if (Topic.LOGISTICS_EVENTS.topicName().equals(row.getTopic())) {
                ctx.output(LOGISTICS_EVENTS, logistics(row));
            } else {
                // ck_outbox_topic makes this unreachable unless the DDL changes / Chỉ xảy ra khi DDL đổi
                throw new InvalidEventException("Outbox row " + row.getSeq() + " has unknown topic " + row.getTopic());
            }
        } catch (RuntimeException e) {
            // Decoding depends only on the row, so any failure is a data error / Giải mã chỉ phụ thuộc dòng nên lỗi là lỗi dữ liệu
            ctx.output(inventory ? INVENTORY_DEAD_LETTERS : LOGISTICS_DEAD_LETTERS,
                    DeadLetter.of(raw(row), consumer, e, System.currentTimeMillis()));
        }
    }

    Envelope<StockMovementEvent> inventory(OutboxRow row) {
        MovementType type = MovementType.parse(row.getEventType());
        StockMovementEvent data = parse(row, StockMovementEvent.class);
        if (type == null) {
            throw new InvalidEventException("unknown movement type " + row.getEventType());
        }
        check(InventoryRules.violations(row.getEventType(), row.getVersion(), row.getAggregateId(), data));
        return Envelope.of(type, source, row.getAggregateId(), row.getVersion(), row.getEventTime(), data);
    }

    Envelope<ShipmentEvent> logistics(OutboxRow row) {
        ShipmentEventType type = ShipmentEventType.parse(row.getEventType());
        ShipmentEvent data = parse(row, ShipmentEvent.class);
        if (type == null) {
            throw new InvalidEventException("unknown shipment event type " + row.getEventType());
        }
        check(LogisticsRules.violations(row.getEventType(), row.getVersion(), row.getAggregateId(), data));
        return Envelope.of(type, source, row.getAggregateId(), row.getVersion(), row.getEventTime(), data);
    }

    private static <T> T parse(OutboxRow row, Class<T> type) {
        try {
            return Json.mapper().readValue(row.getPayload(), type);
        } catch (IOException e) {
            throw new InvalidEventException("Malformed payload of outbox row " + row.getSeq() + ": " + e.getMessage(), e);
        }
    }

    private static void check(List<String> violations) {
        if (!violations.isEmpty()) {
            throw new InvalidEventException(String.join("; ", violations));
        }
    }

    /** The row as a record, so the DLQ keeps key and payload; offset = outbox Seq. / offset = Seq của outbox. */
    static RawRecord raw(OutboxRow row) {
        return new RawRecord(row.getTopic(), -1, row.getSeq(), row.getEventTime(),
                row.getAggregateId() == null ? null : row.getAggregateId().getBytes(StandardCharsets.UTF_8),
                row.getPayload() == null ? null : row.getPayload().getBytes(StandardCharsets.UTF_8),
                Collections.singletonMap(EventHeaders.EVENT_TYPE, String.valueOf(row.getEventType())));
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<Envelope<StockMovementEvent>> getProducedType() {
        return TypeInformation.of((Class<Envelope<StockMovementEvent>>) (Class<?>) Envelope.class);
    }
}
