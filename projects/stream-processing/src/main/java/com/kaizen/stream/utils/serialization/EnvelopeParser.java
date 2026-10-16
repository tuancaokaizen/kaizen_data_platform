package com.kaizen.stream.utils.serialization;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.utils.connector.sink.deadletter.DeadLetter;
import com.kaizen.stream.utils.connector.sink.deadletter.DeadLetterOutput;
import com.kaizen.stream.utils.connector.source.kafka.RawRecord;
import com.kaizen.stream.utils.event.EventValidator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * RawRecord → Envelope&lt;T&gt;; bad records go to the DeadLetterOutput side output instead of failing the job.
 * RawRecord → Envelope&lt;T&gt;; record lỗi ra side output DeadLetterOutput thay vì làm job lỗi.
 */
public class EnvelopeParser<T> extends ProcessFunction<RawRecord, Envelope<T>>
        implements ResultTypeQueryable<Envelope<T>> {

    private static final long serialVersionUID = 1L;

    private final Class<T> payload;
    private final EventValidator<T> validator;
    private final String consumer;

    public EnvelopeParser(Class<T> payload, EventValidator<T> validator, String consumer) {
        this.payload = payload;
        this.validator = validator;
        this.consumer = consumer;
    }

    @Override
    public void processElement(RawRecord record, Context ctx, Collector<Envelope<T>> out) {
        Envelope<T> envelope;
        try {
            envelope = EnvelopeCodec.decode(record, payload);
            List<String> violations = validator.violations(envelope.getMeta(), envelope.getData());
            if (!violations.isEmpty()) {
                throw new InvalidEventException(String.join("; ", violations));
            }
        } catch (RuntimeException e) {
            // Validators are pure functions of the message, so any failure here is a data error
            // Validator chỉ phụ thuộc message nên mọi lỗi ở đây là lỗi dữ liệu
            ctx.output(DeadLetterOutput.TAG, DeadLetter.of(record, consumer, e, System.currentTimeMillis()));
            return;
        }
        out.collect(envelope);
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<Envelope<T>> getProducedType() {
        return TypeInformation.of((Class<Envelope<T>>) (Class<?>) Envelope.class);
    }
}
