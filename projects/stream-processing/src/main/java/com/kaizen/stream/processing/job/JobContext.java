package com.kaizen.stream.processing.job;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.config.Topic;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.utils.connector.sink.deadletter.DeadLetter;
import com.kaizen.stream.utils.connector.sink.deadletter.DeadLetterOutput;
import com.kaizen.stream.utils.connector.sink.kafka.KafkaSinkFactory;
import com.kaizen.stream.utils.connector.source.kafka.ConsumerGroups;
import com.kaizen.stream.utils.connector.source.kafka.KafkaSourceFactory;
import com.kaizen.stream.utils.connector.source.kafka.RawRecord;
import com.kaizen.stream.utils.event.EventFilter;
import com.kaizen.stream.utils.event.EventValidator;
import com.kaizen.stream.utils.serialization.EnvelopeParser;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Set;

/**
 * Everything a job needs, pre-wired with the approved Kafka rules (kafka_topic_model.md 8–9).
 * Every source, transform and sink is registered here, so names and uids always follow {@link OperatorNames}.
 * Mọi thứ job cần, đã gắn sẵn quy tắc Kafka đã duyệt (kafka_topic_model.md mục 8–9).
 * Mọi nguồn, biến đổi, đích ghi đều đăng ký qua đây nên tên và uid luôn theo {@link OperatorNames}.
 */
public final class JobContext {

    /** Partitions idle longer than this stop holding back the watermark. / Partition trống lâu hơn thì không giữ watermark. */
    static final Duration SOURCE_IDLENESS = Duration.ofMinutes(1);

    private final String jobName;
    private final StreamSettings settings;
    private final StreamExecutionEnvironment env;
    private final Set<Topic> sourcedTopics = EnumSet.noneOf(Topic.class);

    /** jobName is the full name "&lt;group&gt;-&lt;name&gt;" (see {@link JobRunner#fullName}). / jobName là tên đầy đủ. */
    public JobContext(String jobName, StreamSettings settings, StreamExecutionEnvironment env) {
        this.jobName = ConsumerGroups.checkJobName(jobName);
        this.settings = settings;
        this.env = env;
    }

    public String jobName() {
        return jobName;
    }

    public StreamSettings settings() {
        return settings;
    }

    public StreamExecutionEnvironment env() {
        return env;
    }

    /** Value of meta.source for events this job produces. / Giá trị meta.source cho sự kiện job này phát ra. */
    public String eventSource() {
        return "flink." + jobName;
    }

    // ---------------------------------------------------------------- Sources / Nguồn

    public <T> DataStream<Envelope<T>> source(Topic topic, Class<T> payload, EventFilter filter) {
        return source(topic, payload, filter, EventValidator.none());
    }

    /**
     * Kafka topic → validated Envelope&lt;T&gt;. Group, offsets, header filter and dead letters are applied here.
     * Topic Kafka → Envelope&lt;T&gt; đã kiểm tra. Group, offset, lọc header và dead letter được gắn tại đây.
     *
     * <pre>source.kafka.&lt;topic&gt; → source.kafka.&lt;topic&gt;.parse → (side output) sink.deadletter.&lt;topic&gt;</pre>
     */
    public <T> DataStream<Envelope<T>> source(Topic topic, Class<T> payload, EventFilter filter,
                                              EventValidator<T> validator) {
        // One source per topic per job keeps uids and group offsets unambiguous
        // Mỗi job chỉ 1 source cho 1 topic để uid và offset của group không mơ hồ
        if (!sourcedTopics.add(topic)) {
            throw new IllegalStateException("Topic " + topic.topicName() + " is already a source of " + jobName
                    + "; reuse the returned stream instead");
        }
        int parallelism = settings.flink().getSourceParallelism();
        KafkaSourceFactory.checkParallelism(topic, parallelism);
        String group = ConsumerGroups.of(jobName, topic);
        String sourceName = OperatorNames.source(OperatorNames.KAFKA, topic.topicName());

        SingleOutputStreamOperator<RawRecord> raw = env
                .fromSource(KafkaSourceFactory.raw(settings.kafka(), topic, group, filter),
                        WatermarkStrategy.noWatermarks(), sourceName)
                .uid(sourceName)
                .setParallelism(parallelism);

        String parseName = OperatorNames.parse(sourceName);
        SingleOutputStreamOperator<Envelope<T>> parsed = raw
                .process(new EnvelopeParser<>(payload, validator, group))
                .name(parseName)
                .uid(parseName)
                .setParallelism(parallelism);

        sinkDeadLetters(parsed.getSideOutput(DeadLetterOutput.TAG), topic, parallelism);
        return parsed;
    }

    /**
     * Non-Kafka source (generator, Postgres poller) named "source.&lt;technology&gt;.&lt;name&gt;".
     * Nguồn không phải Kafka (bộ sinh, poller Postgres), tên "source.&lt;công nghệ&gt;.&lt;tên&gt;".
     */
    public <T> DataStream<T> source(SourceFunction<T> function, String technology, String name, int parallelism) {
        String sourceName = OperatorNames.source(technology, name);
        return env.addSource(function, sourceName)
                .uid(sourceName)
                .setParallelism(parallelism);
    }

    /**
     * Event-time on meta.eventTime with bounded disorder and idle-partition handling.
     * Event-time theo meta.eventTime, chấp nhận lệch thứ tự có giới hạn và xử lý partition trống.
     */
    public <T> DataStream<Envelope<T>> withEventTime(DataStream<Envelope<T>> stream, Duration maxOutOfOrder) {
        WatermarkStrategy<Envelope<T>> strategy = WatermarkStrategy
                .<Envelope<T>>forBoundedOutOfOrderness(maxOutOfOrder)
                .withTimestampAssigner((envelope, ts) -> envelope.getMeta().getEventTime())
                .withIdleness(SOURCE_IDLENESS);
        return stream.assignTimestampsAndWatermarks(strategy);
    }

    // ---------------------------------------------------------------- Transforms / Biến đổi

    /** Names a transform operator "transform.&lt;name&gt;". / Đặt tên operator biến đổi "transform.&lt;tên&gt;". */
    public <T> SingleOutputStreamOperator<T> transform(SingleOutputStreamOperator<T> operator, String name,
                                                       int parallelism) {
        String transformName = OperatorNames.transform(name);
        return operator.name(transformName).uid(transformName).setParallelism(parallelism);
    }

    // ---------------------------------------------------------------- Sinks / Đích ghi
    // Sinks keep the parallelism of their input so they chain and keep per-key order
    // Sink giữ parallelism của đầu vào để được chain và giữ thứ tự theo key

    /** Envelope stream → Kafka topic, named "sink.kafka.&lt;topic&gt;". / Ghi Envelope vào topic Kafka. */
    public <T> void sinkToKafka(DataStream<Envelope<T>> stream, Topic topic) {
        String sinkName = OperatorNames.sink(OperatorNames.KAFKA, topic.topicName());
        stream.sinkTo(KafkaSinkFactory.<T>envelopes(settings.kafka(), topic))
                .name(sinkName).uid(sinkName)
                .setParallelism(stream.getParallelism());
    }

    /** Dead letters → "&lt;topic&gt;.dlq", named "sink.deadletter.&lt;topic&gt;". / Ghi dead letter của topic. */
    public void sinkDeadLetters(DataStream<DeadLetter> deadLetters, Topic topic) {
        sinkDeadLetters(deadLetters, topic, deadLetters.getParallelism());
    }

    private void sinkDeadLetters(DataStream<DeadLetter> deadLetters, Topic topic, int parallelism) {
        String sinkName = OperatorNames.deadLetterSink(topic);
        deadLetters.sinkTo(KafkaSinkFactory.deadLetters(settings.kafka(), topic))
                .name(sinkName).uid(sinkName)
                .setParallelism(parallelism);
    }

    /** SinkFunction-based sink (JDBC, ClickHouse), named "sink.&lt;technology&gt;.&lt;name&gt;". */
    public <T> void sink(DataStream<T> stream, SinkFunction<T> function, String technology, String name) {
        String sinkName = OperatorNames.sink(technology, name);
        stream.addSink(function)
                .name(sinkName).uid(sinkName)
                .setParallelism(stream.getParallelism());
    }

    /** Sink V2 (Elasticsearch), named "sink.&lt;technology&gt;.&lt;name&gt;". / Sink API mới (Elasticsearch). */
    public <T> void sink(DataStream<T> stream, Sink<T> sink, String technology, String name) {
        String sinkName = OperatorNames.sink(technology, name);
        stream.sinkTo(sink)
                .name(sinkName).uid(sinkName)
                .setParallelism(stream.getParallelism());
    }
}
