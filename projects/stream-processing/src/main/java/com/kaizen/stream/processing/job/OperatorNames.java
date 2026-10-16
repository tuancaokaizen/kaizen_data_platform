package com.kaizen.stream.processing.job;

import com.kaizen.stream.config.Topic;

import java.util.regex.Pattern;

/**
 * Operator name = uid, so the Flink UI and savepoints show the same text and the role is visible:
 * "source.&lt;technology&gt;.&lt;name&gt;", "transform.&lt;name&gt;", "sink.&lt;technology&gt;.&lt;name&gt;".
 * Tên operator = uid, để Flink UI và savepoint cùng một tên và nhìn là biết vai trò nguồn / biến đổi / đích ghi.
 */
public final class OperatorNames {

    public static final String KAFKA = "kafka";
    public static final String POSTGRES = "postgres";
    public static final String CLICKHOUSE = "clickhouse";
    public static final String ELASTICSEARCH = "elasticsearch";
    public static final String GENERATOR = "generator";
    public static final String DEAD_LETTER = "deadletter";

    /** Every uid in every job must match. / Mọi uid của mọi job phải khớp mẫu này. */
    public static final Pattern PATTERN = Pattern.compile("(source|transform|sink)\\.[a-z0-9_.-]+");

    private OperatorNames() {
    }

    public static String source(String technology, String name) {
        return "source." + technology + "." + name;
    }

    /** Envelope parser that follows a source. / Bộ parse Envelope đứng sau một nguồn. */
    public static String parse(String sourceName) {
        return sourceName + ".parse";
    }

    public static String transform(String name) {
        return "transform." + name;
    }

    public static String sink(String technology, String name) {
        return "sink." + technology + "." + name;
    }

    /** Dead letters of events read from (or meant for) topic. / Dead letter của sự kiện thuộc topic. */
    public static String deadLetterSink(Topic topic) {
        return sink(DEAD_LETTER, topic.topicName());
    }
}
