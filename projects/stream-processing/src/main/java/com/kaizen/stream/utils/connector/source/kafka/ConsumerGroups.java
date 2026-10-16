package com.kaizen.stream.utils.connector.source.kafka;

import com.kaizen.stream.config.Topic;
import java.util.regex.Pattern;

/**
 * Consumer group naming: kaizen.&lt;job&gt;.&lt;topic-short&gt;.
 * Quy ước tên group: kaizen.&lt;job&gt;.&lt;topic-ngắn&gt;.
 */
public final class ConsumerGroups {

    private static final Pattern JOB_NAME = Pattern.compile("[a-z][a-z0-9-]*");

    private ConsumerGroups() {
    }

    public static String of(String jobName, Topic topic) {
        return "kaizen." + checkJobName(jobName) + "." + topic.shortName();
    }

    /** Job names end up in group ids and Flink job names. / Tên job nằm trong group id và tên job Flink. */
    public static String checkJobName(String jobName) {
        if (jobName == null || !JOB_NAME.matcher(jobName).matches()) {
            throw new IllegalArgumentException("Job name must match [a-z][a-z0-9-]*: " + jobName);
        }
        return jobName;
    }
}
