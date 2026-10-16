package com.kaizen.stream.processing.job;

import com.kaizen.stream.config.SettingsLoader;
import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.utils.connector.source.kafka.ConsumerGroups;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Shared main(): load settings, build the job, submit as "Kaizen-&lt;group&gt;-&lt;name&gt;".
 * main() dùng chung: nạp cấu hình, dựng job, submit với tên "Kaizen-&lt;nhóm&gt;-&lt;tên&gt;".
 * State backend, checkpointing and S3 come from the cluster (flink-session.yaml), never from here.
 * State backend, checkpoint và S3 do cluster quy định (flink-session.yaml), không set ở đây.
 */
public final class JobRunner {

    private static final Logger LOG = LoggerFactory.getLogger(JobRunner.class);
    private static final Set<String> GROUPS = Set.of(StreamJob.PROCESSING, StreamJob.SIMULATION);

    private JobRunner() {
    }

    public static void run(StreamJob job, String[] args) throws Exception {
        StreamSettings settings = SettingsLoader.load(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(settings.flink().getParallelism());

        JobContext context = new JobContext(fullName(job), settings, env);
        job.build(context);

        String flinkJobName = flinkJobName(job);
        LOG.info("Submitting {} with {}", flinkJobName, settings.flink());
        env.execute(flinkJobName);
    }

    /** "&lt;group&gt;-&lt;name&gt;", validated. / "&lt;nhóm&gt;-&lt;tên&gt;", đã kiểm tra. */
    public static String fullName(StreamJob job) {
        // Unknown groups would scatter consumer groups and savepoints / Nhóm lạ làm lệch consumer group và savepoint
        if (!GROUPS.contains(job.group())) {
            throw new IllegalArgumentException("Job group must be one of " + GROUPS + ": " + job.group());
        }
        return ConsumerGroups.checkJobName(job.group() + "-" + ConsumerGroups.checkJobName(job.name()));
    }

    public static String flinkJobName(StreamJob job) {
        return "Kaizen-" + fullName(job);
    }
}
