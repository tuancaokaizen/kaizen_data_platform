package com.kaizen.stream.testing;

import com.kaizen.stream.config.RawSettings;
import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.job.JobRunner;
import com.kaizen.stream.processing.job.StreamJob;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.streaming.api.transformations.UnionTransformation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Builds a job graph without a cluster, so tests can check operator names, uids and parallelism.
 * Dựng đồ thị job không cần cluster để test kiểm tra tên operator, uid và parallelism.
 */
public final class JobGraph {

    private JobGraph() {
    }

    /** Settings with every section a job may read; parallelism 3. / Cấu hình đủ mọi mục job có thể đọc. */
    public static StreamSettings settings(Map<String, String> extra) {
        Map<String, String> values = new HashMap<>();
        values.put("kafka.bootstrap_servers", "kafka:9092");
        values.put("flink.parallelism", "3");
        values.put("flink.source_parallelism", "3");
        values.put("postgres.host", "postgres");
        values.put("postgres.database", "information_system");
        values.put("postgres.username", "user");
        values.put("postgres.password", "password");
        values.put("clickhouse.host", "clickhouse");
        values.put("clickhouse.username", "user");
        values.put("clickhouse.password", "password");
        values.put("elasticsearch.hosts", "http://elasticsearch:9200");
        values.putAll(extra);
        return new StreamSettings(new RawSettings(values));
    }

    /** Every transformation of the job (sources included). / Mọi transformation của job, kể cả nguồn. */
    public static List<Transformation<?>> transformations(StreamJob job) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        job.build(new JobContext(JobRunner.fullName(job), settings(Map.of()), env));
        return env.getTransformations().stream()
                .flatMap(t -> t.getTransitivePredecessors().stream())
                .distinct()
                .collect(Collectors.toList());
    }

    /** Real operators only: partitioning, side outputs and unions are virtual. / Chỉ operator thật. */
    public static List<Transformation<?>> operators(StreamJob job) {
        return transformations(job).stream()
                .filter(t -> !(t instanceof PartitionTransformation)
                        && !(t instanceof SideOutputTransformation)
                        && !(t instanceof UnionTransformation))
                .collect(Collectors.toList());
    }

    /** uid → parallelism, sorted. / uid → parallelism, đã sắp xếp. */
    public static Map<String, Integer> parallelismByUid(StreamJob job) {
        Map<String, Integer> result = new TreeMap<>();
        for (Transformation<?> operator : operators(job)) {
            // A missing uid shows up as "null:<name>" in the assertion message / Thiếu uid sẽ hiện "null:<tên>"
            String uid = operator.getUid() == null ? "null:" + operator.getName() : operator.getUid();
            result.put(uid, operator.getParallelism());
        }
        return result;
    }
}
