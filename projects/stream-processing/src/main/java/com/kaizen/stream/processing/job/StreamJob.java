package com.kaizen.stream.processing.job;

/**
 * One Flink job = one StreamJob; build() wires three blocks in order: sources → transforms → sinks.
 * Một job Flink = một StreamJob; build() nối 3 khối theo thứ tự: nguồn → biến đổi → đích ghi.
 *
 * <p>Full name = "&lt;group&gt;-&lt;name&gt;", e.g. "processing-business-realtime": Flink job "Kaizen-processing-business-realtime",
 * consumer groups "kaizen.processing-business-realtime.&lt;topic&gt;", meta.source "flink.processing-business-realtime".
 * Tên đầy đủ = "&lt;nhóm&gt;-&lt;tên&gt;", dùng cho tên job Flink, consumer group và meta.source.
 */
public interface StreamJob {

    /** Jobs that implement business processing. / Job xử lý nghiệp vụ. */
    String PROCESSING = "processing";

    /** Jobs that generate simulated data. / Job sinh dữ liệu giả lập. */
    String SIMULATION = "simulation";

    /** PROCESSING or SIMULATION; must match the package (processing/… or simulation/…). / Phải khớp package. */
    String group();

    /** Short name, unique inside the group, returned as a string literal (deploy_stream.sh reads it). / Tên ngắn, là chuỗi hằng. */
    String name();

    void build(JobContext context);
}
