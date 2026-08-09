package com.kaizen.flink.entrypoint;

import com.kaizen.flink.config.FlinkConfig;
import com.kaizen.flink.model.OrderBundle;
import com.kaizen.flink.simulate.IdPool;
import com.kaizen.flink.simulate.OrderSimulator;
import com.kaizen.flink.sink.KafkaSink;
import com.kaizen.flink.utils.JsonUtils;
import com.kaizen.flink.utils.KafkaClientProps;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job 4/10 – OrderProducerJob
 * <p>
 * Topic: order-bundles (toàn bộ OrderBundle JSON)
 * <p>
 * Business rules:
 * - 100ms/emit (setParallelism=1) → 10 orders/s → 100 records/10s
 * - Voucher: sau 10 đơn → cấp voucher, tự áp dụng ở lần mua tiếp theo
 * - Return: 25% xác suất, trong vòng 7 ngày, partial hoặc full
 * <p>
 * Phụ thuộc: Master Data được sinh từ Airflow và lưu vào Postgres.
 * Consumer jobs (OrderPgConsumerJob) đảm bảo FK ordering khi insert.
 */
public class OrderProducerJob {

    private static final Logger LOG = LoggerFactory.getLogger(OrderProducerJob.class);

    public static void main(String[] args) throws Exception {
        final FlinkConfig config = new FlinkConfig("config.ini");
        final StreamExecutionEnvironment env = config.createEnv();

        env.enableCheckpointing(config.getLong("flink", "checkpoint_interval_ms", 10_000L));
        final int kafkaParallelism = KafkaClientProps.kafkaParallelism(config);
        final int sourceParallelism = config.getInt("flink", "source_parallelism", 1);
        env.setParallelism(kafkaParallelism);
        // Đọc cấu hình emitMs từ config.ini (ví dụ: 1000ms = 1 order/s)
        final long emitMs = config.getLong("flink", "emit_interval_ms", 1000L);

        // Source parallelism = 1 để đảm bảo đúng throughput (không scale)
        @SuppressWarnings({"deprecation", "serial"})
        DataStream<String> bundleStream = env.addSource(new SourceFunction<String>() {
            volatile boolean running = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                IdPool.init(config);
                int count = 0;
                long windowStart = System.currentTimeMillis();
                while (running) {
                    OrderBundle bundle = OrderSimulator.generate();
                    ctx.collect(JsonUtils.toJson(bundle));
                    count++;
                    if (count % 10 == 0) {
                        long elapsed = System.currentTimeMillis() - windowStart;
                        LOG.info("OrderProducer: {} bundles in {}ms ({} bundle/s)",
                                count, elapsed, count * 1000L / Math.max(1, elapsed));
                        count = 0;
                        windowStart = System.currentTimeMillis();
                    }
                    Thread.sleep(emitMs);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        }).setParallelism(sourceParallelism).name("Src-OrderBundle");

        bundleStream.sinkTo(KafkaSink.buildStringSink(config, "order-bundles"))
                .setParallelism(kafkaParallelism)
                .name("Kafka-OrderBundles");

        LOG.info("Starting OrderProducerJob (" + emitMs + "ms/order)...");
        env.execute("Kaizen-OrderProducerJob");
    }
}
