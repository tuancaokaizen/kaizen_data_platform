package com.kaizen.flink.entrypoint;

import com.kaizen.flink.config.FlinkConfig;
import com.kaizen.flink.model.PaymentBundle;
import com.kaizen.flink.simulate.IdPool;
import com.kaizen.flink.simulate.PaymentSimulator;
import com.kaizen.flink.sink.KafkaSink;
import com.kaizen.flink.utils.JsonUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job 5/10 – PaymentProducerJob
 *
 * Bước 0: Seed bảng tham chiếu PaymentType và PaymentMethod (idempotent).
 * Topic: payment-bundles
 *
 * Phụ thuộc: OrderProducerJob.
 */
public class PaymentProducerJob {

    private static final Logger LOG = LoggerFactory.getLogger(PaymentProducerJob.class);

    public static void main(String[] args) throws Exception {
        final FlinkConfig config = new FlinkConfig("config.ini");

        final StreamExecutionEnvironment env = config.createEnv();
        env.enableCheckpointing(config.getLong("flink", "checkpoint_interval_ms", 10_000L));
        env.setParallelism(config.getInt("flink", "parallelism", 2));
        final long emitMs = config.getLong("flink", "emit_interval_ms", 1000L);

        @SuppressWarnings({"deprecation", "serial"})
        DataStream<String> bundleStream = env.addSource(new SourceFunction<String>() {
            volatile boolean running = true;
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                IdPool.init(config);
                int count = 0;
                long windowStart = System.currentTimeMillis();
                while (running) {
                    PaymentBundle bundle = PaymentSimulator.generate(config);
                    if (bundle != null) {
                        ctx.collect(JsonUtils.toJson(bundle));
                        count++;
                        if (count % 10 == 0) {
                            long elapsed = System.currentTimeMillis() - windowStart;
                            LOG.info("PaymentProducer: {} bundles in {}ms ({} bundle/s)",
                                    count, elapsed, count * 1000L / Math.max(1, elapsed));
                            count = 0;
                            windowStart = System.currentTimeMillis();
                        }
                    }
                    Thread.sleep(emitMs);
                }
            }
            @Override public void cancel() { running = false; }
        }).setParallelism(1).name("Src-PaymentBundle");

        bundleStream.sinkTo(KafkaSink.buildStringSink(config, "payment-bundles"))
                    .name("Kafka-PaymentBundles");

        LOG.info("Starting PaymentProducerJob (" + emitMs + "ms/payment)...");
        env.execute("Kaizen-PaymentProducerJob");
    }
}
