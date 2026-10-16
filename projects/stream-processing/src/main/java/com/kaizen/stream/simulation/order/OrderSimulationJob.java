package com.kaizen.stream.simulation.order;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.job.JobRunner;
import com.kaizen.stream.processing.job.StreamJob;
import com.kaizen.stream.simulation.order.sink.OrderBundlesKafkaSink;
import com.kaizen.stream.simulation.order.source.OrderGeneratorSource;
import com.kaizen.stream.simulation.order.transform.ConfigDemandModel;
import com.kaizen.stream.simulation.order.transform.OrderGenerator;
import com.kaizen.stream.simulation.order.transform.ReturnSchedulerFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Simulated live orders → order-bundles: orders arrive as a Poisson process over the shops' opening hours (a thin
 * trickle at night), a few come back 1–14 days later as ORDER_RETURNED. Master data is read from Postgres.
 * Đơn live giả lập → order-bundles: đơn đến theo quá trình Poisson trong giờ mở cửa (ban đêm rất ít), một số ít bị
 * trả lại sau 1–14 ngày dưới dạng ORDER_RETURNED. Master data đọc từ Postgres.
 *
 * <pre>
 * source.generator.order (p=1, lookup PostgresMasterDataSource)
 *   → transform.return-scheduler (keyBy orderId)
 *   → sink.kafka.order-bundles
 * </pre>
 */
public final class OrderSimulationJob implements StreamJob {

    public static void main(String[] args) throws Exception {
        JobRunner.run(new OrderSimulationJob(), args);
    }

    @Override
    public String group() {
        return SIMULATION;
    }

    @Override
    public String name() {
        return "order";
    }

    @Override
    public void build(JobContext context) {
        // The generator decides both new orders and their returns / Bộ sinh quyết định cả đơn mới và phiếu trả
        StreamSettings.Order profile = context.settings().order();
        OrderGenerator generator = new OrderGenerator(profile, new ConfigDemandModel(profile));

        // Sources / Nguồn dữ liệu
        DataStream<Envelope<OrderSnapshot>> placed = OrderGeneratorSource.read(generator, context);

        // Transform / Biến đổi
        DataStream<Envelope<OrderSnapshot>> orders = ReturnSchedulerFunction.apply(placed, generator, context);

        // Sinks / Đích ghi
        OrderBundlesKafkaSink.write(orders, context);
    }
}
