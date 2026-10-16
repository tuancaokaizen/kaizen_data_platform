package com.kaizen.stream.simulation.saleswriter;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.job.JobRunner;
import com.kaizen.stream.processing.job.StreamJob;
import com.kaizen.stream.simulation.saleswriter.sink.OrderPostgresSink;
import com.kaizen.stream.simulation.saleswriter.sink.PaymentPostgresSink;
import com.kaizen.stream.simulation.saleswriter.source.OrderBundlesKafkaSource;
import com.kaizen.stream.simulation.saleswriter.source.PaymentBundlesKafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Simulated sales → Postgres OLTP tables, playing the role of the store system: orders, payments, stock postings
 * (StockPosting) and voucher redemptions (VoucherRedemption) commit in one transaction per batch.
 * No keyBy: sinks run at source parallelism, so events of one order stay in Kafka partition order.
 * Bán hàng giả lập → bảng OLTP Postgres, đóng vai hệ thống cửa hàng: đơn, phiếu, ghi sổ kho (StockPosting) và đánh dấu
 * voucher (VoucherRedemption) commit chung một transaction mỗi batch.
 * Không keyBy: sink chạy cùng parallelism với nguồn nên sự kiện của một đơn giữ đúng thứ tự partition.
 *
 * <pre>
 * source.kafka.order-bundles   → sink.postgres.order
 * source.kafka.payment-bundles → sink.postgres.payment
 * </pre>
 */
public final class SalesWriterJob implements StreamJob {

    public static void main(String[] args) throws Exception {
        JobRunner.run(new SalesWriterJob(), args);
    }

    @Override
    public String group() {
        return SIMULATION;
    }

    @Override
    public String name() {
        return "sales-writer";
    }

    @Override
    public void build(JobContext context) {
        // Sources / Nguồn dữ liệu
        DataStream<Envelope<OrderSnapshot>> orders = OrderBundlesKafkaSource.read(context);
        DataStream<Envelope<PaymentSnapshot>> payments = PaymentBundlesKafkaSource.read(context);

        // Sinks / Đích ghi
        OrderPostgresSink.write(orders, context);
        PaymentPostgresSink.write(payments, context);
    }
}
