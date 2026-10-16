package com.kaizen.stream.processing.searchindex;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.job.JobRunner;
import com.kaizen.stream.processing.job.StreamJob;
import com.kaizen.stream.processing.searchindex.sink.OrderBundlesElasticsearchSink;
import com.kaizen.stream.processing.searchindex.sink.PaymentBundlesElasticsearchSink;
import com.kaizen.stream.processing.searchindex.source.OrderBundlesKafkaSource;
import com.kaizen.stream.processing.searchindex.source.PaymentBundlesKafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * order-bundles / payment-bundles → Elasticsearch (one document per order / payment) for the backend search.
 * No keyBy: sinks run at source parallelism, so events of one order stay in Kafka partition order.
 * order-bundles / payment-bundles → Elasticsearch (mỗi đơn / phiếu một document) cho tìm kiếm của backend.
 * Không keyBy: sink chạy cùng parallelism với source nên sự kiện của một đơn giữ đúng thứ tự partition.
 *
 * <pre>
 * source.kafka.order-bundles   → sink.elasticsearch.order-bundles
 * source.kafka.payment-bundles → sink.elasticsearch.payment-bundles
 * </pre>
 */
public final class SearchIndexJob implements StreamJob {

    public static void main(String[] args) throws Exception {
        JobRunner.run(new SearchIndexJob(), args);
    }

    @Override
    public String group() {
        return PROCESSING;
    }

    @Override
    public String name() {
        return "search-index";
    }

    @Override
    public void build(JobContext context) {
        // Sources / Nguồn dữ liệu
        DataStream<Envelope<OrderSnapshot>> orders = OrderBundlesKafkaSource.read(context);
        DataStream<Envelope<PaymentSnapshot>> payments = PaymentBundlesKafkaSource.read(context);

        // Sinks / Đích ghi
        OrderBundlesElasticsearchSink.write(orders, context);
        PaymentBundlesElasticsearchSink.write(payments, context);
    }
}
