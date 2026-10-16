package com.kaizen.stream.processing.businessrealtime;

import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.model.realtime.RealtimeBusinessByOrderRow;
import com.kaizen.stream.processing.businessrealtime.sink.RealtimeBusinessClickHouseSink;
import com.kaizen.stream.processing.businessrealtime.source.OrderBundlesKafkaSource;
import com.kaizen.stream.processing.businessrealtime.source.PaymentBundlesKafkaSource;
import com.kaizen.stream.processing.businessrealtime.transform.BusinessRealtimeJoinFunction;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.job.JobRunner;
import com.kaizen.stream.processing.job.StreamJob;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * order-bundles + successful payments → ClickHouse report.realtime_business_by_order (realtime business by order).
 * order-bundles + phiếu thành công → ClickHouse report.realtime_business_by_order (tình hình kinh doanh realtime theo đơn).
 *
 * <pre>
 * source.kafka.order-bundles   ┐
 * source.kafka.payment-bundles ┴→ transform.business-realtime-join (lookup ShopNames)
 *   → sink.clickhouse.realtime_business_by_order
 * </pre>
 */
public final class BusinessRealtimeJob implements StreamJob {

    public static void main(String[] args) throws Exception {
        JobRunner.run(new BusinessRealtimeJob(), args);
    }

    @Override
    public String group() {
        return PROCESSING;
    }

    @Override
    public String name() {
        return "business-realtime";
    }

    @Override
    public void build(JobContext context) {
        // Sources / Nguồn dữ liệu
        DataStream<Envelope<OrderSnapshot>> orders = OrderBundlesKafkaSource.read(context);
        DataStream<Envelope<PaymentSnapshot>> payments = PaymentBundlesKafkaSource.read(context);

        // Transform / Biến đổi
        DataStream<RealtimeBusinessByOrderRow> rows = BusinessRealtimeJoinFunction.apply(orders, payments, context);

        // Sinks / Đích ghi
        RealtimeBusinessClickHouseSink.write(rows, context);
    }
}
