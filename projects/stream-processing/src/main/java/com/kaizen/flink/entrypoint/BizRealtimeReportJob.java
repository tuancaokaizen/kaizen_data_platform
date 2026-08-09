package com.kaizen.flink.entrypoint;

import com.kaizen.flink.config.FlinkConfig;
import com.kaizen.flink.model.BizRtOrderRow;
import com.kaizen.flink.model.OrderBundle;
import com.kaizen.flink.model.OrderPayment;
import com.kaizen.flink.model.PaymentBundle;
import com.kaizen.flink.model.ReturnPayment;
import com.kaizen.flink.process.BizRtOrderCoProcessFunction;
import com.kaizen.flink.source.KafkaSourceBuilder;
import com.kaizen.flink.utils.JsonUtils;
import com.kaizen.flink.utils.KafkaClientProps;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Realtime business-by-order report: order + payment → ClickHouse report.biz_rt_order.
 * Báo cáo tình hình KD theo đơn realtime: order + payment → ClickHouse.
 */
public class BizRealtimeReportJob {

    private static final Logger LOG = LoggerFactory.getLogger(BizRealtimeReportJob.class);
    private static final String ORDER_GROUP = "kaizen-biz-rt-order-group";
    private static final String PAYMENT_GROUP = "kaizen-biz-rt-payment-group";

    private static final String INSERT_SQL =
            "INSERT INTO report.biz_rt_order ("
                    + "ReportDate, OrderId, ReturnId, DocumentType, ShopCode, ShopName, OrderStatus, "
                    + "SalesAmount, ReturnAmount, NetRevenue, TotalReceipts, TotalExpenditures, NetCashFlow, "
                    + "PaymentMethodSummary, TotalOrderAmount, TotalReturnAmount, ReconciliationDiff, "
                    + "PendingOrder, UpdatedAt"
                    + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    public static void main(String[] args) throws Exception {
        final FlinkConfig config = new FlinkConfig("config.ini");
        final StreamExecutionEnvironment env = config.createEnv();

        env.enableCheckpointing(config.getLong("flink", "checkpoint_interval_ms", 10_000L));
        final int kafkaParallelism = KafkaClientProps.kafkaParallelism(config);
        env.setParallelism(kafkaParallelism);

        long hotDays = config.getLong("clickhouse", "biz_rt_hot_days", 14L);
        int batchSize = config.getInt("clickhouse", "biz_rt_batch_size", 500);
        long batchIntervalMs = config.getLong("clickhouse", "biz_rt_batch_interval_ms", 2000L);

        LOG.info("Starting BizRealtimeReportJob hotDays={} batchSize={} flushMs={}",
                hotDays, batchSize, batchIntervalMs);

        DataStream<OrderBundle> orders = env
                .fromSource(
                        KafkaSourceBuilder.buildKafkaSource(config, "order-bundles", ORDER_GROUP, true),
                        WatermarkStrategy.noWatermarks(),
                        "Kafka-OrderBizRt")
                .setParallelism(kafkaParallelism)
                .map(json -> JsonUtils.fromJson(json, OrderBundle.class))
                .returns(OrderBundle.class)
                .filter(b -> b != null && b.getOrder() != null && b.getOrder().getId() != null)
                .name("Parse-OrderBundle");

        DataStream<PaymentBundle> payments = env
                .fromSource(
                        KafkaSourceBuilder.buildKafkaSource(config, "payment-bundles", PAYMENT_GROUP, true),
                        WatermarkStrategy.noWatermarks(),
                        "Kafka-PaymentBizRt")
                .setParallelism(kafkaParallelism)
                .map(json -> JsonUtils.fromJson(json, PaymentBundle.class))
                .returns(PaymentBundle.class)
                .filter(BizRealtimeReportJob::hasOrderId)
                .name("Parse-PaymentBundle");

        SingleOutputStreamOperator<BizRtOrderRow> rows = orders
                .keyBy(b -> b.getOrder().getId())
                .connect(payments.keyBy(BizRealtimeReportJob::resolveOrderId))
                .process(new BizRtOrderCoProcessFunction(hotDays))
                .name("Join-Order-Payment-Reconcile");

        rows.addSink(buildClickHouseSink(config, batchSize, batchIntervalMs))
                .name("ClickHouse-biz_rt_order")
                .setParallelism(1);

        env.execute("Kaizen-BizRealtimeReportJob");
    }

    private static boolean hasOrderId(PaymentBundle b) {
        return b != null && b.getPayment() != null && resolveOrderId(b) != null;
    }

    private static String resolveOrderId(PaymentBundle bundle) {
        OrderPayment op = bundle.getOrderPayment();
        if (op != null && op.getOrderId() != null && !op.getOrderId().isEmpty()) {
            return op.getOrderId();
        }
        ReturnPayment rp = bundle.getReturnPayment();
        if (rp != null && rp.getOrderId() != null && !rp.getOrderId().isEmpty()) {
            return rp.getOrderId();
        }
        return null;
    }

    private static SinkFunction<BizRtOrderRow> buildClickHouseSink(
            FlinkConfig config, int batchSize, long batchIntervalMs) {
        String host = config.getOrDefault("clickhouse", "host",
                "clickhouse-service.databases.svc.cluster.local");
        int port = config.getInt("clickhouse", "port", 8123);
        String database = config.getOrDefault("clickhouse", "database", "default");
        String user = config.getOrDefault("clickhouse", "username", "admin");
        String password = config.getOrDefault("clickhouse", "password", "kaizen123");
        String jdbcUrl = "jdbc:clickhouse://" + host + ":" + port + "/" + database;

        return JdbcSink.sink(
                INSERT_SQL,
                (ps, r) -> {
                    ps.setString(1, r.getReportDate());
                    ps.setString(2, r.getOrderId());
                    ps.setString(3, r.getReturnId() == null ? "" : r.getReturnId());
                    ps.setString(4, r.getDocumentType());
                    ps.setString(5, r.getShopCode());
                    ps.setString(6, r.getShopName());
                    ps.setString(7, r.getOrderStatus());
                    ps.setDouble(8, r.getSalesAmount());
                    ps.setDouble(9, r.getReturnAmount());
                    ps.setDouble(10, r.getNetRevenue());
                    ps.setDouble(11, r.getTotalReceipts());
                    ps.setDouble(12, r.getTotalExpenditures());
                    ps.setDouble(13, r.getNetCashFlow());
                    ps.setString(14, r.getPaymentMethodSummary());
                    ps.setDouble(15, r.getTotalOrderAmount());
                    ps.setDouble(16, r.getTotalReturnAmount());
                    ps.setDouble(17, r.getReconciliationDiff());
                    ps.setByte(18, r.getPendingOrder());
                    ps.setTimestamp(19, new java.sql.Timestamp(r.getUpdatedAtMs()));
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(batchSize)
                        .withBatchIntervalMs(batchIntervalMs)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(jdbcUrl)
                        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                        .withUsername(user)
                        .withPassword(password)
                        .build());
    }
}
