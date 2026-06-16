package com.kaizen.flink.entrypoint;

import com.kaizen.flink.config.FlinkConfig;
import com.kaizen.flink.model.OrderBundle;
import com.kaizen.flink.model.OrderItem;
import com.kaizen.flink.model.Return;
import com.kaizen.flink.model.ReturnItem;
import com.kaizen.flink.model.PaymentBundle;
import com.kaizen.flink.model.PaymentTransaction;
import com.kaizen.flink.sink.EsSink;
import com.kaizen.flink.source.KafkaSourceBuilder;
import com.kaizen.flink.utils.JsonUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

/**
 * Unified Consumer Job - KaizenConsumerJob
 *
 * Consolidates OrderPgConsumerJob, PaymentPgConsumerJob, and TransactionEsConsumerJob
 * into a single Flink application. This allows all three streaming topologies to share
 * Task Slots under the same default Slot Sharing Group, dramatically reducing required Flink slots.
 */
public class KaizenConsumerJob {

    private static final Logger LOG = LoggerFactory.getLogger(KaizenConsumerJob.class);
    private static final String PG_ORDER_GROUP_ID = "kaizen-pg-order-group";
    private static final String PG_PAYMENT_GROUP_ID = "kaizen-pg-payment-group";
    private static final String ES_GROUP_ID = "kaizen-es-transaction-group";

    public static void main(String[] args) throws Exception {
        final FlinkConfig config = new FlinkConfig("config.ini");
        final StreamExecutionEnvironment env = config.createEnv();

        env.enableCheckpointing(config.getLong("flink", "checkpoint_interval_ms", 10_000L));
        env.setParallelism(config.getInt("flink", "parallelism", 2));

        // ── 1. Order-bundles → PostgreSQL Sink ────────────────────────────────
        KafkaSource<String> orderPgSrc = KafkaSourceBuilder.buildKafkaSource(config, "order-bundles", PG_ORDER_GROUP_ID);
        env.fromSource(orderPgSrc, WatermarkStrategy.noWatermarks(), "Kafka-OrderPg")
           .map(json -> JsonUtils.fromJson(json, OrderBundle.class))
           .returns(org.apache.flink.api.common.typeinfo.TypeInformation.of(OrderBundle.class))
           .addSink(new OrderBundlePgSink(config))
           .name("PG-OrderSink");

        // ── 2. Payment-bundles → PostgreSQL Sink ──────────────────────────────
        KafkaSource<String> paymentPgSrc = KafkaSourceBuilder.buildKafkaSource(config, "payment-bundles", PG_PAYMENT_GROUP_ID);
        env.fromSource(paymentPgSrc, WatermarkStrategy.noWatermarks(), "Kafka-PaymentPg")
           .map(json -> JsonUtils.fromJson(json, PaymentBundle.class))
           .returns(org.apache.flink.api.common.typeinfo.TypeInformation.of(PaymentBundle.class))
           .addSink(new PaymentBundlePgSink(config))
           .name("PG-PaymentSink");

        // ── 3. Order-bundles → Elasticsearch Sink ──────────────────────────────
        KafkaSource<String> orderEsSrc = KafkaSourceBuilder.buildKafkaSource(config, "order-bundles", ES_GROUP_ID);
        env.fromSource(orderEsSrc, WatermarkStrategy.noWatermarks(), "Kafka-OrderEs")
           .sinkTo(EsSink.buildStringIndexSink(config, "order-bundles"))
           .name("ES-OrderSink");

        // ── 4. Payment-bundles → Elasticsearch Sink ────────────────────────────
        KafkaSource<String> paymentEsSrc = KafkaSourceBuilder.buildKafkaSource(config, "payment-bundles", ES_GROUP_ID);
        env.fromSource(paymentEsSrc, WatermarkStrategy.noWatermarks(), "Kafka-PaymentEs")
           .sinkTo(EsSink.buildStringIndexSink(config, "payment-bundles"))
           .name("ES-PaymentSink");

        LOG.info("Starting KaizenConsumerJob...");
        env.execute("Kaizen-KaizenConsumerJob");
    }

    // ── RichSinkFunction - Order PG ──────────────────────────────────────────

    @SuppressWarnings("serial")
    private static final class OrderBundlePgSink extends RichSinkFunction<OrderBundle> {
        private final FlinkConfig config;
        private transient Connection conn;

        OrderBundlePgSink(FlinkConfig config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = com.kaizen.flink.utils.PostgresSink.getConnection(config);
            conn.setAutoCommit(false);
            com.kaizen.flink.simulate.IdPool.init(config);
        }

        @Override
        public void invoke(OrderBundle bundle, Context ctx) throws Exception {
            try {
                ensureConnected();
                upsertOrder(bundle.getOrder());
                List<OrderItem> items = bundle.getOrderItems();
                if (items != null) {
                    for (OrderItem item : items) upsertOrderItem(item);
                }
                Return ret = bundle.getReturnRecord();
                if (ret != null) {
                    upsertReturn(ret);
                    List<ReturnItem> retItems = bundle.getReturnItems();
                    if (retItems != null) {
                        for (ReturnItem ri : retItems) upsertReturnItem(ri);
                    }
                }
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                LOG.error("Failed to upsert OrderBundle {}: {}", bundle.getOrder().getId(), e.getMessage());
                throw e;
            }
        }

        @Override
        public void close() throws Exception {
            if (conn != null && !conn.isClosed()) conn.close();
        }

        private void ensureConnected() throws SQLException {
            if (conn == null || conn.isClosed()) {
                conn = com.kaizen.flink.utils.PostgresSink.getConnection(config);
                conn.setAutoCommit(false);
            }
        }

        private void upsertOrder(com.kaizen.flink.model.Order o) throws SQLException {
            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO \"order\".\"Order\" " +
                "(\"Id\",\"CustomerId\",\"ShopCode\",\"TransactionDate\",\"TotalBill\",\"TotalDiscount\",\"TotalPayment\",\"EmployeeSaleId\",\"Status\",\"CreatedDate\",\"ModifiedDate\",\"VoucherSeriesCode\") " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?) " +
                "ON CONFLICT (\"Id\") DO UPDATE SET " +
                "\"CustomerId\"=EXCLUDED.\"CustomerId\", " +
                "\"ShopCode\"=EXCLUDED.\"ShopCode\", " +
                "\"TransactionDate\"=EXCLUDED.\"TransactionDate\", " +
                "\"TotalBill\"=EXCLUDED.\"TotalBill\", " +
                "\"TotalDiscount\"=EXCLUDED.\"TotalDiscount\", " +
                "\"TotalPayment\"=EXCLUDED.\"TotalPayment\", " +
                "\"EmployeeSaleId\"=EXCLUDED.\"EmployeeSaleId\", " +
                "\"Status\"=EXCLUDED.\"Status\", " +
                "\"ModifiedDate\"=EXCLUDED.\"ModifiedDate\", " +
                "\"VoucherSeriesCode\"=EXCLUDED.\"VoucherSeriesCode\"")) {
                ps.setString(1, o.getId());
                ps.setString(2, o.getCustomerId());
                ps.setObject(3, o.getShopCode(), Types.VARCHAR);
                ps.setTimestamp(4, o.getTransactionDate() != null ? new Timestamp(o.getTransactionDate()) : null);
                ps.setObject(5, o.getTotalBill(), Types.INTEGER);
                ps.setObject(6, o.getTotalDiscount(), Types.INTEGER);
                ps.setObject(7, o.getTotalPayment(), Types.INTEGER);
                ps.setString(8, o.getEmployeeSaleId());
                ps.setObject(9, o.getStatus(), Types.VARCHAR);
                ps.setTimestamp(10, new Timestamp(o.getCreatedDate()));
                ps.setTimestamp(11, new Timestamp(o.getModifiedDate()));
                ps.setObject(12, o.getVoucherSeriesCode(), Types.VARCHAR);
                ps.executeUpdate();
            }
        }

        private void upsertOrderItem(OrderItem i) throws SQLException {
            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO \"order\".\"OrderItem\" " +
                "(\"Id\",\"OrderId\",\"ProductId\",\"Quantity\",\"UnitPrice\",\"CreatedDate\",\"ModifiedDate\") " +
                "VALUES (?,?,?,?,?,?,?) " +
                "ON CONFLICT (\"Id\",\"OrderId\") DO UPDATE SET " +
                "\"Quantity\"=EXCLUDED.\"Quantity\", \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\"")) {
                ps.setString(1, i.getId());
                ps.setString(2, i.getOrderId());
                ps.setObject(3, i.getProductId(), Types.VARCHAR);
                ps.setObject(4, i.getQuantity(), Types.INTEGER);
                ps.setObject(5, i.getUnitPrice(), Types.INTEGER);
                ps.setTimestamp(6, i.getCreatedDate() != null ? new Timestamp(i.getCreatedDate()) : null);
                ps.setTimestamp(7, i.getModifiedDate() != null ? new Timestamp(i.getModifiedDate()) : null);
                ps.executeUpdate();
            }
        }

        private void upsertReturn(Return r) throws SQLException {
            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO \"order\".\"Return\" " +
                "(\"Id\",\"OrderId\",\"ReturnAmount\",\"CreatedDate\",\"ModifiedDate\") " +
                "VALUES (?,?,?,?,?) " +
                "ON CONFLICT (\"Id\",\"OrderId\") DO UPDATE SET \"ReturnAmount\"=EXCLUDED.\"ReturnAmount\"")) {
                ps.setString(1, r.getId());
                ps.setString(2, r.getOrderId());
                ps.setObject(3, r.getReturnAmount(), Types.INTEGER);
                ps.setTimestamp(4, r.getCreatedDate() != null ? new Timestamp(r.getCreatedDate()) : null);
                ps.setTimestamp(5, r.getModifiedTimeMs() != null ? new Timestamp(r.getModifiedTimeMs()) : null);
                ps.executeUpdate();
            }
        }

        private void upsertReturnItem(ReturnItem ri) throws SQLException {
            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO \"order\".\"ReturnItem\" " +
                "(\"Id\",\"ReturnId\",\"ProductId\",\"Quantity\",\"UnitPrice\",\"OrderItemId\",\"OrderId\",\"TotalAmount\",\"CreatedDate\",\"ModifiedDate\") " +
                "VALUES (?,?,?,?,?,?,?,?,?,?) " +
                "ON CONFLICT (\"Id\") DO UPDATE SET " +
                "\"Quantity\"=EXCLUDED.\"Quantity\", \"TotalAmount\"=EXCLUDED.\"TotalAmount\", \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\"")) {
                ps.setString(1, ri.getId());
                ps.setString(2, ri.getReturnId());
                ps.setObject(3, ri.getProductId(), Types.VARCHAR);
                ps.setInt(4, ri.getQuantity());
                ps.setObject(5, ri.getUnitPrice(), Types.INTEGER);
                ps.setObject(6, ri.getOrderItemId(), Types.VARCHAR);
                ps.setObject(7, ri.getOrderId(), Types.VARCHAR);
                ps.setObject(8, ri.getTotalAmount(), Types.INTEGER);
                ps.setTimestamp(9, ri.getCreatedDate() != null ? new Timestamp(ri.getCreatedDate()) : null);
                ps.setTimestamp(10, ri.getModifiedDate() != null ? new Timestamp(ri.getModifiedDate()) : null);
                ps.executeUpdate();
            }
        }
    }

    // ── RichSinkFunction - Payment PG ────────────────────────────────────────

    @SuppressWarnings("serial")
    private static final class PaymentBundlePgSink extends RichSinkFunction<PaymentBundle> {
        private final FlinkConfig config;
        private transient Connection conn;

        PaymentBundlePgSink(FlinkConfig config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = com.kaizen.flink.utils.PostgresSink.getConnection(config);
            conn.setAutoCommit(false);
            com.kaizen.flink.simulate.IdPool.init(config);
        }

        @Override
        public void invoke(PaymentBundle bundle, Context ctx) throws Exception {
            try {
                ensureConnected();

                // Skip processing the payment bundle if the referenced order or return does not exist yet.
                // This prevents writing temporary stubs/placeholders with "Pending" status and respects FK constraints.
                if (bundle.getOrderPayment() != null) {
                    String orderId = bundle.getOrderPayment().getOrderId();
                    if (!orderExists(orderId)) {
                        LOG.warn("Skipping PaymentBundle {} because Order {} does not exist in DB yet.",
                                bundle.getPayment().getId(), orderId);
                        return;
                    }
                }

                if (bundle.getReturnPayment() != null) {
                    String returnId = bundle.getReturnPayment().getReturnId();
                    String orderId = bundle.getReturnPayment().getOrderId();
                    if (!returnExists(returnId, orderId)) {
                        LOG.warn("Skipping PaymentBundle {} because Return {} / Order {} does not exist in DB yet.",
                                bundle.getPayment().getId(), returnId, orderId);
                        return;
                    }
                }

                upsertPayment(bundle.getPayment());
                List<PaymentTransaction> txs = bundle.getTransactions();
                if (txs != null) {
                    for (PaymentTransaction tx : txs) upsertTransaction(tx);
                }
                if (bundle.getOrderPayment() != null) {
                    upsertOrderPayment(bundle.getOrderPayment());
                }
                if (bundle.getReturnPayment() != null) {
                    upsertReturnPayment(bundle.getReturnPayment());
                }
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                LOG.error("Failed to upsert PaymentBundle {}: {}", bundle.getPayment().getId(), e.getMessage());
                throw e;
            }
        }

        @Override
        public void close() throws Exception {
            if (conn != null && !conn.isClosed()) conn.close();
        }

        private void ensureConnected() throws SQLException {
            if (conn == null || conn.isClosed()) {
                conn = com.kaizen.flink.utils.PostgresSink.getConnection(config);
                conn.setAutoCommit(false);
            }
        }

        private boolean orderExists(String orderId) throws SQLException {
            try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM \"order\".\"Order\" WHERE \"Id\" = ?")) {
                ps.setString(1, orderId);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next();
                }
            }
        }

        private boolean returnExists(String returnId, String orderId) throws SQLException {
            try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM \"order\".\"Return\" WHERE \"Id\" = ? AND \"OrderId\" = ?")) {
                ps.setString(1, returnId);
                ps.setString(2, orderId);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next();
                }
            }
        }

        private void upsertPayment(com.kaizen.flink.model.Payment p) throws SQLException {
            short pType = p.getPaymentType();
            if (pType != 1 && pType != 2) {
                pType = 1; // Fallback to 1 (Phiếu thu)
            }
            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO payment.\"Payment\" (\"Id\",\"PaymentType\",\"Status\",\"TotalPayment\",\"CreatedDate\",\"ModifiedDate\") " +
                "VALUES (?,?,?,?,?,?) " +
                "ON CONFLICT (\"Id\") DO UPDATE SET \"Status\"=EXCLUDED.\"Status\", \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\"")) {
                ps.setString(1, p.getId());
                ps.setShort(2, pType);
                ps.setObject(3, p.getStatus(), Types.VARCHAR);
                ps.setInt(4, p.getTotalPayment());
                ps.setTimestamp(5, new Timestamp(p.getCreatedDate()));
                ps.setTimestamp(6, new Timestamp(p.getModifiedDate()));
                ps.executeUpdate();
            }
        }

        private void upsertTransaction(PaymentTransaction t) throws SQLException {
            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO payment.\"Transaction\" (\"Id\",\"PaymentId\",\"PaymentMethodId\",\"Amount\",\"CreatedDate\",\"ModifiedDate\") " +
                "VALUES (?,?,?,?,?,?) " +
                "ON CONFLICT (\"Id\") DO UPDATE SET \"Amount\"=EXCLUDED.\"Amount\", \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\"")) {
                ps.setString(1, t.getId());
                ps.setString(2, t.getPaymentId());
                ps.setShort(3, t.getPaymentMethodId());
                ps.setInt(4, t.getAmount());
                ps.setTimestamp(5, new Timestamp(t.getCreatedDate()));
                ps.setTimestamp(6, new Timestamp(t.getModifiedDate()));
                ps.executeUpdate();
            }
        }

        private void upsertOrderPayment(com.kaizen.flink.model.OrderPayment op) throws SQLException {
            Timestamp ts = new Timestamp(op.getCreatedDate());
            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO payment.\"OrderPayment\" (\"Id\",\"PaymentId\",\"OrderId\",\"CreatedDate\",\"ModifiedDate\") " +
                "VALUES (?,?,?,?,?) " +
                "ON CONFLICT (\"Id\") DO UPDATE SET \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\"")) {
                ps.setString(1, op.getId());
                ps.setString(2, op.getPaymentId());
                ps.setString(3, op.getOrderId());
                ps.setTimestamp(4, ts);
                ps.setTimestamp(5, new Timestamp(op.getModifiedDate()));
                ps.executeUpdate();
            }
        }

        private void upsertReturnPayment(com.kaizen.flink.model.ReturnPayment rp) throws SQLException {
            Timestamp ts = new Timestamp(rp.getCreatedDate());
            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO payment.\"ReturnPayment\" (\"Id\",\"PaymentId\",\"ReturnId\",\"OrderId\",\"CreatedDate\",\"ModifiedDate\") " +
                "VALUES (?,?,?,?,?,?) " +
                "ON CONFLICT (\"Id\") DO UPDATE SET \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\"")) {
                ps.setString(1, rp.getId());
                ps.setString(2, rp.getPaymentId());
                ps.setString(3, rp.getReturnId());
                ps.setString(4, rp.getOrderId());
                ps.setTimestamp(5, ts);
                ps.setTimestamp(6, new Timestamp(rp.getModifiedDate()));
                ps.executeUpdate();
            }
        }
    }
}
