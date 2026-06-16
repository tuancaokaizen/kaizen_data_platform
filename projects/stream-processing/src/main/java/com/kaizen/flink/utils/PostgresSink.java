package com.kaizen.flink.utils;

import com.kaizen.flink.config.FlinkConfig;
import com.kaizen.flink.model.*;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

public class PostgresSink {

    protected PostgresSink() {}

    protected static JdbcConnectionOptions connOpts(FlinkConfig cfg) {
        String url = String.format("jdbc:postgresql://%s:%s/%s",
                cfg.get("postgres", "host"),
                cfg.get("postgres", "port"),
                cfg.get("postgres", "database"));
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(url)
                .withDriverName("org.postgresql.Driver")
                .withUsername(cfg.get("postgres", "username"))
                .withPassword(cfg.get("postgres", "password"))
                .build();
    }

    public static java.sql.Connection getConnection(FlinkConfig cfg) throws java.sql.SQLException {
        String url = String.format("jdbc:postgresql://%s:%s/%s",
                cfg.get("postgres", "host"),
                cfg.get("postgres", "port"),
                cfg.get("postgres", "database"));
        try {
            // Explicitly register driver — required in Flink fat JAR / classloader context
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new java.sql.SQLException("PostgreSQL JDBC driver not found in classpath", e);
        }
        return java.sql.DriverManager.getConnection(url,
                cfg.get("postgres", "username"),
                cfg.get("postgres", "password"));
    }

    protected static JdbcExecutionOptions execOpts() {
        return JdbcExecutionOptions.builder()
                .withBatchSize(500)
                .withBatchIntervalMs(2000)
                .withMaxRetries(3)
                .build();
    }

    protected static Timestamp ts(long epochMs) { return new Timestamp(epochMs); }
    protected static Timestamp tsNull(Long epochMs) { return epochMs != null ? new Timestamp(epochMs) : null; }



    // ─────────────────────────────────────────────────────────────────────────
    // Order Domain
    // ─────────────────────────────────────────────────────────────────────────

    public static SinkFunction<Order> buildOrderSink(FlinkConfig cfg) {
        return JdbcSink.sink(
            "INSERT INTO \"order\".\"Order\" (\"Id\",\"CustomerId\",\"ShopCode\",\"TransactionDate\",\"TotalBill\",\"TotalDiscount\",\"TotalPayment\",\"EmployeeSaleId\",\"Status\",\"CreatedDate\",\"ModifiedDate\") " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?) " +
            "ON CONFLICT (\"Id\") DO UPDATE SET " +
            "\"Status\"=EXCLUDED.\"Status\", \"TotalPayment\"=EXCLUDED.\"TotalPayment\", \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\"",
            (stmt, o) -> {
                stmt.setString(1, o.getId());
                stmt.setString(2, o.getCustomerId());
                stmt.setObject(3, o.getShopCode(), Types.VARCHAR);
                stmt.setTimestamp(4, tsNull(o.getTransactionDate()));
                stmt.setObject(5, o.getTotalBill(), Types.INTEGER);
                stmt.setObject(6, o.getTotalDiscount(), Types.INTEGER);
                stmt.setObject(7, o.getTotalPayment(), Types.INTEGER);
                stmt.setString(8, o.getEmployeeSaleId());
                stmt.setObject(9, o.getStatus(), Types.VARCHAR);
                stmt.setTimestamp(10, ts(o.getCreatedDate()));
                stmt.setTimestamp(11, ts(o.getModifiedDate()));
            },
            execOpts(), connOpts(cfg));
    }

    public static SinkFunction<OrderItem> buildOrderItemSink(FlinkConfig cfg) {
        return JdbcSink.sink(
            "INSERT INTO \"order\".\"OrderItem\" (\"Id\",\"OrderId\",\"ProductId\",\"Quantity\",\"UnitPrice\",\"CreatedDate\",\"ModifiedDate\") " +
            "VALUES (?,?,?,?,?,?,?) " +
            "ON CONFLICT (\"Id\",\"OrderId\") DO UPDATE SET " +
            "\"Quantity\"=EXCLUDED.\"Quantity\", \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\"",
            (stmt, i) -> {
                stmt.setString(1, i.getId());
                stmt.setString(2, i.getOrderId());
                stmt.setObject(3, i.getProductId(), Types.VARCHAR);
                stmt.setObject(4, i.getQuantity(), Types.INTEGER);
                stmt.setObject(5, i.getUnitPrice(), Types.INTEGER);
                stmt.setTimestamp(6, tsNull(i.getCreatedDate()));
                stmt.setTimestamp(7, tsNull(i.getModifiedDate()));
            },
            execOpts(), connOpts(cfg));
    }

    public static SinkFunction<Return> buildReturnSink(FlinkConfig cfg) {
        return JdbcSink.sink(
            "INSERT INTO \"order\".\"Return\" (\"Id\",\"OrderId\",\"ReturnAmount\",\"CreatedDate\",\"ModifiedDate\") " +
            "VALUES (?,?,?,?,?) " +
            "ON CONFLICT (\"Id\",\"OrderId\") DO UPDATE SET \"ReturnAmount\"=EXCLUDED.\"ReturnAmount\"",
            (stmt, r) -> {
                stmt.setString(1, r.getId());
                stmt.setString(2, r.getOrderId());
                stmt.setObject(3, r.getReturnAmount(), Types.INTEGER);
                stmt.setTimestamp(4, tsNull(r.getCreatedDate()));
                stmt.setTimestamp(5, tsNull(r.getModifiedTimeMs()));
            },
            execOpts(), connOpts(cfg));
    }

    public static SinkFunction<ReturnItem> buildReturnItemSink(FlinkConfig cfg) {
        return JdbcSink.sink(
            "INSERT INTO \"order\".\"ReturnItem\" (\"Id\",\"ReturnId\",\"ProductId\",\"Quantity\",\"UnitPrice\",\"OrderItemId\",\"OrderId\",\"TotalAmount\",\"CreatedDate\",\"ModifiedDate\") " +
            "VALUES (?,?,?,?,?,?,?,?,?,?) " +
            "ON CONFLICT (\"Id\") DO UPDATE SET " +
            "\"Quantity\"=EXCLUDED.\"Quantity\", \"TotalAmount\"=EXCLUDED.\"TotalAmount\", \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\"",
            (stmt, ri) -> {
                stmt.setString(1, ri.getId());
                stmt.setString(2, ri.getReturnId());
                stmt.setObject(3, ri.getProductId(), Types.VARCHAR);
                stmt.setInt(4, ri.getQuantity());
                stmt.setObject(5, ri.getUnitPrice(), Types.INTEGER);
                stmt.setObject(6, ri.getOrderItemId(), Types.VARCHAR);
                stmt.setObject(7, ri.getOrderId(), Types.VARCHAR);
                stmt.setObject(8, ri.getTotalAmount(), Types.INTEGER);
                stmt.setTimestamp(9, tsNull(ri.getCreatedDate()));
                stmt.setTimestamp(10, tsNull(ri.getModifiedDate()));
            },
            execOpts(), connOpts(cfg));
    }



    // ─────────────────────────────────────────────────────────────────────────
    // Payment Domain
    // ─────────────────────────────────────────────────────────────────────────

    public static SinkFunction<Payment> buildPaymentSink(FlinkConfig cfg) {
        return JdbcSink.sink(
            "INSERT INTO payment.\"Payment\" (\"Id\",\"PaymentType\",\"Status\",\"TotalPayment\",\"CreatedDate\",\"ModifiedDate\") " +
            "VALUES (?,?,?,?,?,?) " +
            "ON CONFLICT (\"Id\") DO UPDATE SET \"Status\"=EXCLUDED.\"Status\", \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\"",
            (stmt, p) -> {
                stmt.setString(1, p.getId());
                stmt.setShort(2, p.getPaymentType());
                stmt.setObject(3, p.getStatus(), Types.VARCHAR);
                stmt.setInt(4, p.getTotalPayment());
                stmt.setTimestamp(5, ts(p.getCreatedDate()));
                stmt.setTimestamp(6, ts(p.getModifiedDate()));
            },
            execOpts(), connOpts(cfg));
    }

    public static SinkFunction<PaymentTransaction> buildTransactionSink(FlinkConfig cfg) {
        return JdbcSink.sink(
            "INSERT INTO payment.\"Transaction\" (\"Id\",\"PaymentId\",\"PaymentMethodId\",\"Amount\",\"CreatedDate\",\"ModifiedDate\") " +
            "VALUES (?,?,?,?,?,?) " +
            "ON CONFLICT (\"Id\") DO UPDATE SET \"Amount\"=EXCLUDED.\"Amount\", \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\"",
            (stmt, t) -> {
                stmt.setString(1, t.getId());
                stmt.setString(2, t.getPaymentId());
                stmt.setShort(3, t.getPaymentMethodId());
                stmt.setInt(4, t.getAmount());
                stmt.setTimestamp(5, ts(t.getCreatedDate()));
                stmt.setTimestamp(6, ts(t.getModifiedDate()));
            },
            execOpts(), connOpts(cfg));
    }

    public static SinkFunction<OrderPayment> buildOrderPaymentSink(FlinkConfig cfg) {
        return JdbcSink.sink(
            "INSERT INTO payment.\"OrderPayment\" (\"Id\",\"PaymentId\",\"OrderId\",\"CreatedDate\",\"ModifiedDate\") " +
            "VALUES (?,?,?,?,?) " +
            "ON CONFLICT (\"Id\") DO UPDATE SET \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\"",
            (stmt, op) -> {
                stmt.setString(1, op.getId());
                stmt.setString(2, op.getPaymentId());
                stmt.setString(3, op.getOrderId());
                stmt.setTimestamp(4, ts(op.getCreatedDate()));
                stmt.setTimestamp(5, ts(op.getModifiedDate()));
            },
            execOpts(), connOpts(cfg));
    }

    public static SinkFunction<ReturnPayment> buildReturnPaymentSink(FlinkConfig cfg) {
        return JdbcSink.sink(
            "INSERT INTO payment.\"ReturnPayment\" (\"Id\",\"PaymentId\",\"ReturnId\",\"OrderId\",\"CreatedDate\",\"ModifiedDate\") " +
            "VALUES (?,?,?,?,?,?) " +
            "ON CONFLICT (\"Id\") DO UPDATE SET \"ModifiedDate\"=EXCLUDED.\"ModifiedDate\"",
            (stmt, rp) -> {
                stmt.setString(1, rp.getId());
                stmt.setString(2, rp.getPaymentId());
                stmt.setString(3, rp.getReturnId());
                stmt.setString(4, rp.getOrderId());
                stmt.setTimestamp(5, ts(rp.getCreatedDate()));
                stmt.setTimestamp(6, ts(rp.getModifiedDate()));
            },
            execOpts(), connOpts(cfg));
    }
}
