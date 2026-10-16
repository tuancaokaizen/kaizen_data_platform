package com.kaizen.stream.simulation.saleswriter.sink;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.payment.PaymentHeader;
import com.kaizen.stream.model.payment.PaymentSnapshot;
import com.kaizen.stream.model.payment.PaymentTransaction;
import com.kaizen.stream.model.payment.PaymentType;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.job.OperatorNames;
import com.kaizen.stream.simulation.saleswriter.model.PaymentTables;
import com.kaizen.stream.simulation.saleswriter.model.SalesKeys;
import com.kaizen.stream.utils.connector.sink.postgres.PostgresAggregateSink;
import com.kaizen.stream.utils.connector.sink.postgres.SqlRow;
import com.kaizen.stream.utils.connector.sink.postgres.TableWriter;
import com.kaizen.stream.utils.time.IctTime;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.kaizen.stream.utils.connector.sink.postgres.SqlRow.smallint;
import static com.kaizen.stream.utils.connector.sink.postgres.SqlRow.timestamptz;

/**
 * Payment snapshot → "payment" schema (schema_manager/postgres_source/payment.sql).
 * The header moves PENDING → SUCCESS/FAILED; transactions appear once at SUCCESS; the link row
 * (OrderPayment for receipts, ReturnPayment for refunds) is written with the first snapshot, "Id" = payment id.
 * Snapshot phiếu → schema "payment" (schema_manager/postgres_source/payment.sql).
 * Header đi PENDING → SUCCESS/FAILED; giao dịch xuất hiện một lần khi SUCCESS; dòng liên kết
 * (OrderPayment cho phiếu thu, ReturnPayment cho phiếu chi) ghi từ snapshot đầu tiên, "Id" = id phiếu.
 */
public final class PaymentPostgresSink {

    private PaymentPostgresSink() {
    }

    /** Parent tables first so foreign keys hold inside the transaction. / Bảng cha trước để FK đúng trong transaction. */
    public static List<TableWriter<PaymentSnapshot>> tables() {
        return new ArrayList<>(Arrays.asList(
                TableWriter.of(PaymentTables.PAYMENT, PaymentPostgresSink::paymentRows),
                TableWriter.of(PaymentTables.TRANSACTION, PaymentPostgresSink::transactionRows),
                TableWriter.of(PaymentTables.ORDER_PAYMENT, PaymentPostgresSink::orderPaymentRows),
                TableWriter.of(PaymentTables.RETURN_PAYMENT, PaymentPostgresSink::returnPaymentRows)));
    }

    static PostgresAggregateSink<PaymentSnapshot> sink(StreamSettings.Postgres postgres) {
        return new PostgresAggregateSink<>("payment", postgres, SalesKeys::paymentId, tables());
    }

    /**
     * sink.postgres.payment at source parallelism, so events of one order stay in Kafka partition order.
     * sink.postgres.payment cùng parallelism với nguồn để sự kiện của một đơn giữ thứ tự partition Kafka.
     */
    public static void write(DataStream<Envelope<PaymentSnapshot>> payments, JobContext context) {
        context.sink(payments, sink(context.settings().postgres()), OperatorNames.POSTGRES, "payment");
    }

    static List<SqlRow> paymentRows(PaymentSnapshot s) {
        PaymentHeader p = s.getPayment();
        return List.of(SqlRow.create()
                .set("Id", p.getId())
                .set("PaymentType", smallint(p.getType().code()))
                .set("Status", p.getStatus().name())
                .set("OrderId", p.getOrderId())
                .set("ReturnId", p.getReturnId())
                .set("ShopCode", p.getShopCode())
                .set("TotalPayment", p.getTotalAmount())
                .set("Attempt", smallint(p.getAttempt()))
                .set("PaymentDate", paymentDate(p))
                .set("CreatedDate", timestamptz(p.getCreatedDate()))
                .set("ModifiedDate", timestamptz(p.getModifiedDate())));
    }

    /** Transactions are created when the payment succeeds. / Giao dịch được tạo khi phiếu thành công. */
    static List<SqlRow> transactionRows(PaymentSnapshot s) {
        PaymentHeader p = s.getPayment();
        List<SqlRow> rows = new ArrayList<>();
        for (PaymentTransaction t : s.getTransactions()) {
            rows.add(SqlRow.create()
                    .set("Id", t.getId())
                    .set("PaymentId", p.getId())
                    .set("PaymentMethodId", smallint(t.getPaymentMethodId()))
                    .set("Amount", t.getAmount())
                    .set("PaymentDate", paymentDate(p))
                    .set("CreatedDate", timestamptz(p.getModifiedDate()))
                    .set("ModifiedDate", timestamptz(p.getModifiedDate())));
        }
        return rows;
    }

    static List<SqlRow> orderPaymentRows(PaymentSnapshot s) {
        PaymentHeader p = s.getPayment();
        if (p.getType() != PaymentType.RECEIPT) {
            return Collections.emptyList();
        }
        return List.of(SqlRow.create()
                .set("Id", p.getId())
                .set("PaymentId", p.getId())
                .set("OrderId", p.getOrderId())
                .set("PaymentDate", paymentDate(p))
                .set("CreatedDate", timestamptz(p.getCreatedDate()))
                .set("ModifiedDate", timestamptz(p.getCreatedDate())));
    }

    static List<SqlRow> returnPaymentRows(PaymentSnapshot s) {
        PaymentHeader p = s.getPayment();
        if (p.getType() != PaymentType.REFUND) {
            return Collections.emptyList();
        }
        return List.of(SqlRow.create()
                .set("Id", p.getId())
                .set("PaymentId", p.getId())
                .set("ReturnId", p.getReturnId())
                .set("OrderId", p.getOrderId())
                .set("PaymentDate", paymentDate(p))
                .set("CreatedDate", timestamptz(p.getCreatedDate()))
                .set("ModifiedDate", timestamptz(p.getCreatedDate())));
    }

    private static LocalDate paymentDate(PaymentHeader p) {
        return IctTime.businessDate(p.getCreatedDate());
    }
}
