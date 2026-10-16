package com.kaizen.stream.simulation.saleswriter.sink;

import com.kaizen.stream.config.StreamSettings;
import com.kaizen.stream.model.envelope.Envelope;
import com.kaizen.stream.model.order.OrderHeader;
import com.kaizen.stream.model.order.OrderLine;
import com.kaizen.stream.model.order.OrderSnapshot;
import com.kaizen.stream.model.order.ReturnDocument;
import com.kaizen.stream.model.order.ReturnLine;
import com.kaizen.stream.processing.job.JobContext;
import com.kaizen.stream.processing.job.OperatorNames;
import com.kaizen.stream.simulation.saleswriter.model.OrderTables;
import com.kaizen.stream.simulation.saleswriter.model.SalesKeys;
import com.kaizen.stream.simulation.saleswriter.sink.step.StockPosting;
import com.kaizen.stream.simulation.saleswriter.sink.step.VoucherRedemption;
import com.kaizen.stream.utils.connector.sink.postgres.PostgresAggregateSink;
import com.kaizen.stream.utils.connector.sink.postgres.SqlRow;
import com.kaizen.stream.utils.connector.sink.postgres.TableWriter;
import com.kaizen.stream.utils.time.IctTime;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.kaizen.stream.utils.connector.sink.postgres.SqlRow.timestamptz;

/**
 * Order snapshot → "order" schema (schema_manager/postgres_source/order.sql).
 * Only the header changes over time (status after a return); lines and returns are written once.
 * Snapshot đơn → schema "order" (schema_manager/postgres_source/order.sql).
 * Chỉ header thay đổi theo thời gian (trạng thái sau khi trả); dòng đơn và phiếu trả chỉ ghi một lần.
 */
public final class OrderPostgresSink {

    private OrderPostgresSink() {
    }

    /** Parent tables first so foreign keys hold inside the transaction. / Bảng cha trước để FK đúng trong transaction. */
    public static List<TableWriter<OrderSnapshot>> tables() {
        return new ArrayList<>(Arrays.asList(
                TableWriter.of(OrderTables.ORDER, OrderPostgresSink::orderRows),
                TableWriter.of(OrderTables.ORDER_ITEM, OrderPostgresSink::itemRows),
                TableWriter.of(OrderTables.RETURN, OrderPostgresSink::returnRows),
                TableWriter.of(OrderTables.RETURN_ITEM, OrderPostgresSink::returnItemRows)));
    }

    /**
     * Stock postings and voucher redemptions commit in the same transaction as the order rows.
     * Ghi sổ kho và đánh dấu mã voucher commit cùng transaction với dòng đơn.
     */
    static PostgresAggregateSink<OrderSnapshot> sink(StreamSettings.Postgres postgres) {
        return new PostgresAggregateSink<>("order", postgres, SalesKeys::orderId, tables(),
                Arrays.asList(new StockPosting(), new VoucherRedemption()));
    }

    /**
     * sink.postgres.order at source parallelism, so events of one order stay in Kafka partition order.
     * sink.postgres.order cùng parallelism với nguồn để sự kiện của một đơn giữ thứ tự partition Kafka.
     */
    public static void write(DataStream<Envelope<OrderSnapshot>> orders, JobContext context) {
        context.sink(orders, sink(context.settings().postgres()), OperatorNames.POSTGRES, "order");
    }

    static List<SqlRow> orderRows(OrderSnapshot s) {
        OrderHeader o = s.getOrder();
        return List.of(SqlRow.create()
                .set("Id", o.getId())
                .set("ShopCode", o.getShopCode())
                .set("CustomerId", o.getCustomerId())
                .set("EmployeeSaleId", o.getEmployeeSaleId())
                .set("TransactionDate", timestamptz(o.getTransactionDate()))
                .set("OrderDate", orderDate(o))
                .set("TotalBill", o.getTotalBill())
                .set("TotalDiscount", o.getTotalDiscount())
                .set("TotalPayment", o.getTotalPayment())
                .set("VoucherSeriesCode", o.getVoucherSeriesCode())
                .set("Status", o.getStatus().label())
                .set("CreatedDate", timestamptz(o.getCreatedDate()))
                .set("ModifiedDate", timestamptz(o.getModifiedDate())));
    }

    /** Lines exist from order creation on. / Dòng đơn tồn tại từ lúc tạo đơn. */
    static List<SqlRow> itemRows(OrderSnapshot s) {
        OrderHeader o = s.getOrder();
        List<SqlRow> rows = new ArrayList<>();
        for (OrderLine l : s.getItems()) {
            rows.add(SqlRow.create()
                    .set("Id", l.getId())
                    .set("OrderId", o.getId())
                    .set("LineNo", l.getLineNo())
                    .set("ProductId", l.getProductId())
                    .set("Quantity", l.getQuantity())
                    .set("UnitPrice", l.getUnitPrice())
                    .set("DiscountAmount", l.getDiscountAmount())
                    .set("LineAmount", l.getLineAmount())
                    .set("IsGift", Boolean.TRUE.equals(l.getGift()))
                    .set("UnitCost", l.getUnitCost())
                    .set("OrderDate", orderDate(o))
                    .set("CreatedDate", timestamptz(o.getCreatedDate()))
                    .set("ModifiedDate", timestamptz(o.getCreatedDate())));
        }
        return rows;
    }

    /** A return is booked at the order's shop on its own date. / Phiếu trả ghi ở shop của đơn, theo ngày của phiếu. */
    static List<SqlRow> returnRows(OrderSnapshot s) {
        OrderHeader o = s.getOrder();
        List<SqlRow> rows = new ArrayList<>();
        for (ReturnDocument r : s.getReturns()) {
            rows.add(SqlRow.create()
                    .set("Id", r.getId())
                    .set("OrderId", o.getId())
                    .set("ShopCode", o.getShopCode())
                    .set("ReturnDate", IctTime.businessDate(r.getCreatedDate()))
                    .set("Reason", r.getReason())
                    .set("ReturnAmount", r.getReturnAmount())
                    .set("CreatedDate", timestamptz(r.getCreatedDate()))
                    .set("ModifiedDate", timestamptz(r.getCreatedDate())));
        }
        return rows;
    }

    static List<SqlRow> returnItemRows(OrderSnapshot s) {
        OrderHeader o = s.getOrder();
        List<SqlRow> rows = new ArrayList<>();
        for (ReturnDocument r : s.getReturns()) {
            LocalDate returnDate = IctTime.businessDate(r.getCreatedDate());
            for (ReturnLine l : r.getItems()) {
                rows.add(SqlRow.create()
                        .set("Id", l.getId())
                        .set("ReturnId", r.getId())
                        .set("OrderId", o.getId())
                        .set("OrderItemId", l.getOrderItemId())
                        .set("ProductId", l.getProductId())
                        .set("Quantity", l.getQuantity())
                        .set("RestockQty", l.getRestockQty())
                        .set("DamageQty", l.getDamageQty())
                        .set("UnitPrice", l.getUnitPrice())
                        .set("UnitCost", l.getUnitCost())
                        .set("LineAmount", l.getLineAmount())
                        .set("ReturnDate", returnDate)
                        .set("CreatedDate", timestamptz(r.getCreatedDate()))
                        .set("ModifiedDate", timestamptz(r.getCreatedDate())));
            }
        }
        return rows;
    }

    private static LocalDate orderDate(OrderHeader o) {
        return IctTime.businessDate(o.getTransactionDate());
    }
}
