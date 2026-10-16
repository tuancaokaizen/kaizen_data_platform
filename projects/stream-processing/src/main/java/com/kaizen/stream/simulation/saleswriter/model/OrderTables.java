package com.kaizen.stream.simulation.saleswriter.model;

import com.kaizen.stream.utils.connector.sink.postgres.VersionedUpsert;

/**
 * Target tables of OrderPostgresSink in PG schema "order" (schema_manager/postgres_source/order.sql).
 * Bảng đích của OrderPostgresSink trong schema PG "order" (schema_manager/postgres_source/order.sql).
 */
public final class OrderTables {

    public static final VersionedUpsert ORDER = VersionedUpsert.into("order", "Order")
            .key("Id")
            .columns("ShopCode", "CustomerId", "EmployeeSaleId", "TransactionDate", "OrderDate",
                    "TotalBill", "TotalDiscount", "TotalPayment", "VoucherSeriesCode", "Status",
                    "CreatedDate", "ModifiedDate")
            .insertOnly("OrderDate", "CreatedDate")
            .build();

    public static final VersionedUpsert ORDER_ITEM = VersionedUpsert.into("order", "OrderItem")
            .key("Id")
            .columns("OrderId", "LineNo", "ProductId", "Quantity", "UnitPrice", "DiscountAmount", "LineAmount",
                    "IsGift", "UnitCost", "OrderDate", "CreatedDate", "ModifiedDate")
            .immutable()
            .build();

    public static final VersionedUpsert RETURN = VersionedUpsert.into("order", "Return")
            .key("Id")
            .columns("OrderId", "ShopCode", "ReturnDate", "Reason", "ReturnAmount", "CreatedDate", "ModifiedDate")
            .immutable()
            .build();

    public static final VersionedUpsert RETURN_ITEM = VersionedUpsert.into("order", "ReturnItem")
            .key("Id")
            .columns("ReturnId", "OrderId", "OrderItemId", "ProductId", "Quantity", "RestockQty", "DamageQty",
                    "UnitPrice", "UnitCost", "LineAmount", "ReturnDate", "CreatedDate", "ModifiedDate")
            .immutable()
            .build();

    private OrderTables() {
    }
}
