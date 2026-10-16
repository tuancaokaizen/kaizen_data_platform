package com.kaizen.stream.simulation.saleswriter.model;

import com.kaizen.stream.utils.connector.sink.postgres.VersionedUpsert;

/**
 * Target tables of PaymentPostgresSink in PG schema "payment" (schema_manager/postgres_source/payment.sql).
 * Bảng đích của PaymentPostgresSink trong schema PG "payment" (schema_manager/postgres_source/payment.sql).
 */
public final class PaymentTables {

    public static final VersionedUpsert PAYMENT = VersionedUpsert.into("payment", "Payment")
            .key("Id")
            .columns("PaymentType", "Status", "OrderId", "ReturnId", "ShopCode", "TotalPayment", "Attempt",
                    "PaymentDate", "CreatedDate", "ModifiedDate")
            .insertOnly("PaymentDate", "CreatedDate")
            .build();

    public static final VersionedUpsert TRANSACTION = VersionedUpsert.into("payment", "Transaction")
            .key("Id")
            .columns("PaymentId", "PaymentMethodId", "Amount", "PaymentDate", "CreatedDate", "ModifiedDate")
            .immutable()
            .build();

    public static final VersionedUpsert ORDER_PAYMENT = VersionedUpsert.into("payment", "OrderPayment")
            .key("Id")
            .columns("PaymentId", "OrderId", "PaymentDate", "CreatedDate", "ModifiedDate")
            .immutable()
            .build();

    public static final VersionedUpsert RETURN_PAYMENT = VersionedUpsert.into("payment", "ReturnPayment")
            .key("Id")
            .columns("PaymentId", "ReturnId", "OrderId", "PaymentDate", "CreatedDate", "ModifiedDate")
            .immutable()
            .build();

    private PaymentTables() {
    }
}
