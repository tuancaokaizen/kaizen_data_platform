package com.kaizen.stream.utils.connector.sink.postgres;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VersionedUpsertTest {

    @Test
    void buildsConditionalUpsert() {
        VersionedUpsert u = VersionedUpsert.into("order", "Order")
                .key("Id")
                .columns("ShopCode", "TotalPayment", "Status", "OrderDate", "CreatedDate", "ModifiedDate")
                .insertOnly("OrderDate", "CreatedDate")
                .build();

        assertEquals("INSERT INTO \"order\".\"Order\" AS t (\"Id\", \"ShopCode\", \"TotalPayment\", \"Status\", "
                        + "\"OrderDate\", \"CreatedDate\", \"ModifiedDate\") VALUES (?, ?, ?, ?, ?, ?, ?) "
                        + "ON CONFLICT (\"Id\") DO UPDATE SET \"ShopCode\" = EXCLUDED.\"ShopCode\", "
                        + "\"TotalPayment\" = EXCLUDED.\"TotalPayment\", \"Status\" = EXCLUDED.\"Status\", "
                        + "\"ModifiedDate\" = EXCLUDED.\"ModifiedDate\" "
                        + "WHERE EXCLUDED.\"ModifiedDate\" >= t.\"ModifiedDate\"",
                u.sql());
        assertEquals(1, u.indexOf("Id"));
        assertEquals(7, u.indexOf("ModifiedDate"));
    }

    @Test
    void compositeKeyAndCustomVersion() {
        VersionedUpsert u = VersionedUpsert.into("report", "x")
                .key("A", "B").columns("V", "Version").versionColumn("Version").build();
        assertEquals("INSERT INTO \"report\".\"x\" AS t (\"A\", \"B\", \"V\", \"Version\") VALUES (?, ?, ?, ?) "
                        + "ON CONFLICT (\"A\", \"B\") DO UPDATE SET \"V\" = EXCLUDED.\"V\", "
                        + "\"Version\" = EXCLUDED.\"Version\" WHERE EXCLUDED.\"Version\" >= t.\"Version\"",
                u.sql());
    }

    @Test
    void immutableTableNeverUpdates() {
        VersionedUpsert u = VersionedUpsert.into("order", "OrderItem")
                .key("Id").columns("OrderId", "ModifiedDate").immutable().build();
        assertEquals("INSERT INTO \"order\".\"OrderItem\" AS t (\"Id\", \"OrderId\", \"ModifiedDate\") "
                + "VALUES (?, ?, ?) ON CONFLICT (\"Id\") DO NOTHING", u.sql());
        assertThrows(IllegalStateException.class, () -> VersionedUpsert.into("s", "t")
                .key("Id").columns("CreatedDate").insertOnly("CreatedDate").immutable().build());
    }

    @Test
    void rejectsBadDefinitions() {
        assertThrows(IllegalStateException.class,
                () -> VersionedUpsert.into("s", "t").columns("ModifiedDate").build());
        assertThrows(IllegalStateException.class,
                () -> VersionedUpsert.into("s", "t").key("Id").columns("Name").build());
        assertThrows(IllegalStateException.class,
                () -> VersionedUpsert.into("s", "t").key("Id").columns("Id", "ModifiedDate").build());
        assertThrows(IllegalStateException.class,
                () -> VersionedUpsert.into("s", "t").key("Id").columns("ModifiedDate").insertOnly("ModifiedDate").build());
        assertThrows(IllegalArgumentException.class,
                () -> VersionedUpsert.into("s", "t\"; drop").key("Id").columns("ModifiedDate").build());
        assertThrows(IllegalArgumentException.class,
                () -> VersionedUpsert.into("s", "t").key("Id").columns("ModifiedDate").build().indexOf("Nope"));
    }
}
