-- Schema "order" v2 (data_model_v2.md 3.2-3.3). Money is bigint VND, times are timestamptz.
-- Schema "order" v2 (data_model_v2.md 3.2-3.3). Tiền là bigint VND, thời gian là timestamptz.
-- Written by SalesWriterJob from order-bundles; LostSale by OrderSimulationJob and the day-close job.
-- Ghi bởi SalesWriterJob từ order-bundles; LostSale bởi OrderSimulationJob và job chốt sổ.
-- Business dates (OrderDate, ReturnDate, BusinessDate) are ICT dates set once at creation.
-- Ngày nghiệp vụ (OrderDate, ReturnDate, BusinessDate) là ngày giờ VN, ghi một lần lúc tạo.
-- Only FKs inside one aggregate are declared: rows from other streams or master tables may
-- arrive in any order; cross-aggregate integrity is checked by reconciliation instead.
-- Chỉ khai báo FK trong cùng một aggregate: dữ liệu từ stream khác hoặc bảng master có thể
-- đến lệch thứ tự; toàn vẹn giữa các aggregate do job đối soát kiểm tra.

BEGIN;

CREATE SCHEMA IF NOT EXISTS "order";

CREATE TABLE IF NOT EXISTS "order"."Order"
(
    "Id"                text        NOT NULL,
    "ShopCode"          text        NOT NULL,
    -- Null for walk-in customers / Null với khách vãng lai
    "CustomerId"        text,
    "EmployeeSaleId"    text        NOT NULL,
    "TransactionDate"   timestamptz NOT NULL,
    "OrderDate"         date        NOT NULL,
    "TotalBill"         bigint      NOT NULL,
    "TotalDiscount"     bigint      NOT NULL,
    "TotalPayment"      bigint      NOT NULL,
    "VoucherSeriesCode" text,
    "Status"            text        NOT NULL,
    "CreatedDate"       timestamptz NOT NULL,
    "ModifiedDate"      timestamptz NOT NULL,
    CONSTRAINT "pk_order" PRIMARY KEY ("Id"),
    CONSTRAINT "ck_order_status" CHECK ("Status" IN ('Completed', 'PartiallyReturned', 'Returned')),
    CONSTRAINT "ck_order_money" CHECK ("TotalDiscount" >= 0 AND "TotalPayment" >= 0
        AND "TotalBill" - "TotalDiscount" = "TotalPayment"),
    CONSTRAINT "ck_order_version" CHECK ("ModifiedDate" >= "CreatedDate")
);

CREATE TABLE IF NOT EXISTS "order"."OrderItem"
(
    "Id"             text        NOT NULL,
    "OrderId"        text        NOT NULL,
    "LineNo"         integer     NOT NULL,
    "ProductId"      text        NOT NULL,
    "Quantity"       integer     NOT NULL,
    "UnitPrice"      bigint      NOT NULL,
    -- Voucher share allocated to this line / Phần voucher phân bổ cho dòng
    "DiscountAmount" bigint      NOT NULL DEFAULT 0,
    "LineAmount"     bigint      NOT NULL,
    "IsGift"         boolean     NOT NULL DEFAULT false,
    -- Cost at sale time, drives gross profit / Giá vốn lúc bán, dùng tính lãi gộp
    "UnitCost"       bigint      NOT NULL,
    "OrderDate"      date        NOT NULL,
    "CreatedDate"    timestamptz NOT NULL,
    "ModifiedDate"   timestamptz NOT NULL,
    CONSTRAINT "pk_order_item" PRIMARY KEY ("Id"),
    CONSTRAINT "uq_order_item_line" UNIQUE ("OrderId", "LineNo"),
    CONSTRAINT "fk_order_item_order" FOREIGN KEY ("OrderId") REFERENCES "order"."Order" ("Id"),
    CONSTRAINT "ck_order_item_qty" CHECK ("Quantity" > 0 AND "LineNo" > 0),
    CONSTRAINT "ck_order_item_money" CHECK ("UnitPrice" >= 0 AND "UnitCost" >= 0
        AND "DiscountAmount" >= 0
        AND "LineAmount" = "Quantity" * "UnitPrice" - "DiscountAmount"
        AND "LineAmount" >= 0),
    CONSTRAINT "ck_order_item_gift" CHECK (NOT "IsGift" OR ("UnitPrice" = 0 AND "DiscountAmount" = 0))
);

CREATE TABLE IF NOT EXISTS "order"."Return"
(
    "Id"           text        NOT NULL,
    "OrderId"      text        NOT NULL,
    "ShopCode"     text        NOT NULL,
    "ReturnDate"   date        NOT NULL,
    "Reason"       text        NOT NULL,
    "ReturnAmount" bigint      NOT NULL,
    "CreatedDate"  timestamptz NOT NULL,
    "ModifiedDate" timestamptz NOT NULL,
    CONSTRAINT "pk_return" PRIMARY KEY ("Id"),
    CONSTRAINT "fk_return_order" FOREIGN KEY ("OrderId") REFERENCES "order"."Order" ("Id"),
    CONSTRAINT "ck_return_amount" CHECK ("ReturnAmount" >= 0)
);

CREATE TABLE IF NOT EXISTS "order"."ReturnItem"
(
    "Id"           text        NOT NULL,
    "ReturnId"     text        NOT NULL,
    "OrderId"      text        NOT NULL,
    "OrderItemId"  text        NOT NULL,
    "ProductId"    text        NOT NULL,
    "Quantity"     integer     NOT NULL,
    -- Back to sellable stock / Nhập lại kho bán được
    "RestockQty"   integer     NOT NULL,
    -- Written off as damaged / Huỷ do hỏng
    "DamageQty"    integer     NOT NULL,
    "UnitPrice"    bigint      NOT NULL,
    "UnitCost"     bigint      NOT NULL,
    "LineAmount"   bigint      NOT NULL,
    "ReturnDate"   date        NOT NULL,
    "CreatedDate"  timestamptz NOT NULL,
    "ModifiedDate" timestamptz NOT NULL,
    CONSTRAINT "pk_return_item" PRIMARY KEY ("Id"),
    CONSTRAINT "fk_return_item_return" FOREIGN KEY ("ReturnId") REFERENCES "order"."Return" ("Id"),
    CONSTRAINT "fk_return_item_order_item" FOREIGN KEY ("OrderItemId") REFERENCES "order"."OrderItem" ("Id"),
    CONSTRAINT "ck_return_item_qty" CHECK ("Quantity" > 0 AND "RestockQty" >= 0 AND "DamageQty" >= 0
        AND "RestockQty" + "DamageQty" = "Quantity"),
    CONSTRAINT "ck_return_item_money" CHECK ("UnitPrice" >= 0 AND "UnitCost" >= 0 AND "LineAmount" >= 0)
);

-- Stock-out requests logged at the counter / Yêu cầu mua khi hết hàng, ghi tại quầy
CREATE TABLE IF NOT EXISTS "order"."LostSale"
(
    "Id"                  text        NOT NULL,
    "BusinessDate"        date        NOT NULL,
    "ShopCode"            text        NOT NULL,
    "ProductId"           text        NOT NULL,
    "RequestedQty"        integer     NOT NULL,
    "SubstitutedQty"      integer     NOT NULL DEFAULT 0,
    "SubstituteProductId" text,
    "Reason"              text        NOT NULL DEFAULT 'OUT_OF_STOCK',
    "CreatedDate"         timestamptz NOT NULL,
    "ModifiedDate"        timestamptz NOT NULL,
    CONSTRAINT "pk_lost_sale" PRIMARY KEY ("Id"),
    CONSTRAINT "ck_lost_sale_qty" CHECK ("RequestedQty" > 0
        AND "SubstitutedQty" BETWEEN 0 AND "RequestedQty"),
    -- A substitute product is named exactly when something was substituted
    -- Có sản phẩm thay thế khi và chỉ khi có số lượng thay thế
    CONSTRAINT "ck_lost_sale_substitute" CHECK (("SubstitutedQty" = 0) = ("SubstituteProductId" IS NULL)),
    CONSTRAINT "ck_lost_sale_reason" CHECK ("Reason" IN ('OUT_OF_STOCK'))
);

-- Incremental ingest watermark / Watermark cho ingest tăng dần
CREATE INDEX IF NOT EXISTS "ix_order_modified" ON "order"."Order" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_order_item_modified" ON "order"."OrderItem" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_return_modified" ON "order"."Return" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_return_item_modified" ON "order"."ReturnItem" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_lost_sale_modified" ON "order"."LostSale" ("ModifiedDate");

-- Business-date lookups per shop / Tra cứu theo ngày nghiệp vụ và shop
CREATE INDEX IF NOT EXISTS "ix_order_date_shop" ON "order"."Order" ("OrderDate", "ShopCode");
CREATE INDEX IF NOT EXISTS "ix_return_date_shop" ON "order"."Return" ("ReturnDate", "ShopCode");
CREATE INDEX IF NOT EXISTS "ix_lost_sale_date_shop" ON "order"."LostSale" ("BusinessDate", "ShopCode");

-- Join and lookup keys / Khoá join và tra cứu
CREATE INDEX IF NOT EXISTS "ix_order_customer" ON "order"."Order" ("CustomerId");
CREATE INDEX IF NOT EXISTS "ix_order_item_product" ON "order"."OrderItem" ("ProductId");
CREATE INDEX IF NOT EXISTS "ix_return_order" ON "order"."Return" ("OrderId");
CREATE INDEX IF NOT EXISTS "ix_return_item_return" ON "order"."ReturnItem" ("ReturnId");
CREATE INDEX IF NOT EXISTS "ix_return_item_order_item" ON "order"."ReturnItem" ("OrderItemId");
CREATE INDEX IF NOT EXISTS "ix_return_item_product" ON "order"."ReturnItem" ("ProductId");

COMMIT;
