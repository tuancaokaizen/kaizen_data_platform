-- Schema DDL for order schema
CREATE SCHEMA IF NOT EXISTS "order";

CREATE TABLE IF NOT EXISTS "order"."Order"
(
    "Id" text NOT NULL,
    "CustomerId" text NOT NULL,
    "ShopCode" text,
    "TransactionDate" timestamp(6) with time zone,
    "TotalBill" integer,
    "TotalDiscount" integer,
    "TotalPayment" integer,
    "EmployeeSaleId" text NOT NULL,
    "Status" text,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    "VoucherSeriesCode" text,
    PRIMARY KEY ("Id")
);

CREATE TABLE IF NOT EXISTS "order"."OrderItem"
(
    "Id" text NOT NULL,
    "OrderId" text NOT NULL,
    "ProductId" text,
    "Quantity" integer,
    "UnitPrice" integer,
    "CreatedDate" timestamp(6) with time zone,
    "ModifiedDate" timestamp(6) with time zone,
    PRIMARY KEY ("Id", "OrderId")
);

CREATE TABLE IF NOT EXISTS "order"."Return"
(
    "Id" text NOT NULL,
    "OrderId" text NOT NULL,
    "ReturnAmount" integer,
    "CreatedDate" timestamp(6) with time zone,
    "ModifiedDate" time(6) without time zone,
    PRIMARY KEY ("Id", "OrderId")
);

CREATE TABLE IF NOT EXISTS "order"."ReturnItem"
(
    "Id" text NOT NULL,
    "ReturnId" text NOT NULL,
    "ProductId" text,
    "Quantity" integer NOT NULL,
    "UnitPrice" integer,
    "OrderItemId" text,      
    "OrderId" text,           
    "TotalAmount" integer,
    "CreatedDate" timestamp(6) with time zone,
    "ModifiedDate" timestamp(6) with time zone,
    PRIMARY KEY ("Id")
);

-- Foreign Key Constraints
ALTER TABLE IF EXISTS "order"."Order"
    ADD CONSTRAINT "fk_customerId" FOREIGN KEY ("CustomerId")
    REFERENCES customer."Customer" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS "order"."Order"
    ADD CONSTRAINT fk_shop FOREIGN KEY ("ShopCode")
    REFERENCES shop."Shop" ("Code") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS "order"."Order"
    ADD CONSTRAINT fk_employee FOREIGN KEY ("EmployeeSaleId")
    REFERENCES shop."Employee" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS "order"."Order"
    ADD CONSTRAINT "fk_voucherSeriesCode" FOREIGN KEY ("VoucherSeriesCode")
    REFERENCES promotion."VoucherSeries" ("SeriesCode") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS "order"."OrderItem"
    ADD CONSTRAINT fk_product FOREIGN KEY ("ProductId")
    REFERENCES product."Product" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS "order"."OrderItem"
    ADD CONSTRAINT fk_order FOREIGN KEY ("OrderId")
    REFERENCES "order"."Order" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS "order"."Return"
    ADD CONSTRAINT fk_order FOREIGN KEY ("OrderId")
    REFERENCES "order"."Order" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS "order"."ReturnItem"
    ADD CONSTRAINT "fk_orderItem" FOREIGN KEY ("OrderItemId", "OrderId")
    REFERENCES "order"."OrderItem" ("Id", "OrderId") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS "order"."ReturnItem"
    ADD CONSTRAINT fk_product FOREIGN KEY ("ProductId")
    REFERENCES product."Product" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS "order"."ReturnItem"
    ADD CONSTRAINT fk_return FOREIGN KEY ("ReturnId", "OrderId")
    REFERENCES "order"."Return" ("Id", "OrderId") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_order_tx_date ON "order"."Order" ("TransactionDate" DESC);
CREATE INDEX IF NOT EXISTS idx_return_created_date ON "order"."Return" ("CreatedDate" DESC);
CREATE INDEX IF NOT EXISTS idx_order_shop_status_date ON "order"."Order" ("ShopCode", "Status", "TransactionDate" DESC);
CREATE INDEX IF NOT EXISTS idx_return_order_date ON "order"."Return" ("OrderId", "CreatedDate" DESC);
CREATE INDEX IF NOT EXISTS idx_order_customer ON "order"."Order" ("CustomerId");
CREATE INDEX IF NOT EXISTS idx_order_employee ON "order"."Order" ("EmployeeSaleId");
CREATE INDEX IF NOT EXISTS idx_orderitem_product ON "order"."OrderItem" ("ProductId");
CREATE INDEX IF NOT EXISTS idx_returnitem_product ON "order"."ReturnItem" ("ProductId");
