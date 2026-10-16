-- Schema "procurement" v2 (supply_chain_forecast_optimization.md 5.2, data_model_v2.md 3.3).
-- Schema "procurement" v2 (supply_chain_forecast_optimization.md 5.2, data_model_v2.md 3.3).
-- Suppliers are master data; purchase orders are written by the day-close simulator (Phase 1).
-- Nhà cung cấp là master; đơn mua do bộ mô phỏng chốt sổ ghi (Phase 1).
-- Money is bigint VND, times are timestamptz, business dates are ICT dates.
-- Tiền là bigint VND, thời gian là timestamptz, ngày nghiệp vụ là ngày giờ VN.

BEGIN;

CREATE SCHEMA IF NOT EXISTS procurement;

CREATE TABLE IF NOT EXISTS procurement."Supplier"
(
    "Id"               text         NOT NULL,
    "Name"             text         NOT NULL,
    "LeadTimeMeanDays" numeric(5,2) NOT NULL,
    "LeadTimeStdDays"  numeric(5,2) NOT NULL,
    -- Share of ordered units actually delivered / Tỷ lệ số lượng giao được trên số đặt
    "FillRate"         numeric(4,3) NOT NULL,
    "MinOrderValue"    bigint       NOT NULL DEFAULT 0,
    "CreatedDate"      timestamptz  NOT NULL,
    "ModifiedDate"     timestamptz  NOT NULL,
    CONSTRAINT "pk_supplier" PRIMARY KEY ("Id"),
    CONSTRAINT "ck_supplier_lead_time" CHECK ("LeadTimeMeanDays" > 0 AND "LeadTimeStdDays" >= 0),
    CONSTRAINT "ck_supplier_fill_rate" CHECK ("FillRate" > 0 AND "FillRate" <= 1),
    CONSTRAINT "ck_supplier_moq" CHECK ("MinOrderValue" >= 0)
);

CREATE TABLE IF NOT EXISTS procurement."SupplierProduct"
(
    "SupplierId"    text        NOT NULL,
    "ProductId"     text        NOT NULL,
    "UnitCost"      bigint      NOT NULL,
    -- Units per case; orders are rounded up to whole cases / Số đơn vị mỗi thùng; đặt hàng làm tròn lên theo thùng
    "CasePack"      integer     NOT NULL DEFAULT 1,
    "MinOrderQty"   integer     NOT NULL DEFAULT 0,
    "IsPrimary"     boolean     NOT NULL DEFAULT true,
    "EffectiveFrom" date        NOT NULL,
    "EffectiveTo"   date,
    "CreatedDate"   timestamptz NOT NULL,
    "ModifiedDate"  timestamptz NOT NULL,
    CONSTRAINT "pk_supplier_product" PRIMARY KEY ("SupplierId", "ProductId"),
    CONSTRAINT "ck_supplier_product_qty" CHECK ("UnitCost" >= 0 AND "CasePack" > 0 AND "MinOrderQty" >= 0),
    CONSTRAINT "ck_supplier_product_period" CHECK ("EffectiveTo" IS NULL OR "EffectiveTo" >= "EffectiveFrom")
);

CREATE TABLE IF NOT EXISTS procurement."PurchaseOrder"
(
    "Id"           text        NOT NULL,
    "SupplierId"   text        NOT NULL,
    "WarehouseId"  text        NOT NULL,
    "Status"       text        NOT NULL,
    "OrderDate"    timestamptz NOT NULL,
    "ExpectedDate" date        NOT NULL,
    "ReceivedDate" timestamptz,
    "TotalAmount"  bigint      NOT NULL,
    "CreatedDate"  timestamptz NOT NULL,
    "ModifiedDate" timestamptz NOT NULL,
    CONSTRAINT "pk_purchase_order" PRIMARY KEY ("Id"),
    CONSTRAINT "ck_purchase_order_status"
        CHECK ("Status" IN ('CREATED', 'CONFIRMED', 'PARTIAL', 'RECEIVED', 'CANCELLED')),
    CONSTRAINT "ck_purchase_order_amount" CHECK ("TotalAmount" >= 0),
    -- Received orders carry a receipt time / Đơn đã nhận phải có giờ nhận
    CONSTRAINT "ck_purchase_order_received"
        CHECK ("Status" NOT IN ('PARTIAL', 'RECEIVED') OR "ReceivedDate" IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS procurement."PurchaseOrderItem"
(
    "Id"              text        NOT NULL,
    "PurchaseOrderId" text        NOT NULL,
    "ProductId"       text        NOT NULL,
    "OrderedQty"      integer     NOT NULL,
    "ReceivedQty"     integer     NOT NULL DEFAULT 0,
    "UnitCost"        bigint      NOT NULL,
    "CreatedDate"     timestamptz NOT NULL,
    "ModifiedDate"    timestamptz NOT NULL,
    CONSTRAINT "pk_purchase_order_item" PRIMARY KEY ("Id"),
    CONSTRAINT "fk_purchase_order_item_order" FOREIGN KEY ("PurchaseOrderId")
        REFERENCES procurement."PurchaseOrder" ("Id"),
    CONSTRAINT "ck_purchase_order_item_qty" CHECK ("OrderedQty" > 0 AND "ReceivedQty" >= 0 AND "UnitCost" >= 0)
);

-- Incremental ingest watermark / Watermark cho ingest tăng dần
CREATE INDEX IF NOT EXISTS "ix_supplier_modified" ON procurement."Supplier" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_supplier_product_modified" ON procurement."SupplierProduct" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_purchase_order_modified" ON procurement."PurchaseOrder" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_purchase_order_item_modified" ON procurement."PurchaseOrderItem" ("ModifiedDate");

-- Join and lookup keys / Khoá join và tra cứu
CREATE INDEX IF NOT EXISTS "ix_supplier_product_product" ON procurement."SupplierProduct" ("ProductId");
CREATE INDEX IF NOT EXISTS "ix_purchase_order_supplier" ON procurement."PurchaseOrder" ("SupplierId", "OrderDate");
CREATE INDEX IF NOT EXISTS "ix_purchase_order_expected" ON procurement."PurchaseOrder" ("ExpectedDate", "WarehouseId");
CREATE INDEX IF NOT EXISTS "ix_purchase_order_item_order" ON procurement."PurchaseOrderItem" ("PurchaseOrderId");
CREATE INDEX IF NOT EXISTS "ix_purchase_order_item_product" ON procurement."PurchaseOrderItem" ("ProductId");

COMMIT;
