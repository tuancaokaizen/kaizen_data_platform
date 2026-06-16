BEGIN;

-- =========================================================
-- 0. TẠO CÁC SCHEMA (CREATE SCHEMAS)
-- =========================================================
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS shop;
CREATE SCHEMA IF NOT EXISTS customer;
CREATE SCHEMA IF NOT EXISTS product;
CREATE SCHEMA IF NOT EXISTS promotion;
CREATE SCHEMA IF NOT EXISTS "order";
CREATE SCHEMA IF NOT EXISTS payment;

-- =========================================================
-- 1. TẠO CẤU TRÚC CÁC BẢNG (TABLE SCHEMAS)
-- =========================================================

-- SCHEMA SHOP
CREATE TABLE IF NOT EXISTS shop."Shop"
(
    "Code" text NOT NULL,
    "Name" text NOT NULL,
    "Address" text NOT NULL,
    "WardId" text,
    "ManagerId" text,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Code")
);

CREATE TABLE IF NOT EXISTS shop."Employee"
(
    "Id" text NOT NULL,
    "Name" text NOT NULL,
    "Degree" text NOT NULL,
    "Phone" text NOT NULL,
    "Address" text,
    "Status" boolean NOT NULL,
    "ShopCode" text NOT NULL,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    "IsManager" boolean NOT NULL,
    PRIMARY KEY ("Id")
);

CREATE TABLE IF NOT EXISTS shop."EmployeeSalary"
(
    "Id" text NOT NULL,
    "EmployeeId" text NOT NULL,
    "ShopCode" text NOT NULL,
    "SalaryMonth" timestamp(6) with time zone NOT NULL,
    "BaseSalary" bigint NOT NULL,
    "CommissionRate" integer,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Id")
);

-- SCHEMA CUSTOMER
CREATE TABLE IF NOT EXISTS customer."Customer"
(
    "Id" text NOT NULL,
    "FullName" text NOT NULL,
    "Phone" text NOT NULL,
    "WardId" text,
    "Address" text,
    "YearOfBirth" integer,
    "Gender" text,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Id")
);

-- SCHEMA PUBLIC
CREATE TABLE IF NOT EXISTS public."Geography"
(
    "Id" text NOT NULL,
    "Name" text NOT NULL,
    "Population" integer,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Id")
);

-- SCHEMA PRODUCT
CREATE TABLE IF NOT EXISTS product."Product"
(
    "Id" text NOT NULL,
    "Name" text NOT NULL,
    "Type" text NOT NULL,
    "Unit" text,
    "StockPrice" integer NOT NULL DEFAULT 0,
    "RetailPrice" integer NOT NULL DEFAULT 0,
    "TaxRate" integer NOT NULL DEFAULT 0,
    "IsPromotion" boolean NOT NULL,
    "Status" boolean NOT NULL,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Id")
);

CREATE TABLE IF NOT EXISTS product."ProductSupplement"
(
    "ProductId" text NOT NULL,
    "PrimaryFunction" text NOT NULL,
    "Brand" text,
    PRIMARY KEY ("ProductId")
);

CREATE TABLE IF NOT EXISTS product."ProductMedicine"
(
    "ProductId" text NOT NULL,
    "Indication" text NOT NULL,
    "IsPrescription" boolean NOT NULL,
    "SpecialtyDisease" text,
    PRIMARY KEY ("ProductId")
);

-- SCHEMA PROMOTION
CREATE TABLE IF NOT EXISTS promotion."Voucher"
(
    "Code" text NOT NULL,
    "Name" text NOT NULL,
    "PromotionCode" text,
    "VoucherAmount" integer,
    "MinOrderValue" integer,
    "StartDate" timestamp(3) with time zone NOT NULL,
    "EndDate" timestamp(3) with time zone NOT NULL,
    "IssuedQuantity" integer NOT NULL DEFAULT 0,
    "Status" boolean NOT NULL,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Code")
);

CREATE TABLE IF NOT EXISTS promotion."VoucherSeries"
(
    "SeriesCode" text NOT NULL,
    "VoucherCode" text NOT NULL,
    "ExpiryDay" integer,
    "Status" text NOT NULL,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone,
    PRIMARY KEY ("SeriesCode")
);

CREATE TABLE IF NOT EXISTS promotion."Promotion"
(
    "Code" text NOT NULL,
    "Name" text NOT NULL,
    "StartDate" timestamp(3) with time zone NOT NULL,
    "EndDate" timestamp(3) with time zone NOT NULL,
    "Status" boolean NOT NULL,
    "Type" text,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Code")
);

CREATE TABLE IF NOT EXISTS promotion."Gift"
(
    "Id" text NOT NULL,
    "ProductId" text NOT NULL,
    "PromotionCode" text NOT NULL,
    "PromotionQuantity" integer,
    "Status" boolean,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Id")
);

-- SCHEMA ORDER
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
    "ModifiedDate" timestamp(6) with time zone,
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

-- SCHEMA PAYMENT
CREATE TABLE IF NOT EXISTS payment."Payment"
(
    "Id" text NOT NULL,
    "PaymentType" smallint NOT NULL,
    "Status" text,
    "TotalPayment" integer NOT NULL,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Id")
);

CREATE TABLE IF NOT EXISTS payment."PaymentType"
(
    "Type" smallint NOT NULL,
    "Name" text NOT NULL,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Type")
);

CREATE TABLE IF NOT EXISTS payment."OrderPayment"
(
    "Id" text NOT NULL,
    "PaymentId" text NOT NULL,
    "OrderId" text NOT NULL,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Id")
);

CREATE TABLE IF NOT EXISTS payment."PaymentMethod"
(
    "Id" smallint NOT NULL,
    "Name" text NOT NULL,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Id")
);

CREATE TABLE IF NOT EXISTS payment."Transaction"
(
    "Id" text NOT NULL,
    "PaymentId" text NOT NULL,
    "PaymentMethodId" smallint NOT NULL,
    "Amount" integer NOT NULL DEFAULT 0,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Id")
);

CREATE TABLE IF NOT EXISTS payment."ReturnPayment"
(
    "Id" text NOT NULL,
    "PaymentId" text NOT NULL,
    "ReturnId" text NOT NULL,
    "OrderId" text NOT NULL,   
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Id")
);


-- =========================================================
-- 2. ĐỊNH NGHĨA KHÓA NGOẠI (FOREIGN KEYS)
-- =========================================================

ALTER TABLE IF EXISTS shop."Shop"
    ADD CONSTRAINT "fk_wardId" FOREIGN KEY ("WardId")
    REFERENCES public."Geography" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS shop."Shop"
    ADD CONSTRAINT "fk_managerId" FOREIGN KEY ("ManagerId")
    REFERENCES shop."Employee" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS shop."Employee"
    ADD CONSTRAINT "fk_shopCode" FOREIGN KEY ("ShopCode")
    REFERENCES shop."Shop" ("Code") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS shop."EmployeeSalary"
    ADD CONSTRAINT "fk_employeeId" FOREIGN KEY ("EmployeeId")
    REFERENCES shop."Employee" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS customer."Customer"
    ADD CONSTRAINT "fk_wardId" FOREIGN KEY ("WardId")
    REFERENCES public."Geography" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS product."ProductSupplement"
    ADD CONSTRAINT "fk_productId" FOREIGN KEY ("ProductId")
    REFERENCES product."Product" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS product."ProductMedicine"
    ADD CONSTRAINT "fk_productId" FOREIGN KEY ("ProductId")
    REFERENCES product."Product" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS promotion."Voucher"
    ADD CONSTRAINT "fk_promotionCode" FOREIGN KEY ("PromotionCode")
    REFERENCES promotion."Promotion" ("Code") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS promotion."VoucherSeries"
    ADD CONSTRAINT "fk_voucherCode" FOREIGN KEY ("VoucherCode")
    REFERENCES promotion."Voucher" ("Code") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS promotion."Gift"
    ADD CONSTRAINT "fk_productId" FOREIGN KEY ("ProductId")
    REFERENCES product."Product" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS promotion."Gift"
    ADD CONSTRAINT "fk_promotionCode" FOREIGN KEY ("PromotionCode")
    REFERENCES promotion."Promotion" ("Code") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

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

ALTER TABLE IF EXISTS payment."Payment"
    ADD CONSTRAINT "fk_paymentType" FOREIGN KEY ("PaymentType")
    REFERENCES payment."PaymentType" ("Type") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS payment."OrderPayment"
    ADD CONSTRAINT fk_payment FOREIGN KEY ("PaymentId")
    REFERENCES payment."Payment" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS payment."OrderPayment"
    ADD CONSTRAINT fk_order FOREIGN KEY ("OrderId")
    REFERENCES "order"."Order" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS payment."Transaction"
    ADD CONSTRAINT fk_payment FOREIGN KEY ("PaymentId")
    REFERENCES payment."Payment" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS payment."Transaction"
    ADD CONSTRAINT "fk_paymentMethod" FOREIGN KEY ("PaymentMethodId")
    REFERENCES payment."PaymentMethod" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS payment."ReturnPayment"
    ADD CONSTRAINT fk_return FOREIGN KEY ("ReturnId", "OrderId")
    REFERENCES "order"."Return" ("Id", "OrderId") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS payment."ReturnPayment"
    ADD CONSTRAINT fk_payment FOREIGN KEY ("PaymentId")
    REFERENCES payment."Payment" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;


-- =========================================================
-- 3. HỆ THỐNG CHỈ MỤC TỐI ƯU HÓA TRUY VẤN (INDEXES)
-- =========================================================

-- 3.1 Index cho bộ lọc Khoảng thời gian (DateTime Range Queries)
CREATE INDEX IF NOT EXISTS idx_order_tx_date ON "order"."Order" ("TransactionDate" DESC);
CREATE INDEX IF NOT EXISTS idx_return_created_date ON "order"."Return" ("CreatedDate" DESC);
CREATE INDEX IF NOT EXISTS idx_transaction_created_date ON payment."Transaction" ("CreatedDate" DESC);
CREATE INDEX IF NOT EXISTS idx_promotion_date ON promotion."Promotion" ("StartDate", "EndDate");
CREATE INDEX IF NOT EXISTS idx_voucher_date ON promotion."Voucher" ("StartDate", "EndDate");

-- 3.2 Index cho các bộ lọc Tìm kiếm thông tin nhanh
CREATE INDEX IF NOT EXISTS idx_customer_phone ON customer."Customer" ("Phone");
CREATE INDEX IF NOT EXISTS idx_employee_phone ON shop."Employee" ("Phone");
CREATE INDEX IF NOT EXISTS idx_product_name ON product."Product" ("Name");

-- 3.3 Composite Index cho các bộ lọc Tìm kiếm nâng cao kết hợp điều kiện
CREATE INDEX IF NOT EXISTS idx_order_shop_status_date ON "order"."Order" ("ShopCode", "Status", "TransactionDate" DESC);
CREATE INDEX IF NOT EXISTS idx_return_order_date ON "order"."Return" ("OrderId", "CreatedDate" DESC);
CREATE INDEX IF NOT EXISTS idx_salary_emp_month ON shop."EmployeeSalary" ("EmployeeId", "SalaryMonth");

-- 3.4 Index cho Khóa ngoại (Tối ưu hiệu năng các câu lệnh JOIN bảng)
CREATE INDEX IF NOT EXISTS idx_order_customer ON "order"."Order" ("CustomerId");
CREATE INDEX IF NOT EXISTS idx_order_employee ON "order"."Order" ("EmployeeSaleId");
CREATE INDEX IF NOT EXISTS idx_orderitem_product ON "order"."OrderItem" ("ProductId");
CREATE INDEX IF NOT EXISTS idx_returnitem_product ON "order"."ReturnItem" ("ProductId");
CREATE INDEX IF NOT EXISTS idx_orderpayment_payid ON payment."OrderPayment" ("PaymentId");
CREATE INDEX IF NOT EXISTS idx_returnpayment_payid ON payment."ReturnPayment" ("PaymentId");
CREATE INDEX IF NOT EXISTS idx_transaction_payid ON payment."Transaction" ("PaymentId");
CREATE INDEX IF NOT EXISTS idx_shop_ward ON shop."Shop" ("WardId");
CREATE INDEX IF NOT EXISTS idx_customer_ward ON customer."Customer" ("WardId");

COMMIT;
