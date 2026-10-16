-- Unified DDL for the master schemas. "order" and "payment" are v2 and live in
-- order.sql / payment.sql (run those after this file).
-- DDL gộp cho các schema master. "order" và "payment" đã lên v2, nằm ở
-- order.sql / payment.sql (chạy sau file này).
BEGIN;

-- =========================================================
-- 0. TẠO CÁC SCHEMA (CREATE SCHEMAS)
-- =========================================================
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS shop;
CREATE SCHEMA IF NOT EXISTS customer;
CREATE SCHEMA IF NOT EXISTS product;
CREATE SCHEMA IF NOT EXISTS promotion;

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


-- =========================================================
-- 3. HỆ THỐNG CHỈ MỤC TỐI ƯU HÓA TRUY VẤN (INDEXES)
-- =========================================================

-- 3.1 Index cho bộ lọc Khoảng thời gian (DateTime Range Queries)
CREATE INDEX IF NOT EXISTS idx_promotion_date ON promotion."Promotion" ("StartDate", "EndDate");
CREATE INDEX IF NOT EXISTS idx_voucher_date ON promotion."Voucher" ("StartDate", "EndDate");

-- 3.2 Index cho các bộ lọc Tìm kiếm thông tin nhanh
CREATE INDEX IF NOT EXISTS idx_customer_phone ON customer."Customer" ("Phone");
CREATE INDEX IF NOT EXISTS idx_employee_phone ON shop."Employee" ("Phone");
CREATE INDEX IF NOT EXISTS idx_product_name ON product."Product" ("Name");

-- 3.3 Composite Index cho các bộ lọc Tìm kiếm nâng cao kết hợp điều kiện
CREATE INDEX IF NOT EXISTS idx_salary_emp_month ON shop."EmployeeSalary" ("EmployeeId", "SalaryMonth");

-- 3.4 Index cho Khóa ngoại (Tối ưu hiệu năng các câu lệnh JOIN bảng)
CREATE INDEX IF NOT EXISTS idx_shop_ward ON shop."Shop" ("WardId");
CREATE INDEX IF NOT EXISTS idx_customer_ward ON customer."Customer" ("WardId");

COMMIT;
