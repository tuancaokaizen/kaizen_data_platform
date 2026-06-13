-- Schema DDL for promotion schema
CREATE SCHEMA IF NOT EXISTS promotion;

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

-- Foreign Key Constraints
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

-- Indexes
CREATE INDEX IF NOT EXISTS idx_promotion_date ON promotion."Promotion" ("StartDate", "EndDate");
CREATE INDEX IF NOT EXISTS idx_voucher_date ON promotion."Voucher" ("StartDate", "EndDate");
