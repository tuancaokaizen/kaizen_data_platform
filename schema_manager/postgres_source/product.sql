-- Schema DDL for product schema
CREATE SCHEMA IF NOT EXISTS product;

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

-- Foreign Key Constraints
ALTER TABLE IF EXISTS product."ProductSupplement"
    ADD CONSTRAINT "fk_productId" FOREIGN KEY ("ProductId")
    REFERENCES product."Product" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS product."ProductMedicine"
    ADD CONSTRAINT "fk_productId" FOREIGN KEY ("ProductId")
    REFERENCES product."Product" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_product_name ON product."Product" ("Name");
