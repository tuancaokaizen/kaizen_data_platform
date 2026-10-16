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

-- v2 logistics attributes (data_model_v2.md 3.2); CategoryId comes with the demand model.
-- Thuộc tính logistics v2 (data_model_v2.md 3.2); CategoryId đi cùng mô hình nhu cầu.
ALTER TABLE product."Product"
    ADD COLUMN IF NOT EXISTS "SupplierId"       text,
    ADD COLUMN IF NOT EXISTS "UnitVolumeCm3"    integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "UnitWeightG"      integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "CasePack"         integer NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS "ShelfLifeDays"    integer,
    ADD COLUMN IF NOT EXISTS "IsColdChain"      boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS "LaunchDate"       date,
    ADD COLUMN IF NOT EXISTS "DiscontinuedDate" date;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_product_logistics') THEN
        ALTER TABLE product."Product" ADD CONSTRAINT "ck_product_logistics" CHECK ("UnitVolumeCm3" >= 0
            AND "UnitWeightG" >= 0 AND "CasePack" > 0 AND ("ShelfLifeDays" IS NULL OR "ShelfLifeDays" > 0));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS "ix_product_modified" ON product."Product" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_product_supplier" ON product."Product" ("SupplierId");
