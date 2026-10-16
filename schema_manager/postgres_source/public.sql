-- Schema DDL for public schema
CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public."Geography"
(
    "Id" text NOT NULL,
    "Name" text NOT NULL,
    "Population" integer,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Id")
);

-- v2 additions (data_model_v2.md 3.6), filled from schema_manager/reference/geography_hcm_2025.csv.
-- Bổ sung v2 (data_model_v2.md 3.6), nạp từ schema_manager/reference/geography_hcm_2025.csv.
ALTER TABLE public."Geography"
    ADD COLUMN IF NOT EXISTS "AdminCode"        text,
    ADD COLUMN IF NOT EXISTS "WardType"         text,
    ADD COLUMN IF NOT EXISTS "ProvinceCode"     text,
    ADD COLUMN IF NOT EXISTS "LegacyProvince"   text,
    ADD COLUMN IF NOT EXISTS "SubRegion"        text,
    ADD COLUMN IF NOT EXISTS "Latitude"         numeric(9,6),
    ADD COLUMN IF NOT EXISTS "Longitude"        numeric(9,6),
    ADD COLUMN IF NOT EXISTS "AreaKm2"          numeric(10,2),
    ADD COLUMN IF NOT EXISTS "IsRoadAccessible" boolean,
    ADD COLUMN IF NOT EXISTS "CoordMethod"      text,
    ADD COLUMN IF NOT EXISTS "CoordSource"      text;

DO $$
BEGIN
    -- Coarse box: catches swapped lat/lon or a wrong decimal, not "inside Vietnam"
    -- Khung thô: bắt lỗi đảo lat/lon hoặc sai thập phân, không đảm bảo "nằm trong Việt Nam"
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_geography_coord') THEN
        ALTER TABLE public."Geography" ADD CONSTRAINT "ck_geography_coord" CHECK (
            ("Latitude" IS NULL OR "Latitude" BETWEEN 8.5 AND 11.6)
            AND ("Longitude" IS NULL OR "Longitude" BETWEEN 106.0 AND 107.7));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_geography_ward_type') THEN
        ALTER TABLE public."Geography" ADD CONSTRAINT "ck_geography_ward_type"
            CHECK ("WardType" IS NULL OR "WardType" IN ('PHUONG', 'XA', 'DAC_KHU'));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_geography_sub_region') THEN
        ALTER TABLE public."Geography" ADD CONSTRAINT "ck_geography_sub_region" CHECK ("SubRegion" IS NULL
            OR "SubRegion" IN ('SG_CORE', 'SG_OUTER', 'BINH_DUONG', 'BA_RIA_VUNG_TAU', 'CON_DAO'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS "ix_geography_modified" ON public."Geography" ("ModifiedDate");
