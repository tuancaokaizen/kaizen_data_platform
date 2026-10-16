-- Schema DDL for shop schema
CREATE SCHEMA IF NOT EXISTS shop;

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

-- Foreign Key Constraints
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

-- Indexes
CREATE INDEX IF NOT EXISTS idx_employee_phone ON shop."Employee" ("Phone");
CREATE INDEX IF NOT EXISTS idx_salary_emp_month ON shop."EmployeeSalary" ("EmployeeId", "SalaryMonth");
CREATE INDEX IF NOT EXISTS idx_shop_ward ON shop."Shop" ("WardId");

-- v2 additions (data_model_v2.md 3.2, 3.6); the point lies inside the polygon of WardId.
-- Bổ sung v2 (data_model_v2.md 3.2, 3.6); toạ độ nằm trong ranh giới phường WardId.
ALTER TABLE shop."Shop"
    ADD COLUMN IF NOT EXISTS "Tier"               char(1),
    ADD COLUMN IF NOT EXISTS "OpenDate"           date,
    ADD COLUMN IF NOT EXISTS "CloseDate"          date,
    ADD COLUMN IF NOT EXISTS "ServingWarehouseId" text,
    ADD COLUMN IF NOT EXISTS "Latitude"           numeric(9,6),
    ADD COLUMN IF NOT EXISTS "Longitude"          numeric(9,6);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_shop_tier') THEN
        ALTER TABLE shop."Shop" ADD CONSTRAINT "ck_shop_tier" CHECK ("Tier" IS NULL OR "Tier" IN ('A', 'B', 'C'));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_shop_coord') THEN
        ALTER TABLE shop."Shop" ADD CONSTRAINT "ck_shop_coord" CHECK (
            ("Latitude" IS NULL OR "Latitude" BETWEEN 8.5 AND 11.6)
            AND ("Longitude" IS NULL OR "Longitude" BETWEEN 106.0 AND 107.7));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_shop_period') THEN
        ALTER TABLE shop."Shop" ADD CONSTRAINT "ck_shop_period"
            CHECK ("CloseDate" IS NULL OR "CloseDate" >= "OpenDate");
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS "ix_shop_modified" ON shop."Shop" ("ModifiedDate");

-- Storage and receiving capacity, one version per period (SCD2 source)
-- Sức chứa và năng lực nhận hàng, mỗi giai đoạn một phiên bản (nguồn SCD2)
CREATE TABLE IF NOT EXISTS shop."ShopCapacity"
(
    "Id"                   text         NOT NULL,
    "ShopCode"             text         NOT NULL,
    "StorageM3"            numeric(8,2) NOT NULL,
    "ColdStorageL"         integer      NOT NULL,
    "ShelfSlots"           integer      NOT NULL,
    "ReceivingSlotsPerDay" smallint     NOT NULL,
    "StaffCount"           smallint     NOT NULL,
    "EffectiveFrom"        date         NOT NULL,
    "EffectiveTo"          date,
    "CreatedDate"          timestamptz  NOT NULL,
    "ModifiedDate"         timestamptz  NOT NULL,
    CONSTRAINT "pk_shop_capacity" PRIMARY KEY ("Id"),
    CONSTRAINT "ck_shop_capacity_values" CHECK ("StorageM3" > 0 AND "ColdStorageL" >= 0 AND "ShelfSlots" > 0
        AND "ReceivingSlotsPerDay" > 0 AND "StaffCount" >= 0),
    CONSTRAINT "ck_shop_capacity_period" CHECK ("EffectiveTo" IS NULL OR "EffectiveTo" >= "EffectiveFrom")
);

CREATE INDEX IF NOT EXISTS "ix_shop_capacity_shop" ON shop."ShopCapacity" ("ShopCode", "EffectiveFrom");
CREATE INDEX IF NOT EXISTS "ix_shop_capacity_modified" ON shop."ShopCapacity" ("ModifiedDate");
