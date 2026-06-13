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
