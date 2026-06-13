-- Schema DDL for customer schema
CREATE SCHEMA IF NOT EXISTS customer;

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

-- Foreign Key Constraints
ALTER TABLE IF EXISTS customer."Customer"
    ADD CONSTRAINT "fk_wardId" FOREIGN KEY ("WardId")
    REFERENCES public."Geography" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_customer_phone ON customer."Customer" ("Phone");
CREATE INDEX IF NOT EXISTS idx_customer_ward ON customer."Customer" ("WardId");
