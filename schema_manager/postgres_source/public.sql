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
