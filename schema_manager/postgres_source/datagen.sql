-- Schema "datagen" (data_model_v2.md 3.4): parameters and run log of the data generator (dags/jobs/kaizen_sim).
-- Never ingested into the lakehouse; nothing a real pharmacy chain could observe lives here.
-- Schema "datagen" (data_model_v2.md 3.4): tham số và nhật ký chạy của bộ sinh dữ liệu (dags/jobs/kaizen_sim).
-- Không bao giờ ingest vào lakehouse; ở đây không có dữ liệu nào chuỗi nhà thuốc thật quan sát được.

BEGIN;

CREATE SCHEMA IF NOT EXISTS datagen;

-- Seed and parameters of one generator setup / Seed và tham số của một cấu hình bộ sinh
CREATE TABLE IF NOT EXISTS datagen."GeneratorConfig"
(
    "Id"          text        NOT NULL,
    "Name"        text        NOT NULL,
    "Seed"        bigint      NOT NULL,
    "StartDate"   date        NOT NULL,
    "Params"      jsonb       NOT NULL,
    "CreatedDate" timestamptz NOT NULL,
    CONSTRAINT "pk_generator_config" PRIMARY KEY ("Id")
);

-- One row per run; a DAILY_CLOSE row with Status SUCCESS marks the business day as closed
-- Mỗi lần chạy một dòng; dòng DAILY_CLOSE trạng thái SUCCESS đánh dấu ngày nghiệp vụ đã chốt
CREATE TABLE IF NOT EXISTS datagen."GeneratorRun"
(
    "RunId"        text        NOT NULL,
    "Mode"         text        NOT NULL,
    "BusinessDate" date        NOT NULL,
    "Status"       text        NOT NULL,
    "StartedAt"    timestamptz NOT NULL,
    "FinishedAt"   timestamptz,
    "Stats"        jsonb,
    CONSTRAINT "pk_generator_run" PRIMARY KEY ("RunId"),
    CONSTRAINT "ck_generator_run_mode"
        CHECK ("Mode" IN ('MASTER_SUPPLY', 'REBASELINE', 'DAILY_CLOSE', 'TICK', 'BACKFILL')),
    CONSTRAINT "ck_generator_run_status" CHECK ("Status" IN ('RUNNING', 'SUCCESS', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS "ix_generator_run_mode" ON datagen."GeneratorRun" ("Mode", "BusinessDate");

COMMIT;
