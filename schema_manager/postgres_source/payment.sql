-- Schema DDL for payment schema
CREATE SCHEMA IF NOT EXISTS payment;

CREATE TABLE IF NOT EXISTS payment."Payment"
(
    "Id" text NOT NULL,
    "PaymentType" smallint NOT NULL,
    "Status" text,
    "TotalPayment" integer NOT NULL,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Id")
);

CREATE TABLE IF NOT EXISTS payment."PaymentType"
(
    "Type" smallint NOT NULL,
    "Name" text NOT NULL,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Type")
);

CREATE TABLE IF NOT EXISTS payment."OrderPayment"
(
    "Id" text NOT NULL,
    "PaymentId" text NOT NULL,
    "OrderId" text NOT NULL,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Id")
);

CREATE TABLE IF NOT EXISTS payment."PaymentMethod"
(
    "Id" smallint NOT NULL,
    "Name" text NOT NULL,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Id")
);

CREATE TABLE IF NOT EXISTS payment."Transaction"
(
    "Id" text NOT NULL,
    "PaymentId" text NOT NULL,
    "PaymentMethodId" smallint NOT NULL,
    "Amount" integer NOT NULL DEFAULT 0,
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Id")
);

CREATE TABLE IF NOT EXISTS payment."ReturnPayment"
(
    "Id" text NOT NULL,
    "PaymentId" text NOT NULL,
    "ReturnId" text NOT NULL,
    "OrderId" text NOT NULL,   
    "CreatedDate" timestamp(6) with time zone NOT NULL,
    "ModifiedDate" timestamp(6) with time zone NOT NULL,
    PRIMARY KEY ("Id")
);

-- Foreign Key Constraints
ALTER TABLE IF EXISTS payment."Payment"
    ADD CONSTRAINT "fk_paymentType" FOREIGN KEY ("PaymentType")
    REFERENCES payment."PaymentType" ("Type") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS payment."OrderPayment"
    ADD CONSTRAINT fk_payment FOREIGN KEY ("PaymentId")
    REFERENCES payment."Payment" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS payment."OrderPayment"
    ADD CONSTRAINT fk_order FOREIGN KEY ("OrderId")
    REFERENCES "order"."Order" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS payment."Transaction"
    ADD CONSTRAINT fk_payment FOREIGN KEY ("PaymentId")
    REFERENCES payment."Payment" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS payment."Transaction"
    ADD CONSTRAINT "fk_paymentMethod" FOREIGN KEY ("PaymentMethodId")
    REFERENCES payment."PaymentMethod" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS payment."ReturnPayment"
    ADD CONSTRAINT fk_return FOREIGN KEY ("ReturnId", "OrderId")
    REFERENCES "order"."Return" ("Id", "OrderId") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

ALTER TABLE IF EXISTS payment."ReturnPayment"
    ADD CONSTRAINT fk_payment FOREIGN KEY ("PaymentId")
    REFERENCES payment."Payment" ("Id") MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_transaction_created_date ON payment."Transaction" ("CreatedDate" DESC);
CREATE INDEX IF NOT EXISTS idx_orderpayment_payid ON payment."OrderPayment" ("PaymentId");
CREATE INDEX IF NOT EXISTS idx_returnpayment_payid ON payment."ReturnPayment" ("PaymentId");
CREATE INDEX IF NOT EXISTS idx_transaction_payid ON payment."Transaction" ("PaymentId");
