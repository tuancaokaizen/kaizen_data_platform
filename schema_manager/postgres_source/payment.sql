-- Schema "payment" v2 (data_model_v2.md 3.2). Money is bigint VND, times are timestamptz.
-- Schema "payment" v2 (data_model_v2.md 3.2). Tiền là bigint VND, thời gian là timestamptz.
-- Written by SalesWriterJob from payment-bundles; one message becomes Payment + Transaction rows
-- plus one OrderPayment (RECEIPT) or ReturnPayment (REFUND) link with "Id" = "PaymentId".
-- Ghi bởi SalesWriterJob từ payment-bundles; mỗi message thành Payment + Transaction và một dòng
-- liên kết OrderPayment (phiếu thu) hoặc ReturnPayment (phiếu chi) có "Id" = "PaymentId".
-- No FK to "order": payments and orders come from different topics and may land in any order.
-- Không FK sang "order": phiếu và đơn đến từ hai topic khác nhau, có thể lệch thứ tự.

BEGIN;

CREATE SCHEMA IF NOT EXISTS payment;

CREATE TABLE IF NOT EXISTS payment."PaymentType"
(
    "Type"         smallint    NOT NULL,
    "Name"         text        NOT NULL,
    "CreatedDate"  timestamptz NOT NULL,
    "ModifiedDate" timestamptz NOT NULL,
    CONSTRAINT "pk_payment_type" PRIMARY KEY ("Type")
);

CREATE TABLE IF NOT EXISTS payment."PaymentMethod"
(
    "Id"           smallint    NOT NULL,
    "Name"         text        NOT NULL,
    "CreatedDate"  timestamptz NOT NULL,
    "ModifiedDate" timestamptz NOT NULL,
    CONSTRAINT "pk_payment_method" PRIMARY KEY ("Id")
);

CREATE TABLE IF NOT EXISTS payment."Payment"
(
    "Id"           text        NOT NULL,
    -- 1 = RECEIPT (thu), 2 = REFUND (chi)
    "PaymentType"  smallint    NOT NULL,
    "Status"       text        NOT NULL,
    -- Copied from the event so one-success-per-document can be indexed
    -- Chép từ event để đánh index "mỗi chứng từ đúng 1 phiếu thành công"
    "OrderId"      text        NOT NULL,
    "ReturnId"     text,
    "ShopCode"     text        NOT NULL,
    "TotalPayment" bigint      NOT NULL,
    "Attempt"      smallint    NOT NULL DEFAULT 1,
    "PaymentDate"  date        NOT NULL,
    "CreatedDate"  timestamptz NOT NULL,
    "ModifiedDate" timestamptz NOT NULL,
    CONSTRAINT "pk_payment" PRIMARY KEY ("Id"),
    CONSTRAINT "fk_payment_type" FOREIGN KEY ("PaymentType") REFERENCES payment."PaymentType" ("Type"),
    CONSTRAINT "ck_payment_status" CHECK ("Status" IN ('PENDING', 'SUCCESS', 'FAILED')),
    -- Receipts never carry a return, refunds always do / Phiếu thu không có phiếu trả, phiếu chi luôn có
    CONSTRAINT "ck_payment_return" CHECK (("PaymentType" = 2) = ("ReturnId" IS NOT NULL)),
    CONSTRAINT "ck_payment_amount" CHECK ("TotalPayment" >= 0 AND "Attempt" >= 1),
    CONSTRAINT "ck_payment_version" CHECK ("ModifiedDate" >= "CreatedDate")
);

CREATE TABLE IF NOT EXISTS payment."Transaction"
(
    "Id"              text        NOT NULL,
    "PaymentId"       text        NOT NULL,
    "PaymentMethodId" smallint    NOT NULL,
    "Amount"          bigint      NOT NULL,
    "PaymentDate"     date        NOT NULL,
    "CreatedDate"     timestamptz NOT NULL,
    "ModifiedDate"    timestamptz NOT NULL,
    CONSTRAINT "pk_transaction" PRIMARY KEY ("Id"),
    CONSTRAINT "fk_transaction_payment" FOREIGN KEY ("PaymentId") REFERENCES payment."Payment" ("Id"),
    CONSTRAINT "fk_transaction_method" FOREIGN KEY ("PaymentMethodId") REFERENCES payment."PaymentMethod" ("Id"),
    CONSTRAINT "ck_transaction_amount" CHECK ("Amount" > 0)
);

CREATE TABLE IF NOT EXISTS payment."OrderPayment"
(
    "Id"           text        NOT NULL,
    "PaymentId"    text        NOT NULL,
    "OrderId"      text        NOT NULL,
    "PaymentDate"  date        NOT NULL,
    "CreatedDate"  timestamptz NOT NULL,
    "ModifiedDate" timestamptz NOT NULL,
    CONSTRAINT "pk_order_payment" PRIMARY KEY ("Id"),
    CONSTRAINT "uq_order_payment_payment" UNIQUE ("PaymentId"),
    CONSTRAINT "fk_order_payment_payment" FOREIGN KEY ("PaymentId") REFERENCES payment."Payment" ("Id")
);

CREATE TABLE IF NOT EXISTS payment."ReturnPayment"
(
    "Id"           text        NOT NULL,
    "PaymentId"    text        NOT NULL,
    "ReturnId"     text        NOT NULL,
    "OrderId"      text        NOT NULL,
    "PaymentDate"  date        NOT NULL,
    "CreatedDate"  timestamptz NOT NULL,
    "ModifiedDate" timestamptz NOT NULL,
    CONSTRAINT "pk_return_payment" PRIMARY KEY ("Id"),
    CONSTRAINT "uq_return_payment_payment" UNIQUE ("PaymentId"),
    CONSTRAINT "fk_return_payment_payment" FOREIGN KEY ("PaymentId") REFERENCES payment."Payment" ("Id")
);

-- Property 3: at most one successful receipt per order and one successful refund per return
-- Property 3: mỗi đơn tối đa 1 phiếu thu thành công, mỗi phiếu trả tối đa 1 phiếu chi thành công
CREATE UNIQUE INDEX IF NOT EXISTS "ux_payment_receipt_success" ON payment."Payment" ("OrderId")
    WHERE "PaymentType" = 1 AND "Status" = 'SUCCESS';
CREATE UNIQUE INDEX IF NOT EXISTS "ux_payment_refund_success" ON payment."Payment" ("ReturnId")
    WHERE "PaymentType" = 2 AND "Status" = 'SUCCESS';

-- Incremental ingest watermark / Watermark cho ingest tăng dần
CREATE INDEX IF NOT EXISTS "ix_payment_modified" ON payment."Payment" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_transaction_modified" ON payment."Transaction" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_order_payment_modified" ON payment."OrderPayment" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_return_payment_modified" ON payment."ReturnPayment" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_payment_type_modified" ON payment."PaymentType" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_payment_method_modified" ON payment."PaymentMethod" ("ModifiedDate");

-- Business-date lookups per shop / Tra cứu theo ngày nghiệp vụ và shop
CREATE INDEX IF NOT EXISTS "ix_payment_date_shop" ON payment."Payment" ("PaymentDate", "ShopCode");

-- Join and lookup keys / Khoá join và tra cứu
CREATE INDEX IF NOT EXISTS "ix_payment_order" ON payment."Payment" ("OrderId");
CREATE INDEX IF NOT EXISTS "ix_transaction_payment" ON payment."Transaction" ("PaymentId");
CREATE INDEX IF NOT EXISTS "ix_order_payment_order" ON payment."OrderPayment" ("OrderId");
CREATE INDEX IF NOT EXISTS "ix_return_payment_return" ON payment."ReturnPayment" ("ReturnId");

-- Fixed reference rows; the master generator upserts the same values
-- Dòng tham chiếu cố định; bộ sinh master upsert cùng giá trị
INSERT INTO payment."PaymentType" ("Type", "Name", "CreatedDate", "ModifiedDate")
VALUES (1, 'Phiếu thu', now(), now()),
       (2, 'Phiếu chi', now(), now())
ON CONFLICT ("Type") DO NOTHING;

INSERT INTO payment."PaymentMethod" ("Id", "Name", "CreatedDate", "ModifiedDate")
VALUES (1, 'Tiền mặt', now(), now()),
       (2, 'Thẻ ngân hàng', now(), now()),
       (3, 'Ví điện tử (MoMo/ZaloPay)', now(), now())
ON CONFLICT ("Id") DO NOTHING;

COMMIT;
