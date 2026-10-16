-- Schema "inventory" v2 (supply_chain_forecast_optimization.md 5.2, data_model_v2.md 3.3).
-- Schema "inventory" v2 (supply_chain_forecast_optimization.md 5.2, data_model_v2.md 3.3).
-- PG is the source of truth for stock: every change goes through inventory.apply_movement, which writes the
-- ledger row, the balance and one Outbox row in the caller's transaction (DB-first + outbox).
-- PG là nguồn sự thật về tồn: mọi thay đổi đi qua inventory.apply_movement, hàm này ghi dòng sổ cái, số tồn
-- và một dòng Outbox trong transaction của bên gọi (DB-first + outbox).
-- Writers: SalesWriterJob (sales, returns via post_sales_stock), day-close simulator (receipts, transfers,
-- expiry, snapshots; Phase 1). Outbox is read by OutboxRelayJob and is not ingested into the lakehouse.
-- Bên ghi: SalesWriterJob (bán, trả qua post_sales_stock), bộ mô phỏng chốt sổ (nhập, điều chuyển, hết hạn,
-- snapshot; Phase 1). Outbox do OutboxRelayJob đọc, không ingest vào lakehouse.

BEGIN;

CREATE SCHEMA IF NOT EXISTS inventory;

CREATE TABLE IF NOT EXISTS inventory."Warehouse"
(
    "Id"              text          NOT NULL,
    "Name"            text          NOT NULL,
    "WardId"          text,
    "Latitude"        numeric(9,6),
    "Longitude"       numeric(9,6),
    "StorageM3"       numeric(10,2) NOT NULL,
    "ColdStorageL"    integer       NOT NULL,
    "DockSlotsPerDay" smallint      NOT NULL,
    "CreatedDate"     timestamptz   NOT NULL,
    "ModifiedDate"    timestamptz   NOT NULL,
    CONSTRAINT "pk_warehouse" PRIMARY KEY ("Id"),
    CONSTRAINT "ck_warehouse_capacity" CHECK ("StorageM3" > 0 AND "ColdStorageL" >= 0 AND "DockSlotsPerDay" > 0)
);

CREATE TABLE IF NOT EXISTS inventory."InventoryLot"
(
    "Id"              text        NOT NULL,
    "ProductId"       text        NOT NULL,
    "LotNo"           text        NOT NULL,
    -- Null sorts last in FEFO / Null xếp cuối khi xuất theo hạn dùng (FEFO)
    "ExpiryDate"      date,
    "ReceivedDate"    date        NOT NULL,
    "SupplierId"      text,
    "PurchaseOrderId" text,
    "CreatedDate"     timestamptz NOT NULL,
    "ModifiedDate"    timestamptz NOT NULL,
    CONSTRAINT "pk_inventory_lot" PRIMARY KEY ("Id")
);

-- Current stock per location, product and lot; changed only by apply_movement
-- Tồn hiện tại theo vị trí, sản phẩm và lô; chỉ apply_movement được sửa
CREATE TABLE IF NOT EXISTS inventory."InventoryBalance"
(
    "LocationType" text        NOT NULL,
    "LocationId"   text        NOT NULL,
    "ProductId"    text        NOT NULL,
    "LotId"        text        NOT NULL,
    "OnHandQty"    integer     NOT NULL,
    "ReservedQty"  integer     NOT NULL DEFAULT 0,
    "ModifiedDate" timestamptz NOT NULL,
    CONSTRAINT "pk_inventory_balance" PRIMARY KEY ("LocationType", "LocationId", "ProductId", "LotId"),
    CONSTRAINT "ck_inventory_balance_location" CHECK ("LocationType" IN ('SHOP', 'DC')),
    CONSTRAINT "ck_inventory_balance_qty" CHECK ("OnHandQty" >= 0 AND "ReservedQty" BETWEEN 0 AND "OnHandQty")
);

-- Global movement order: ledger replay and the Kafka event version / Thứ tự biến động toàn cục: dựng lại sổ cái và version sự kiện Kafka
CREATE SEQUENCE IF NOT EXISTS inventory.stock_movement_seq AS bigint;

-- Stock ledger, signed quantities; Id is deterministic so a replay writes nothing
-- Sổ cái kho, số lượng có dấu; Id tất định nên ghi lại không sinh dòng mới
CREATE TABLE IF NOT EXISTS inventory."StockMovement"
(
    "Id"           text        NOT NULL,
    "Seq"          bigint      NOT NULL DEFAULT nextval('inventory.stock_movement_seq'),
    "MovementType" text        NOT NULL,
    "LocationType" text        NOT NULL,
    "LocationId"   text        NOT NULL,
    "ProductId"    text        NOT NULL,
    "LotId"        text        NOT NULL,
    "Qty"          integer     NOT NULL,
    "UnitCost"     bigint      NOT NULL,
    "RefDocType"   text        NOT NULL,
    "RefDocId"     text        NOT NULL,
    "RefLineId"    text,
    -- Why an ADJUST happened (OPENING, OVERSELL, COUNT) / Lý do điều chỉnh
    "Reason"       text,
    "MovementTime" timestamptz NOT NULL,
    "MovementDate" date        NOT NULL,
    "CreatedDate"  timestamptz NOT NULL,
    "ModifiedDate" timestamptz NOT NULL,
    CONSTRAINT "pk_stock_movement" PRIMARY KEY ("Id", "MovementTime"),
    CONSTRAINT "ck_stock_movement_type" CHECK ("MovementType" IN ('SALE', 'RETURN_RESTOCK', 'RECEIPT',
        'TRANSFER_OUT', 'TRANSFER_IN', 'EXPIRE', 'DAMAGE', 'ADJUST')),
    CONSTRAINT "ck_stock_movement_location" CHECK ("LocationType" IN ('SHOP', 'DC')),
    CONSTRAINT "ck_stock_movement_ref" CHECK ("RefDocType" IN ('ORDER', 'RETURN', 'PO', 'TRANSFER', 'SHIPMENT', 'ADJ')),
    -- Outflows negative, inflows positive, adjustments either way / Xuất âm, nhập dương, điều chỉnh hai chiều
    CONSTRAINT "ck_stock_movement_sign" CHECK (
        ("MovementType" IN ('SALE', 'TRANSFER_OUT', 'EXPIRE', 'DAMAGE') AND "Qty" < 0)
        OR ("MovementType" IN ('RETURN_RESTOCK', 'RECEIPT', 'TRANSFER_IN') AND "Qty" > 0)
        OR ("MovementType" = 'ADJUST' AND "Qty" <> 0)),
    CONSTRAINT "ck_stock_movement_reason" CHECK (("MovementType" = 'ADJUST') = ("Reason" IS NOT NULL)),
    CONSTRAINT "ck_stock_movement_cost" CHECK ("UnitCost" >= 0)
) PARTITION BY RANGE ("MovementTime");

-- End-of-day stock per location and product, written by the day-close job
-- Tồn cuối ngày theo vị trí và sản phẩm, do job chốt sổ ghi
CREATE TABLE IF NOT EXISTS inventory."InventorySnapshot"
(
    "SnapshotDate"     date        NOT NULL,
    "LocationType"     text        NOT NULL,
    "LocationId"       text        NOT NULL,
    "ProductId"        text        NOT NULL,
    "OnHandQty"        integer     NOT NULL,
    "InTransitQty"     integer     NOT NULL,
    "OnOrderQty"       integer     NOT NULL,
    "ExpiredQty"       integer     NOT NULL,
    "StockoutFlag"     boolean     NOT NULL,
    "OverCapacityFlag" boolean     NOT NULL,
    "CreatedDate"      timestamptz NOT NULL,
    "ModifiedDate"     timestamptz NOT NULL,
    CONSTRAINT "pk_inventory_snapshot" PRIMARY KEY ("SnapshotDate", "LocationType", "LocationId", "ProductId"),
    CONSTRAINT "ck_inventory_snapshot_location" CHECK ("LocationType" IN ('SHOP', 'DC')),
    CONSTRAINT "ck_inventory_snapshot_qty" CHECK ("OnHandQty" >= 0 AND "InTransitQty" >= 0
        AND "OnOrderQty" >= 0 AND "ExpiredQty" >= 0)
) PARTITION BY RANGE ("SnapshotDate");

-- Reorder rules per location and product; the optimizer writes new versions (closed loop)
-- Quy tắc đặt hàng theo vị trí và sản phẩm; bộ tối ưu ghi phiên bản mới (vòng lặp kín)
CREATE TABLE IF NOT EXISTS inventory."ReplenishmentPolicy"
(
    "Id"               text        NOT NULL,
    "LocationType"     text        NOT NULL,
    "LocationId"       text        NOT NULL,
    "ProductId"        text        NOT NULL,
    "PolicyType"       text        NOT NULL,
    "ReviewPeriodDays" smallint,
    "ReorderPoint"     integer,
    "OrderUpTo"        integer,
    "SafetyStock"      integer,
    "Source"           text        NOT NULL,
    "SolverRunId"      text,
    "EffectiveFrom"    date        NOT NULL,
    "EffectiveTo"      date,
    "CreatedDate"      timestamptz NOT NULL,
    "ModifiedDate"     timestamptz NOT NULL,
    CONSTRAINT "pk_replenishment_policy" PRIMARY KEY ("Id"),
    CONSTRAINT "ck_replenishment_policy_location" CHECK ("LocationType" IN ('SHOP', 'DC')),
    CONSTRAINT "ck_replenishment_policy_type" CHECK ("PolicyType" IN ('RsS', 'ROP')),
    CONSTRAINT "ck_replenishment_policy_source" CHECK ("Source" IN ('BASELINE', 'OPTIMIZER')),
    -- Optimizer rows name their solver run / Dòng của bộ tối ưu phải ghi lần chạy solver
    CONSTRAINT "ck_replenishment_policy_run" CHECK ("Source" = 'BASELINE' OR "SolverRunId" IS NOT NULL),
    CONSTRAINT "ck_replenishment_policy_levels" CHECK ("ReorderPoint" >= 0 AND "SafetyStock" >= 0
        AND ("OrderUpTo" IS NULL OR "OrderUpTo" >= "ReorderPoint")),
    CONSTRAINT "ck_replenishment_policy_review" CHECK ("PolicyType" = 'ROP' OR "ReviewPeriodDays" > 0),
    CONSTRAINT "ck_replenishment_policy_period" CHECK ("EffectiveTo" IS NULL OR "EffectiveTo" >= "EffectiveFrom")
);

-- DC → shop replenishment order / Lệnh điều chuyển từ DC về shop
CREATE TABLE IF NOT EXISTS inventory."TransferOrder"
(
    "Id"                  text        NOT NULL,
    "FromWarehouseId"     text        NOT NULL,
    "ToShopCode"          text        NOT NULL,
    "Status"              text        NOT NULL,
    "RequestedDate"       date        NOT NULL,
    "PlannedDeliveryDate" date        NOT NULL,
    "CreatedDate"         timestamptz NOT NULL,
    "ModifiedDate"        timestamptz NOT NULL,
    CONSTRAINT "pk_transfer_order" PRIMARY KEY ("Id"),
    CONSTRAINT "ck_transfer_order_status"
        CHECK ("Status" IN ('CREATED', 'PICKED', 'SHIPPED', 'RECEIVED', 'PARTIAL', 'CANCELLED')),
    CONSTRAINT "ck_transfer_order_dates" CHECK ("PlannedDeliveryDate" >= "RequestedDate")
);

CREATE TABLE IF NOT EXISTS inventory."TransferOrderItem"
(
    "Id"              text        NOT NULL,
    "TransferOrderId" text        NOT NULL,
    "ProductId"       text        NOT NULL,
    "RequestedQty"    integer     NOT NULL,
    "ShippedQty"      integer     NOT NULL DEFAULT 0,
    "ReceivedQty"     integer     NOT NULL DEFAULT 0,
    "CreatedDate"     timestamptz NOT NULL,
    "ModifiedDate"    timestamptz NOT NULL,
    CONSTRAINT "pk_transfer_order_item" PRIMARY KEY ("Id"),
    CONSTRAINT "fk_transfer_order_item_order" FOREIGN KEY ("TransferOrderId")
        REFERENCES inventory."TransferOrder" ("Id"),
    CONSTRAINT "ck_transfer_order_item_qty" CHECK ("RequestedQty" > 0
        AND "ShippedQty" BETWEEN 0 AND "RequestedQty" AND "ReceivedQty" BETWEEN 0 AND "ShippedQty")
);

-- Events waiting for OutboxRelayJob; one row per Kafka message. Not ingested, purged 3 days after publishing
-- Sự kiện chờ OutboxRelayJob; mỗi dòng một message Kafka. Không ingest, xoá 3 ngày sau khi phát
CREATE TABLE IF NOT EXISTS inventory."Outbox"
(
    "Seq"         bigint      GENERATED ALWAYS AS IDENTITY,
    "Topic"       text        NOT NULL,
    -- Kafka key and envelope aggregateId: location:product or shipmentId / Key Kafka và aggregateId
    "AggregateId" text        NOT NULL,
    "EventType"   text        NOT NULL,
    -- Envelope version: movement Seq or shipment event seq / Version của envelope
    "Version"     bigint      NOT NULL,
    "Payload"     jsonb       NOT NULL,
    -- Not published before this time (future shipment events) / Không phát trước thời điểm này
    "EventTime"   timestamptz NOT NULL,
    "CreatedDate" timestamptz NOT NULL DEFAULT now(),
    "PublishedAt" timestamptz,
    CONSTRAINT "pk_outbox" PRIMARY KEY ("Seq"),
    CONSTRAINT "ck_outbox_topic" CHECK ("Topic" IN ('inventory-movements', 'logistics-events'))
);

-- Ledger cut-over: the sales stream posts stock only for documents at or after PostFrom (none if no row)
-- Mốc bắt đầu ghi sổ: stream bán hàng chỉ ghi tồn cho chứng từ từ PostFrom trở đi (không có dòng thì không ghi)
CREATE TABLE IF NOT EXISTS inventory."PostingCutover"
(
    "Source"       text        NOT NULL,
    "PostFrom"     timestamptz NOT NULL,
    "ModifiedDate" timestamptz NOT NULL,
    CONSTRAINT "pk_posting_cutover" PRIMARY KEY ("Source"),
    CONSTRAINT "ck_posting_cutover_source" CHECK ("Source" IN ('SALES_STREAM'))
);

-- Monthly partitions 2024-01 … 2027-12 plus a default / Partition theo tháng 2024-01 … 2027-12 và một default
DO $$
DECLARE
    m date;
BEGIN
    FOR m IN SELECT generate_series(date '2024-01-01', date '2027-12-01', interval '1 month')::date LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS inventory.%I PARTITION OF inventory."StockMovement" '
                       'FOR VALUES FROM (%L) TO (%L)',
                       'StockMovement_' || to_char(m, 'YYYYMM'),
                       (m::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh'),
                       ((m + interval '1 month')::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh'));
        EXECUTE format('CREATE TABLE IF NOT EXISTS inventory.%I PARTITION OF inventory."InventorySnapshot" '
                       'FOR VALUES FROM (%L) TO (%L)',
                       'InventorySnapshot_' || to_char(m, 'YYYYMM'), m, (m + interval '1 month')::date);
    END LOOP;
END $$;
CREATE TABLE IF NOT EXISTS inventory."StockMovement_default" PARTITION OF inventory."StockMovement" DEFAULT;
CREATE TABLE IF NOT EXISTS inventory."InventorySnapshot_default" PARTITION OF inventory."InventorySnapshot" DEFAULT;

-- Incremental ingest watermark / Watermark cho ingest tăng dần
CREATE INDEX IF NOT EXISTS "ix_warehouse_modified" ON inventory."Warehouse" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_inventory_lot_modified" ON inventory."InventoryLot" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_inventory_balance_modified" ON inventory."InventoryBalance" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_stock_movement_modified" ON inventory."StockMovement" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_inventory_snapshot_modified" ON inventory."InventorySnapshot" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_replenishment_policy_modified" ON inventory."ReplenishmentPolicy" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_transfer_order_modified" ON inventory."TransferOrder" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_transfer_order_item_modified" ON inventory."TransferOrderItem" ("ModifiedDate");

-- Ledger lookups: replay check per document line, history per position, global order
-- Tra cứu sổ cái: kiểm tra ghi lại theo dòng chứng từ, lịch sử theo vị trí, thứ tự toàn cục
CREATE INDEX IF NOT EXISTS "ix_stock_movement_ref_line" ON inventory."StockMovement" ("RefLineId", "MovementType");
CREATE INDEX IF NOT EXISTS "ix_stock_movement_position"
    ON inventory."StockMovement" ("LocationId", "ProductId", "MovementTime");
CREATE INDEX IF NOT EXISTS "ix_stock_movement_seq" ON inventory."StockMovement" ("Seq");
CREATE INDEX IF NOT EXISTS "ix_inventory_lot_product" ON inventory."InventoryLot" ("ProductId", "ExpiryDate");
CREATE INDEX IF NOT EXISTS "ix_replenishment_policy_position"
    ON inventory."ReplenishmentPolicy" ("LocationId", "ProductId", "EffectiveFrom");
CREATE INDEX IF NOT EXISTS "ix_transfer_order_delivery"
    ON inventory."TransferOrder" ("PlannedDeliveryDate", "ToShopCode");
CREATE INDEX IF NOT EXISTS "ix_transfer_order_item_order" ON inventory."TransferOrderItem" ("TransferOrderId");

-- Relay scan of unpublished rows and the purge of published ones / Relay quét dòng chưa phát và dọn dòng đã phát
CREATE INDEX IF NOT EXISTS "ix_outbox_unpublished" ON inventory."Outbox" ("Seq") WHERE "PublishedAt" IS NULL;
CREATE INDEX IF NOT EXISTS "ix_outbox_published" ON inventory."Outbox" ("PublishedAt") WHERE "PublishedAt" IS NOT NULL;

-- ---------------------------------------------------------------------------------------------------------
-- Functions / Hàm
-- ---------------------------------------------------------------------------------------------------------

-- Lot used for opening stock and oversell adjustments, created on first use
-- Lô dùng cho tồn đầu kỳ và điều chỉnh bán vượt tồn, tạo ở lần dùng đầu tiên
CREATE OR REPLACE FUNCTION inventory.default_lot(p_product_id text)
    RETURNS text
    LANGUAGE plpgsql AS
$$
DECLARE
    v_lot text := 'LOT-OPEN-' || p_product_id;
BEGIN
    INSERT INTO inventory."InventoryLot" ("Id", "ProductId", "LotNo", "ExpiryDate", "ReceivedDate",
                                         "SupplierId", "PurchaseOrderId", "CreatedDate", "ModifiedDate")
    VALUES (v_lot, p_product_id, 'OPENING', NULL, (now() AT TIME ZONE 'Asia/Ho_Chi_Minh')::date, NULL, NULL, now(), now())
    ON CONFLICT ("Id") DO NOTHING;
    RETURN v_lot;
END
$$;

-- Write one ledger row, its balance change and its Outbox event; returns the movement Seq,
-- or NULL when the movement already exists (replay: nothing changes)
-- Ghi một dòng sổ cái, thay đổi tồn và sự kiện Outbox; trả về Seq của biến động,
-- hoặc NULL khi biến động đã có (ghi lại: không đổi gì)
CREATE OR REPLACE FUNCTION inventory.apply_movement(
    p_id text, p_type text, p_location_type text, p_location_id text, p_product_id text, p_lot_id text,
    p_qty integer, p_unit_cost bigint, p_ref_doc_type text, p_ref_doc_id text, p_ref_line_id text,
    p_reason text, p_movement_time timestamptz)
    RETURNS bigint
    LANGUAGE plpgsql AS
$$
DECLARE
    v_seq     bigint;
    v_on_hand integer;
    v_expiry  date;
BEGIN
    INSERT INTO inventory."StockMovement" ("Id", "MovementType", "LocationType", "LocationId", "ProductId",
                                          "LotId", "Qty", "UnitCost", "RefDocType", "RefDocId", "RefLineId",
                                          "Reason", "MovementTime", "MovementDate", "CreatedDate", "ModifiedDate")
    VALUES (p_id, p_type, p_location_type, p_location_id, p_product_id, p_lot_id, p_qty, p_unit_cost,
            p_ref_doc_type, p_ref_doc_id, p_ref_line_id, p_reason, p_movement_time,
            (p_movement_time AT TIME ZONE 'Asia/Ho_Chi_Minh')::date, now(), now())
    ON CONFLICT ("Id", "MovementTime") DO NOTHING
    RETURNING "Seq" INTO v_seq;
    IF v_seq IS NULL THEN
        RETURN NULL;
    END IF;

    -- Update first: INSERT ... ON CONFLICT checks the proposed row, so a decrement would fail on its negative qty.
    -- Negative stock is rejected by ck_inventory_balance_qty.
    -- Cập nhật trước: INSERT ... ON CONFLICT kiểm tra dòng định chèn nên lệnh trừ sẽ lỗi vì số âm.
    -- Tồn âm bị ck_inventory_balance_qty chặn.
    UPDATE inventory."InventoryBalance"
    SET "OnHandQty" = "OnHandQty" + p_qty, "ModifiedDate" = now()
    WHERE "LocationType" = p_location_type AND "LocationId" = p_location_id
      AND "ProductId" = p_product_id AND "LotId" = p_lot_id;
    IF NOT FOUND THEN
        INSERT INTO inventory."InventoryBalance" AS b ("LocationType", "LocationId", "ProductId", "LotId",
                                                       "OnHandQty", "ReservedQty", "ModifiedDate")
        VALUES (p_location_type, p_location_id, p_product_id, p_lot_id, p_qty, 0, now())
        ON CONFLICT ("LocationType", "LocationId", "ProductId", "LotId")
            DO UPDATE SET "OnHandQty" = b."OnHandQty" + EXCLUDED."OnHandQty", "ModifiedDate" = EXCLUDED."ModifiedDate";
    END IF;

    SELECT sum("OnHandQty") INTO v_on_hand
    FROM inventory."InventoryBalance"
    WHERE "LocationType" = p_location_type AND "LocationId" = p_location_id AND "ProductId" = p_product_id;
    SELECT "ExpiryDate" INTO v_expiry FROM inventory."InventoryLot" WHERE "Id" = p_lot_id;

    INSERT INTO inventory."Outbox" ("Topic", "AggregateId", "EventType", "Version", "Payload", "EventTime")
    VALUES ('inventory-movements', p_location_id || ':' || p_product_id, p_type, v_seq,
            jsonb_build_object(
                'movementId', p_id, 'seq', v_seq,
                'locationType', p_location_type, 'locationCode', p_location_id, 'productId', p_product_id,
                'lotId', p_lot_id, 'expiryDate', v_expiry,
                'movementType', p_type, 'qty', p_qty, 'onHandAfter', v_on_hand, 'unitCost', p_unit_cost,
                'refType', p_ref_doc_type, 'refId', p_ref_doc_id, 'refLineId', p_ref_line_id, 'reason', p_reason,
                'occurredAt', (extract(epoch FROM p_movement_time) * 1000)::bigint),
            p_movement_time);
    RETURN v_seq;
END
$$;

-- Post SALE (FEFO, gifts included) and RETURN_RESTOCK movements for orders already upserted in this
-- transaction. Lines already in the ledger are skipped, so replays are safe. A shortfall is covered by
-- ADJUST +qty (reason OVERSELL) before the sale, keeping OnHand >= 0. Returns the number of movements.
-- Ghi biến động SALE (FEFO, gồm cả quà) và RETURN_RESTOCK cho các đơn vừa upsert trong transaction này.
-- Dòng đã có trong sổ cái thì bỏ qua nên ghi lại an toàn. Thiếu hàng thì ADJUST +qty (lý do OVERSELL)
-- trước khi bán để OnHand >= 0. Trả về số biến động đã ghi.
CREATE OR REPLACE FUNCTION inventory.post_sales_stock(p_order_ids text[])
    RETURNS integer
    LANGUAGE plpgsql AS
$$
DECLARE
    v_from  timestamptz;
    r       record;
    l       record;
    v_left  integer;
    v_take  integer;
    v_n     integer;
    v_lot   text;
    v_count integer := 0;
BEGIN
    SELECT "PostFrom" INTO v_from FROM inventory."PostingCutover" WHERE "Source" = 'SALES_STREAM';
    IF v_from IS NULL OR cardinality(p_order_ids) = 0 THEN
        RETURN 0;
    END IF;

    -- Lock touched positions in one fixed order so concurrent batches cannot deadlock
    -- Khoá các vị trí bị đụng theo một thứ tự cố định để các batch song song không deadlock
    PERFORM 1
    FROM inventory."InventoryBalance" b
    WHERE b."LocationType" = 'SHOP'
      AND (b."LocationId", b."ProductId") IN (
        SELECT o."ShopCode", i."ProductId"
        FROM "order"."OrderItem" i
                 JOIN "order"."Order" o ON o."Id" = i."OrderId"
        WHERE i."OrderId" = ANY (p_order_ids))
    ORDER BY b."LocationId", b."ProductId", b."LotId"
        FOR UPDATE OF b;

    FOR r IN
        SELECT 0 AS step, o."ShopCode" AS shop, i."ProductId" AS product, i."Id" AS line_id, o."Id" AS doc_id,
               i."Quantity" AS qty, i."UnitCost" AS unit_cost, o."TransactionDate" AS at,
               NULL::text AS sale_line_id, NULL::timestamptz AS sale_at
        FROM "order"."OrderItem" i
                 JOIN "order"."Order" o ON o."Id" = i."OrderId"
        WHERE i."OrderId" = ANY (p_order_ids)
          AND o."TransactionDate" >= v_from
          AND NOT EXISTS (SELECT 1
                          FROM inventory."StockMovement" m
                          WHERE m."RefLineId" = i."Id" AND m."MovementType" = 'SALE'
                            AND m."MovementTime" = o."TransactionDate")
        UNION ALL
        SELECT 1, rt."ShopCode", ri."ProductId", ri."Id", rt."Id", ri."RestockQty", ri."UnitCost", rt."CreatedDate",
               ri."OrderItemId", o."TransactionDate"
        FROM "order"."ReturnItem" ri
                 JOIN "order"."Return" rt ON rt."Id" = ri."ReturnId"
                 JOIN "order"."Order" o ON o."Id" = ri."OrderId"
        WHERE ri."OrderId" = ANY (p_order_ids)
          AND ri."RestockQty" > 0
          AND rt."CreatedDate" >= v_from
          AND NOT EXISTS (SELECT 1
                          FROM inventory."StockMovement" m
                          WHERE m."RefLineId" = ri."Id" AND m."MovementType" = 'RETURN_RESTOCK'
                            AND m."MovementTime" = rt."CreatedDate")
        -- Sales before returns, so a return finds the lot its sale used / Bán trước trả, để phiếu trả tìm được lô đã bán
        ORDER BY 2, 3, 1, 4
        LOOP
            IF r.step = 0 THEN
                v_left := r.qty;
                v_n := 0;
                FOR l IN
                    SELECT b."LotId" AS lot_id, b."OnHandQty" AS on_hand
                    FROM inventory."InventoryBalance" b
                             LEFT JOIN inventory."InventoryLot" lt ON lt."Id" = b."LotId"
                    WHERE b."LocationType" = 'SHOP' AND b."LocationId" = r.shop AND b."ProductId" = r.product
                      AND b."OnHandQty" > 0
                    ORDER BY lt."ExpiryDate" NULLS LAST, lt."ReceivedDate", b."LotId"
                    LOOP
                        EXIT WHEN v_left = 0;
                        v_take := least(v_left, l.on_hand);
                        v_n := v_n + 1;
                        PERFORM inventory.apply_movement(format('SM-ORDER-%s-SALE-%s', r.line_id, v_n), 'SALE',
                                                         'SHOP', r.shop, r.product, l.lot_id, -v_take, r.unit_cost,
                                                         'ORDER', r.doc_id, r.line_id, NULL, r.at);
                        v_left := v_left - v_take;
                        v_count := v_count + 1;
                    END LOOP;
                IF v_left > 0 THEN
                    v_lot := inventory.default_lot(r.product);
                    v_n := v_n + 1;
                    PERFORM inventory.apply_movement(format('SM-ORDER-%s-ADJUST', r.line_id), 'ADJUST',
                                                     'SHOP', r.shop, r.product, v_lot, v_left, r.unit_cost,
                                                     'ADJ', r.doc_id, r.line_id, 'OVERSELL', r.at);
                    PERFORM inventory.apply_movement(format('SM-ORDER-%s-SALE-%s', r.line_id, v_n), 'SALE',
                                                     'SHOP', r.shop, r.product, v_lot, -v_left, r.unit_cost,
                                                     'ORDER', r.doc_id, r.line_id, NULL, r.at);
                    v_count := v_count + 2;
                END IF;
            ELSE
                -- Back into the lot the sale took first; sales before the cut-over use the default lot
                -- Nhập lại vào lô mà lần bán lấy đầu tiên; đơn bán trước mốc ghi sổ dùng lô mặc định
                SELECT m."LotId" INTO v_lot
                FROM inventory."StockMovement" m
                WHERE m."RefLineId" = r.sale_line_id AND m."MovementType" = 'SALE' AND m."MovementTime" = r.sale_at
                ORDER BY m."Seq"
                LIMIT 1;
                IF v_lot IS NULL THEN
                    v_lot := inventory.default_lot(r.product);
                END IF;
                PERFORM inventory.apply_movement(format('SM-RETURN-%s-RETURN_RESTOCK', r.line_id), 'RETURN_RESTOCK',
                                                 'SHOP', r.shop, r.product, v_lot, r.qty, r.unit_cost,
                                                 'RETURN', r.doc_id, r.line_id, NULL, r.at);
                v_count := v_count + 1;
            END IF;
        END LOOP;
    RETURN v_count;
END
$$;

COMMIT;
