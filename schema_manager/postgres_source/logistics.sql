-- Schema "logistics" v2 (supply_chain_forecast_optimization.md 5.2, data_model_v2.md 3.3).
-- Schema "logistics" v2 (supply_chain_forecast_optimization.md 5.2, data_model_v2.md 3.3).
-- Carriers, vehicles, lanes, distances and schedules are master data; shipments and their events are
-- written by the day-close simulator (Phase 1), which also queues each ShipmentEvent in inventory."Outbox"
-- with its future EventTime so OutboxRelayJob publishes it on time.
-- Hãng, xe, tuyến, khoảng cách, lịch giao là master; chuyến và sự kiện do bộ mô phỏng chốt sổ ghi (Phase 1),
-- đồng thời xếp mỗi ShipmentEvent vào inventory."Outbox" với EventTime tương lai để OutboxRelayJob phát đúng giờ.

BEGIN;

CREATE SCHEMA IF NOT EXISTS logistics;

CREATE TABLE IF NOT EXISTS logistics."Carrier"
(
    "Id"                   text         NOT NULL,
    "Name"                 text         NOT NULL,
    "Type"                 text         NOT NULL,
    "FixedCostPerTrip"     bigint       NOT NULL,
    "CostPerKm"            bigint       NOT NULL,
    "CostPerKg"            bigint       NOT NULL,
    "CostPerStop"          bigint       NOT NULL,
    "BaseOnTimeRate"       numeric(4,3) NOT NULL,
    "SupportsColdChain"    boolean      NOT NULL,
    "WeeklyCapacityKg"     integer      NOT NULL,
    "ContractMinKgPerWeek" integer      NOT NULL DEFAULT 0,
    "CreatedDate"          timestamptz  NOT NULL,
    "ModifiedDate"         timestamptz  NOT NULL,
    CONSTRAINT "pk_carrier" PRIMARY KEY ("Id"),
    CONSTRAINT "ck_carrier_type" CHECK ("Type" IN ('INHOUSE', '3PL', 'EXPRESS')),
    CONSTRAINT "ck_carrier_cost" CHECK ("FixedCostPerTrip" >= 0 AND "CostPerKm" >= 0
        AND "CostPerKg" >= 0 AND "CostPerStop" >= 0),
    CONSTRAINT "ck_carrier_rate" CHECK ("BaseOnTimeRate" > 0 AND "BaseOnTimeRate" <= 1),
    CONSTRAINT "ck_carrier_capacity" CHECK ("WeeklyCapacityKg" > 0
        AND "ContractMinKgPerWeek" BETWEEN 0 AND "WeeklyCapacityKg")
);

-- Price list versions; Carrier holds the current one / Các phiên bản bảng giá; Carrier giữ bản hiện hành
CREATE TABLE IF NOT EXISTS logistics."CarrierRate"
(
    "Id"               text        NOT NULL,
    "CarrierId"        text        NOT NULL,
    "FixedCostPerTrip" bigint      NOT NULL,
    "CostPerKm"        bigint      NOT NULL,
    "CostPerKg"        bigint      NOT NULL,
    "CostPerStop"      bigint      NOT NULL,
    "EffectiveFrom"    date        NOT NULL,
    "EffectiveTo"      date,
    "CreatedDate"      timestamptz NOT NULL,
    "ModifiedDate"     timestamptz NOT NULL,
    CONSTRAINT "pk_carrier_rate" PRIMARY KEY ("Id"),
    CONSTRAINT "ck_carrier_rate_cost" CHECK ("FixedCostPerTrip" >= 0 AND "CostPerKm" >= 0
        AND "CostPerKg" >= 0 AND "CostPerStop" >= 0),
    CONSTRAINT "ck_carrier_rate_period" CHECK ("EffectiveTo" IS NULL OR "EffectiveTo" >= "EffectiveFrom")
);

CREATE TABLE IF NOT EXISTS logistics."Vehicle"
(
    "Id"              text         NOT NULL,
    "CarrierId"       text         NOT NULL,
    "VehicleType"     text         NOT NULL,
    "CapacityKg"      integer      NOT NULL,
    "CapacityM3"      numeric(6,2) NOT NULL,
    -- Gross weight class, drives urban truck bans / Tải trọng toàn bộ, quyết định giờ cấm tải nội đô
    "GrossWeightT"    numeric(4,1) NOT NULL,
    "IsCold"          boolean      NOT NULL,
    "HomeWarehouseId" text,
    "Status"          text         NOT NULL,
    "CreatedDate"     timestamptz  NOT NULL,
    "ModifiedDate"    timestamptz  NOT NULL,
    CONSTRAINT "pk_vehicle" PRIMARY KEY ("Id"),
    CONSTRAINT "ck_vehicle_capacity" CHECK ("CapacityKg" > 0 AND "CapacityM3" > 0 AND "GrossWeightT" > 0),
    CONSTRAINT "ck_vehicle_status" CHECK ("Status" IN ('ACTIVE', 'MAINTENANCE', 'RETIRED'))
);

-- DC → shop route / Tuyến DC → shop
CREATE TABLE IF NOT EXISTS logistics."Lane"
(
    "Id"                 text         NOT NULL,
    "WarehouseId"        text         NOT NULL,
    "ShopCode"           text         NOT NULL,
    "StraightKm"         numeric(7,2) NOT NULL,
    "RoadKm"             numeric(7,2) NOT NULL,
    "BaseTransitMinutes" integer      NOT NULL,
    "DeliveryWindowFrom" time         NOT NULL,
    "DeliveryWindowTo"   time         NOT NULL,
    "DefaultCarrierId"   text         NOT NULL,
    "CreatedDate"        timestamptz  NOT NULL,
    "ModifiedDate"       timestamptz  NOT NULL,
    CONSTRAINT "pk_lane" PRIMARY KEY ("Id"),
    CONSTRAINT "uq_lane_route" UNIQUE ("WarehouseId", "ShopCode"),
    CONSTRAINT "ck_lane_distance" CHECK ("StraightKm" >= 0 AND "RoadKm" >= "StraightKm" AND "BaseTransitMinutes" > 0),
    CONSTRAINT "ck_lane_window" CHECK ("DeliveryWindowTo" > "DeliveryWindowFrom")
);

-- Every DC↔shop and shop↔shop pair, for multi-stop routing / Mọi cặp DC↔shop và shop↔shop, cho tuyến nhiều điểm dừng
CREATE TABLE IF NOT EXISTS logistics."DistanceMatrix"
(
    "FromType"       text         NOT NULL,
    "FromId"         text         NOT NULL,
    "ToType"         text         NOT NULL,
    "ToId"           text         NOT NULL,
    "StraightKm"     numeric(7,2) NOT NULL,
    "RoadKm"         numeric(7,2) NOT NULL,
    "CircuityFactor" numeric(4,2) NOT NULL,
    "OffPeakMinutes" integer      NOT NULL,
    "PeakMinutes"    integer      NOT NULL,
    "Method"         text         NOT NULL,
    "CreatedDate"    timestamptz  NOT NULL,
    "ModifiedDate"   timestamptz  NOT NULL,
    CONSTRAINT "pk_distance_matrix" PRIMARY KEY ("FromType", "FromId", "ToType", "ToId"),
    CONSTRAINT "ck_distance_matrix_type" CHECK ("FromType" IN ('SHOP', 'DC') AND "ToType" IN ('SHOP', 'DC')),
    CONSTRAINT "ck_distance_matrix_values" CHECK ("StraightKm" >= 0 AND "RoadKm" >= "StraightKm"
        AND "CircuityFactor" >= 1 AND "OffPeakMinutes" >= 0 AND "PeakMinutes" >= "OffPeakMinutes"),
    CONSTRAINT "ck_distance_matrix_method" CHECK ("Method" IN ('HAVERSINE_FACTOR', 'OSRM'))
);

-- Delivery weekdays per shop (1 = Monday) / Thứ giao hàng của shop (1 = thứ Hai)
CREATE TABLE IF NOT EXISTS logistics."DeliverySchedule"
(
    "Id"            text        NOT NULL,
    "ShopCode"      text        NOT NULL,
    "WarehouseId"   text        NOT NULL,
    "Weekday"       smallint    NOT NULL,
    "CutoffTime"    time        NOT NULL,
    "EffectiveFrom" date        NOT NULL,
    "EffectiveTo"   date,
    "CreatedDate"   timestamptz NOT NULL,
    "ModifiedDate"  timestamptz NOT NULL,
    CONSTRAINT "pk_delivery_schedule" PRIMARY KEY ("Id"),
    CONSTRAINT "ck_delivery_schedule_weekday" CHECK ("Weekday" BETWEEN 1 AND 7),
    CONSTRAINT "ck_delivery_schedule_period" CHECK ("EffectiveTo" IS NULL OR "EffectiveTo" >= "EffectiveFrom")
);

-- One truck trip / Một chuyến xe
CREATE TABLE IF NOT EXISTS logistics."Shipment"
(
    "Id"                   text         NOT NULL,
    "WarehouseId"          text         NOT NULL,
    "CarrierId"            text         NOT NULL,
    "VehicleId"            text,
    "Status"               text         NOT NULL,
    "PlannedDepartureTime" timestamptz  NOT NULL,
    "ActualDepartureTime"  timestamptz,
    "DistanceKm"           numeric(7,2),
    "TotalWeightKg"        numeric(9,2),
    "TotalVolumeM3"        numeric(8,3),
    "StopCount"            smallint     NOT NULL,
    "TransportCost"        bigint       NOT NULL,
    -- ICT date of the planned departure, partition key downstream / Ngày VN của giờ xuất phát dự kiến
    "ShipmentDate"         date         NOT NULL,
    "CreatedDate"          timestamptz  NOT NULL,
    "ModifiedDate"         timestamptz  NOT NULL,
    CONSTRAINT "pk_shipment" PRIMARY KEY ("Id"),
    CONSTRAINT "ck_shipment_status"
        CHECK ("Status" IN ('PLANNED', 'PICKED', 'IN_TRANSIT', 'COMPLETED', 'PARTIAL', 'FAILED')),
    CONSTRAINT "ck_shipment_values" CHECK ("StopCount" > 0 AND "TransportCost" >= 0
        AND ("DistanceKm" IS NULL OR "DistanceKm" >= 0)),
    -- Trips past PLANNED/PICKED have left the dock / Chuyến đã qua PLANNED/PICKED thì đã xuất bến
    CONSTRAINT "ck_shipment_departed"
        CHECK ("Status" IN ('PLANNED', 'PICKED', 'FAILED') OR "ActualDepartureTime" IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS logistics."ShipmentStop"
(
    "Id"                 text        NOT NULL,
    "ShipmentId"         text        NOT NULL,
    "StopSeq"            smallint    NOT NULL,
    "ShopCode"           text        NOT NULL,
    "TransferOrderId"    text        NOT NULL,
    "PlannedArrivalTime" timestamptz NOT NULL,
    "ActualArrivalTime"  timestamptz,
    "Status"             text        NOT NULL,
    "CreatedDate"        timestamptz NOT NULL,
    "ModifiedDate"       timestamptz NOT NULL,
    CONSTRAINT "pk_shipment_stop" PRIMARY KEY ("Id"),
    CONSTRAINT "uq_shipment_stop_seq" UNIQUE ("ShipmentId", "StopSeq"),
    CONSTRAINT "fk_shipment_stop_shipment" FOREIGN KEY ("ShipmentId") REFERENCES logistics."Shipment" ("Id"),
    CONSTRAINT "ck_shipment_stop_seq" CHECK ("StopSeq" > 0),
    CONSTRAINT "ck_shipment_stop_status" CHECK ("Status" IN ('PENDING', 'DELIVERED', 'DELAYED', 'PARTIAL', 'FAILED')),
    CONSTRAINT "ck_shipment_stop_arrived"
        CHECK ("Status" NOT IN ('DELIVERED', 'PARTIAL') OR "ActualArrivalTime" IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS logistics."ShipmentItem"
(
    "Id"                  text        NOT NULL,
    "ShipmentStopId"      text        NOT NULL,
    "TransferOrderItemId" text        NOT NULL,
    "ProductId"           text        NOT NULL,
    "LotId"               text        NOT NULL,
    "Qty"                 integer     NOT NULL,
    "ReceivedQty"         integer,
    "CreatedDate"         timestamptz NOT NULL,
    "ModifiedDate"        timestamptz NOT NULL,
    CONSTRAINT "pk_shipment_item" PRIMARY KEY ("Id"),
    CONSTRAINT "fk_shipment_item_stop" FOREIGN KEY ("ShipmentStopId") REFERENCES logistics."ShipmentStop" ("Id"),
    CONSTRAINT "ck_shipment_item_qty" CHECK ("Qty" > 0 AND ("ReceivedQty" IS NULL OR "ReceivedQty" BETWEEN 0 AND "Qty"))
);

-- Timeline of a trip; EventSeq orders events within the shipment / Dòng thời gian của chuyến; EventSeq xếp thứ tự trong chuyến
CREATE TABLE IF NOT EXISTS logistics."ShipmentEvent"
(
    "Id"             text        NOT NULL,
    "ShipmentId"     text        NOT NULL,
    "EventSeq"       integer     NOT NULL,
    "ShipmentStopId" text,
    "EventType"      text        NOT NULL,
    "EventTime"      timestamptz NOT NULL,
    "ReasonCode"     text,
    "CreatedDate"    timestamptz NOT NULL,
    "ModifiedDate"   timestamptz NOT NULL,
    CONSTRAINT "pk_shipment_event" PRIMARY KEY ("Id"),
    CONSTRAINT "uq_shipment_event_seq" UNIQUE ("ShipmentId", "EventSeq"),
    CONSTRAINT "fk_shipment_event_shipment" FOREIGN KEY ("ShipmentId") REFERENCES logistics."Shipment" ("Id"),
    CONSTRAINT "ck_shipment_event_type" CHECK ("EventType" IN ('TRANSFER_CREATED', 'SHIPMENT_PLANNED', 'PICKED',
        'DEPARTED', 'ARRIVED_STOP', 'DELIVERED', 'PARTIAL', 'DELAYED', 'FAILED')),
    CONSTRAINT "ck_shipment_event_reason" CHECK ("ReasonCode" IS NULL
        OR "ReasonCode" IN ('WEATHER', 'TRAFFIC', 'VEHICLE', 'HOLIDAY', 'SHORT_PICK', 'TRUCK_BAN')),
    -- Stop-level events name their stop / Sự kiện theo điểm dừng phải ghi điểm dừng
    CONSTRAINT "ck_shipment_event_stop"
        CHECK ("EventType" NOT IN ('ARRIVED_STOP', 'DELIVERED', 'PARTIAL') OR "ShipmentStopId" IS NOT NULL)
);

-- Incremental ingest watermark / Watermark cho ingest tăng dần
CREATE INDEX IF NOT EXISTS "ix_carrier_modified" ON logistics."Carrier" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_carrier_rate_modified" ON logistics."CarrierRate" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_vehicle_modified" ON logistics."Vehicle" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_lane_modified" ON logistics."Lane" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_distance_matrix_modified" ON logistics."DistanceMatrix" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_delivery_schedule_modified" ON logistics."DeliverySchedule" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_shipment_modified" ON logistics."Shipment" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_shipment_stop_modified" ON logistics."ShipmentStop" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_shipment_item_modified" ON logistics."ShipmentItem" ("ModifiedDate");
CREATE INDEX IF NOT EXISTS "ix_shipment_event_modified" ON logistics."ShipmentEvent" ("ModifiedDate");

-- Join and lookup keys / Khoá join và tra cứu
CREATE INDEX IF NOT EXISTS "ix_carrier_rate_carrier" ON logistics."CarrierRate" ("CarrierId", "EffectiveFrom");
CREATE INDEX IF NOT EXISTS "ix_vehicle_carrier" ON logistics."Vehicle" ("CarrierId");
CREATE INDEX IF NOT EXISTS "ix_lane_shop" ON logistics."Lane" ("ShopCode");
CREATE INDEX IF NOT EXISTS "ix_delivery_schedule_shop" ON logistics."DeliverySchedule" ("ShopCode", "Weekday");
CREATE INDEX IF NOT EXISTS "ix_shipment_date" ON logistics."Shipment" ("ShipmentDate", "WarehouseId");
CREATE INDEX IF NOT EXISTS "ix_shipment_carrier" ON logistics."Shipment" ("CarrierId", "ShipmentDate");
CREATE INDEX IF NOT EXISTS "ix_shipment_stop_shop" ON logistics."ShipmentStop" ("ShopCode", "PlannedArrivalTime");
CREATE INDEX IF NOT EXISTS "ix_shipment_stop_transfer" ON logistics."ShipmentStop" ("TransferOrderId");
CREATE INDEX IF NOT EXISTS "ix_shipment_item_stop" ON logistics."ShipmentItem" ("ShipmentStopId");
CREATE INDEX IF NOT EXISTS "ix_shipment_event_time" ON logistics."ShipmentEvent" ("EventTime");

COMMIT;
