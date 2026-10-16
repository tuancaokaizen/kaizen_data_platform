"""Supply-chain master data: geography coordinates, shop tiers and locations, product logistics attributes,
suppliers, DCs, carriers, vehicles, distances, lanes, delivery schedules, shop capacity and baseline
replenishment policies. Built by pure functions, written in one transaction; re-running changes nothing.
Master chuỗi cung ứng: toạ độ địa lý, hạng và vị trí shop, thuộc tính logistics sản phẩm, NCC, DC, hãng
vận chuyển, xe, khoảng cách, tuyến, lịch giao, sức chứa shop và chính sách bổ sung baseline. Dựng bằng hàm
thuần, ghi trong một transaction; chạy lại không đổi gì.

Run locally / Chạy local:  python -m jobs.kaizen_sim.master_supply [--apply]
"""
import argparse
import csv
import json
import math
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime

from . import db, geo, policy, rng
from .settings import (CARRIERS, CHAIN_START, CUTOFF_TIME, DCS, DELIVERY_DAYS, FLEET, GENERATOR_ID, ICT,
                       MAX_SERVING_KM, OPENING_COVER_DAYS, PARAMS, SEED, SHOP_MOVES, SHOP_OPEN_DATES,
                       SHOP_RECEIVING_SLOTS, SHOP_STORAGE_FACTOR, SHOP_STORAGE_MIN_M3, SUPPLIERS, TIER_SIZES, VEHICLE_TYPES,
                       WINDOW_CORE, WINDOW_OTHER)

REFERENCE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "schema_manager", "reference")

# (volume cm3, weight g, case packs) per sales unit / (thể tích cm3, khối lượng g, quy cách thùng) theo đơn vị bán
UNIT_SPECS = {
    "Hộp": ((120, 400), (60, 250), (20, 50, 100)),
    "Chai": ((150, 500), (120, 450), (24, 48)),
    "Vỉ": ((20, 60), (10, 30), (100, 200)),
    "Viên": ((2, 8), (1, 3), (500, 1000)),
}
COLD_SHARE = 0.06


@dataclass
class Ward:
    id: str
    name: str
    sub_region: str
    lat: float
    lon: float
    area_km2: float
    road: bool
    geometry: dict | None


@dataclass
class MasterInputs:
    wards: dict
    geo_rows: list
    # code -> (ward id, address) / mã -> (phường, địa chỉ)
    shops: dict
    staff: dict
    # (shop, product) -> opening qty / (shop, sản phẩm) -> số tồn đầu kỳ
    opening: dict
    # id -> dict(type, unit, stock_price, indication, brand)
    products: dict
    today: date


@dataclass
class MasterData:
    shops: dict = field(default_factory=dict)
    products: dict = field(default_factory=dict)
    suppliers: list = field(default_factory=list)
    supplier_products: list = field(default_factory=list)
    warehouses: list = field(default_factory=list)
    carriers: list = field(default_factory=list)
    carrier_rates: list = field(default_factory=list)
    vehicles: list = field(default_factory=list)
    distances: list = field(default_factory=list)
    lanes: list = field(default_factory=list)
    schedules: list = field(default_factory=list)
    capacities: list = field(default_factory=list)
    policies: list = field(default_factory=list)
    warnings: list = field(default_factory=list)


def load_reference(reference_dir: str = REFERENCE_DIR) -> tuple:
    """Wards keyed by Id plus the raw CSV rows. / Phường theo Id cùng các dòng CSV gốc."""
    with open(os.path.join(reference_dir, "geography_hcm_2025.csv"), encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    with open(os.path.join(reference_dir, "geography_hcm_2025.geojson"), encoding="utf-8") as f:
        shapes = {feat["properties"]["Id"]: feat["geometry"] for feat in json.load(f)["features"]}
    wards = {r["Id"]: Ward(r["Id"], r["Name"], r["SubRegion"], float(r["Latitude"]), float(r["Longitude"]),
                           float(r["AreaKm2"]), r["IsRoadAccessible"] == "true", shapes.get(r["Id"]))
             for r in rows}
    return wards, rows


def product_attributes(pid: str, unit: str, ptype: str) -> dict:
    (v_lo, v_hi), (w_lo, w_hi), packs = UNIT_SPECS.get(unit, UNIT_SPECS["Hộp"])
    life = (540, 1095) if ptype == "medicine" else (365, 730)
    return {
        "volume": rng.integer(v_lo, v_hi, pid, "vol"),
        "weight": rng.integer(w_lo, w_hi, pid, "kg"),
        "case_pack": rng.choice(packs, pid, "case"),
        "shelf_life": rng.integer(life[0], life[1], pid, "life"),
        "cold": rng.chance(COLD_SHARE, pid, "cold"),
    }


def supplier_of(product: dict) -> str:
    """Medicines by indication group, supplements by brand origin. / Thuốc theo nhóm chỉ định, TPCN theo xuất xứ."""
    if product["type"] == "medicine":
        text = (product.get("indication") or "").lower()
    else:
        m = re.search(r"\(([^)]*)\)", product.get("brand") or "")
        text = m.group(1) if m else ""
    for spec in SUPPLIERS:
        if any(key.lower() in text.lower() for key in spec.match):
            return spec.id
    return SUPPLIERS[-1].id


def assign_tiers(demand: dict) -> dict:
    """Top 10 shops by expected demand are A, the next 20 B, the rest C. / 10 shop nhu cầu cao nhất hạng A, 20 tiếp B, còn lại C."""
    ranked = sorted(demand, key=lambda s: (-demand[s], s))
    tiers, i = {}, 0
    for tier in ("A", "B", "C"):
        for code in ranked[i:i + TIER_SIZES[tier]]:
            tiers[code] = tier
        i += TIER_SIZES[tier]
    for code in ranked[i:]:
        tiers[code] = "C"
    return tiers


def review_days(tier: str) -> int:
    """Longest gap between two deliveries of the tier. / Khoảng cách dài nhất giữa hai lần giao của hạng."""
    days = sorted(DELIVERY_DAYS[tier])
    gaps = [(days[(i + 1) % len(days)] - d) % 7 or 7 for i, d in enumerate(days)]
    return max(gaps)


def build(inp: MasterInputs, p=PARAMS) -> MasterData:
    out = MasterData()
    wards = inp.wards

    # Shops: move island shops, place them, rank tiers / Shop: dời shop ngoài đảo, đặt toạ độ, xếp hạng
    demand = Counter()
    for (shop, _), qty in inp.opening.items():
        demand[shop] += qty
    tiers = assign_tiers({code: demand.get(code, 0) for code in inp.shops})
    nodes = {}
    for spec in DCS:
        w = wards[spec.ward_id]
        nodes[("DC", spec.id)] = (w.lat, w.lon, w.sub_region)
        out.warehouses.append((spec.id, spec.name, spec.ward_id, w.lat, w.lon, spec.storage_m3, spec.cold_storage_l,
                               spec.dock_slots))
    for code, (ward_id, address) in sorted(inp.shops.items()):
        if code in SHOP_MOVES:
            ward_id, address = SHOP_MOVES[code]
        w = wards[ward_id]
        if not w.road:
            out.warnings.append(f"{code} sits in {ward_id} without road access")
        lat, lon = geo.place_in_ward(code, w.lat, w.lon, w.area_km2, w.geometry)
        nodes[("SHOP", code)] = (lat, lon, w.sub_region)
        out.shops[code] = {"ward_id": ward_id, "address": address, "tier": tiers[code], "lat": lat, "lon": lon,
                           "sub_region": w.sub_region, "open_date": SHOP_OPEN_DATES.get(code, CHAIN_START)}

    # Distance matrix over every ordered pair / Ma trận khoảng cách cho mọi cặp có hướng
    legs = {}
    for a, (lat1, lon1, sub1) in nodes.items():
        for b, (lat2, lon2, sub2) in nodes.items():
            if a == b:
                continue
            leg = geo.leg(lat1, lon1, sub1, lat2, lon2, sub2)
            legs[(a, b)] = leg
            out.distances.append((a[0], a[1], b[0], b[1], leg["straight_km"], leg["road_km"], leg["circuity"],
                                  leg["off_peak_min"], max(leg["peak_min"], leg["off_peak_min"]), "HAVERSINE_FACTOR"))

    # Serving DC = nearest by road / DC phục vụ = gần nhất theo đường bộ
    for code, s in out.shops.items():
        dc = min((spec.id for spec in DCS), key=lambda d: (legs[(("DC", d), ("SHOP", code))]["road_km"], d))
        leg = legs[(("DC", dc), ("SHOP", code))]
        s["dc"] = dc
        if leg["road_km"] > MAX_SERVING_KM:
            out.warnings.append(f"{code} is {leg['road_km']} km from {dc}")
        window = WINDOW_CORE if s["sub_region"] == "SG_CORE" else WINDOW_OTHER
        out.lanes.append((f"LN-{dc}-{code}", dc, code, leg["straight_km"], max(leg["road_km"], leg["straight_km"]),
                          max(1, leg["off_peak_min"]), window[0], window[1], "CAR01"))
        for weekday in DELIVERY_DAYS[s["tier"]]:
            out.schedules.append((f"DS-{code}-{weekday}", code, dc, weekday, CUTOFF_TIME,
                                  max(CHAIN_START, s["open_date"]), None))

    # Products and suppliers / Sản phẩm và nhà cung cấp
    for pid, prod in sorted(inp.products.items()):
        attrs = product_attributes(pid, prod["unit"], prod["type"])
        attrs["supplier"] = supplier_of(prod)
        out.products[pid] = attrs
    for spec in SUPPLIERS:
        lead = round(rng.uniform(4, 10, spec.id, "lead") * 2) / 2
        cv = rng.uniform(0.2, 0.5, spec.id, "cv")
        out.suppliers.append((spec.id, spec.name, lead, round(lead * cv, 2), round(rng.uniform(0.90, 0.99, spec.id, "fill"), 3),
                              rng.integer(5, 20, spec.id, "mov") * 1_000_000))
    lead_of = {s[0]: (s[2], s[3]) for s in out.suppliers}
    for pid, attrs in sorted(out.products.items()):
        out.supplier_products.append((attrs["supplier"], pid, inp.products[pid]["stock_price"], attrs["case_pack"],
                                      attrs["case_pack"], True, CHAIN_START, None))

    # Carriers and fleet / Hãng vận chuyển và đội xe
    for c in CARRIERS:
        out.carriers.append((c.id, c.name, c.type, c.fixed, c.per_km, c.per_kg, c.per_stop, c.on_time, c.cold,
                             c.weekly_kg, c.contract_min_kg))
        out.carrier_rates.append((f"CR-{c.id}-{CHAIN_START:%Y%m%d}", c.id, c.fixed, c.per_km, c.per_kg, c.per_stop,
                                  CHAIN_START, None))
    for carrier, vtype, home, count in FLEET:
        spec = VEHICLE_TYPES[vtype]
        for n in range(1, count + 1):
            out.vehicles.append((f"{carrier}-{home or 'POOL'}-{vtype}-{n:02d}", carrier, vtype, spec.capacity_kg,
                                 spec.capacity_m3, spec.gross_t, spec.cold, home, "ACTIVE"))

    # Baseline policies and shop capacity / Chính sách baseline và sức chứa shop
    dc_mu, dc_var = defaultdict(float), defaultdict(float)
    level_by_shop = defaultdict(dict)
    for (shop, pid), qty in sorted(inp.opening.items()):
        if qty <= 0 or shop not in out.shops:
            continue
        s = out.shops[shop]
        mu = qty / OPENING_COVER_DAYS
        sigma = policy.demand_sigma(mu)
        r = review_days(s["tier"])
        lv = policy.shop_levels(mu, sigma, r, p.shop_lead_days, p.z)
        level_by_shop[shop][pid] = lv
        out.policies.append((f"RP-{shop}-{pid}-BASE", "SHOP", shop, pid, "RsS", r, lv.reorder_point, lv.order_up_to,
                             lv.safety_stock))
        dc_mu[(s["dc"], pid)] += mu
        dc_var[(s["dc"], pid)] += sigma ** 2
    for (dc, pid), mu in sorted(dc_mu.items()):
        lead_mean, lead_std = lead_of[out.products[pid]["supplier"]]
        lv = policy.dc_levels(mu, math.sqrt(dc_var[(dc, pid)]), lead_mean, lead_std, p.dc_cover_days, p.z)
        out.policies.append((f"RP-{dc}-{pid}-BASE", "DC", dc, pid, "ROP", None, lv.reorder_point, lv.order_up_to,
                             lv.safety_stock))

    for code, s in sorted(out.shops.items()):
        levels = level_by_shop.get(code, {})
        dry = sum(lv.order_up_to * out.products[pid]["volume"] for pid, lv in levels.items()
                  if not out.products[pid]["cold"]) / 1e6
        cold_l = sum(lv.order_up_to * out.products[pid]["volume"] for pid, lv in levels.items()
                     if out.products[pid]["cold"]) / 1e3
        storage = max(SHOP_STORAGE_MIN_M3[s["tier"]], math.ceil(dry * SHOP_STORAGE_FACTOR * 2) / 2)
        out.capacities.append((f"SC-{code}-{max(CHAIN_START, s['open_date']):%Y%m%d}", code, storage,
                               int(math.ceil(cold_l * SHOP_STORAGE_FACTOR / 10) * 10),
                               max(1, int(len(levels) * 1.3)), SHOP_RECEIVING_SLOTS[s["tier"]],
                               inp.staff.get(code, 0), max(CHAIN_START, s["open_date"]), None))
        s["storage_m3"] = storage
    return out


def read_inputs(conn, reference_dir: str = REFERENCE_DIR) -> MasterInputs:
    wards, geo_rows = load_reference(reference_dir)
    with conn.cursor() as cur:
        cur.execute('SELECT "Code", "WardId", "Address" FROM shop."Shop"')
        shops = {c: (w, a) for c, w, a in cur.fetchall()}
        cur.execute('SELECT "ShopCode", count(*) FROM shop."Employee" GROUP BY 1')
        staff = dict(cur.fetchall())
        cur.execute("""SELECT "LocationId", "ProductId", sum("Qty") FROM inventory."StockMovement"
                       WHERE "MovementType" = 'ADJUST' AND "Reason" = 'OPENING' AND "LocationType" = 'SHOP'
                       GROUP BY 1, 2""")
        opening = {(s, pid): int(q) for s, pid, q in cur.fetchall()}
        cur.execute("""SELECT p."Id", p."Type", p."Unit", p."StockPrice", m."Indication", s."Brand"
                       FROM product."Product" p
                       LEFT JOIN product."ProductMedicine" m ON m."ProductId" = p."Id"
                       LEFT JOIN product."ProductSupplement" s ON s."ProductId" = p."Id" """)
        products = {r[0]: {"type": r[1], "unit": r[2], "stock_price": int(r[3]), "indication": r[4], "brand": r[5]}
                    for r in cur.fetchall()}
    conn.rollback()
    return MasterInputs(wards, geo_rows, shops, staff, opening, products, datetime.now(ICT).date())


def write(cur, inp: MasterInputs, data: MasterData) -> dict:
    """Write everything; returns rows inserted or changed per table. / Ghi tất cả; trả về số dòng chèn/đổi theo bảng."""
    n = {}
    geo_cols = ("AdminCode", "WardType", "ProvinceCode", "LegacyProvince", "SubRegion", "Latitude", "Longitude",
                "AreaKm2", "IsRoadAccessible", "CoordMethod", "CoordSource")
    geo_types = ("text", "text", "text", "text", "text", "numeric", "numeric", "numeric", "boolean", "text", "text")
    n["Geography"] = db.update_rows(cur, 'public."Geography"', "Id", geo_cols, geo_types,
                                    [(r["Id"],) + tuple(r[c] for c in geo_cols) for r in inp.geo_rows])

    n["Warehouse"] = db.upsert(cur, 'inventory."Warehouse"', ("Id",),
                               ("Name", "WardId", "Latitude", "Longitude", "StorageM3", "ColdStorageL",
                                "DockSlotsPerDay"), data.warehouses)
    shop_cols = ("WardId", "Address", "Tier", "OpenDate", "ServingWarehouseId", "Latitude", "Longitude")
    n["Shop"] = db.update_rows(cur, 'shop."Shop"', "Code", shop_cols,
                               ("text", "text", "char(1)", "date", "text", "numeric", "numeric"),
                               [(c, s["ward_id"], s["address"], s["tier"], s["open_date"], s["dc"], s["lat"], s["lon"])
                                for c, s in sorted(data.shops.items())])
    n["Supplier"] = db.upsert(cur, 'procurement."Supplier"', ("Id",),
                              ("Name", "LeadTimeMeanDays", "LeadTimeStdDays", "FillRate", "MinOrderValue"),
                              data.suppliers)
    prod_cols = ("SupplierId", "UnitVolumeCm3", "UnitWeightG", "CasePack", "ShelfLifeDays", "IsColdChain",
                 "LaunchDate")
    n["Product"] = db.update_rows(cur, 'product."Product"', "Id", prod_cols,
                                  ("text", "integer", "integer", "integer", "integer", "boolean", "date"),
                                  [(pid, a["supplier"], a["volume"], a["weight"], a["case_pack"], a["shelf_life"],
                                    a["cold"], CHAIN_START) for pid, a in sorted(data.products.items())])
    n["SupplierProduct"] = db.upsert(cur, 'procurement."SupplierProduct"', ("SupplierId", "ProductId"),
                                     ("UnitCost", "CasePack", "MinOrderQty", "IsPrimary", "EffectiveFrom",
                                      "EffectiveTo"), data.supplier_products)
    n["Carrier"] = db.upsert(cur, 'logistics."Carrier"', ("Id",),
                             ("Name", "Type", "FixedCostPerTrip", "CostPerKm", "CostPerKg", "CostPerStop",
                              "BaseOnTimeRate", "SupportsColdChain", "WeeklyCapacityKg", "ContractMinKgPerWeek"),
                             data.carriers)
    n["CarrierRate"] = db.upsert(cur, 'logistics."CarrierRate"', ("Id",),
                                 ("CarrierId", "FixedCostPerTrip", "CostPerKm", "CostPerKg", "CostPerStop",
                                  "EffectiveFrom", "EffectiveTo"), data.carrier_rates)
    n["Vehicle"] = db.upsert(cur, 'logistics."Vehicle"', ("Id",),
                             ("CarrierId", "VehicleType", "CapacityKg", "CapacityM3", "GrossWeightT", "IsCold",
                              "HomeWarehouseId", "Status"), data.vehicles)
    n["DistanceMatrix"] = db.upsert(cur, 'logistics."DistanceMatrix"', ("FromType", "FromId", "ToType", "ToId"),
                                    ("StraightKm", "RoadKm", "CircuityFactor", "OffPeakMinutes", "PeakMinutes",
                                     "Method"), data.distances)
    n["Lane"] = db.upsert(cur, 'logistics."Lane"', ("Id",),
                          ("WarehouseId", "ShopCode", "StraightKm", "RoadKm", "BaseTransitMinutes",
                           "DeliveryWindowFrom", "DeliveryWindowTo", "DefaultCarrierId"), data.lanes)
    n["DeliverySchedule"] = db.upsert(cur, 'logistics."DeliverySchedule"', ("Id",),
                                      ("ShopCode", "WarehouseId", "Weekday", "CutoffTime", "EffectiveFrom",
                                       "EffectiveTo"), data.schedules)
    n["ShopCapacity"] = db.upsert(cur, 'shop."ShopCapacity"', ("Id",),
                                  ("ShopCode", "StorageM3", "ColdStorageL", "ShelfSlots", "ReceivingSlotsPerDay",
                                   "StaffCount", "EffectiveFrom", "EffectiveTo"), data.capacities)

    # Baseline policies are written once; later versions come from the weekly refresh or the optimizer
    # Chính sách baseline chỉ ghi một lần; phiên bản sau do làm mới hằng tuần hoặc bộ tối ưu ghi
    cur.execute('SELECT count(*) FROM inventory."ReplenishmentPolicy"')
    if cur.fetchone()[0] == 0:
        rows = [r + ("BASELINE", None, inp.today, None) for r in data.policies]
        n["ReplenishmentPolicy"] = db.upsert(cur, 'inventory."ReplenishmentPolicy"', ("Id",),
                                             ("LocationType", "LocationId", "ProductId", "PolicyType",
                                              "ReviewPeriodDays", "ReorderPoint", "OrderUpTo", "SafetyStock",
                                              "Source", "SolverRunId", "EffectiveFrom", "EffectiveTo"), rows)
    else:
        n["ReplenishmentPolicy"] = 0

    cur.execute('INSERT INTO datagen."GeneratorConfig" ("Id", "Name", "Seed", "StartDate", "Params", "CreatedDate") '
                'VALUES (%s, %s, %s, %s, %s, now()) ON CONFLICT ("Id") DO UPDATE SET "Params" = EXCLUDED."Params"',
                (GENERATOR_ID, "Supply chain live simulation", SEED, CHAIN_START, json.dumps(PARAMS.to_json())))
    return n


def summary(data: MasterData) -> dict:
    tiers = Counter(s["tier"] for s in data.shops.values())
    by_dc = Counter(s["dc"] for s in data.shops.values())
    lane_km = sorted(l[4] for l in data.lanes)
    return {
        "shops_by_tier": dict(tiers),
        "shops_by_dc": dict(by_dc),
        "lane_road_km_min_median_max": (lane_km[0], lane_km[len(lane_km) // 2], lane_km[-1]),
        "suppliers": len(data.suppliers),
        "products_by_supplier": dict(Counter(a["supplier"] for a in data.products.values())),
        "cold_products": sum(a["cold"] for a in data.products.values()),
        "vehicles": len(data.vehicles),
        "distance_rows": len(data.distances),
        "lanes": len(data.lanes),
        "schedules": len(data.schedules),
        "policies_shop_dc": (sum(r[1] == "SHOP" for r in data.policies), sum(r[1] == "DC" for r in data.policies)),
        "shop_storage_m3_by_tier": {t: sorted(s["storage_m3"] for s in data.shops.values() if s["tier"] == t)
                                    for t in ("A", "B", "C")},
        "warnings": data.warnings,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("plan", "dry-run", "apply"), default="plan",
                        help="plan: build only; dry-run: write then roll back; apply: commit")
    args = parser.parse_args()
    counts = {}
    with db.connection() as conn:
        inp = read_inputs(conn)
        data = build(inp)
        if args.mode != "plan":
            with conn.cursor() as cur:
                counts = write(cur, inp, data)
            if args.mode == "apply":
                conn.commit()
            else:
                conn.rollback()
    print(json.dumps({"mode": args.mode, "changed": counts, "summary": summary(data)}, ensure_ascii=False,
                     indent=1, default=str))


if __name__ == "__main__":
    main()
