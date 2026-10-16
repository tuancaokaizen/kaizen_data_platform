"""Fixed parameters of the supply simulation (supply_chain_forecast_optimization.md 4, data_model_v2.md 3.6).
Tham số cố định của mô phỏng cung ứng (supply_chain_forecast_optimization.md 4, data_model_v2.md 3.6).
"""
from dataclasses import asdict, dataclass, field
from datetime import date, time, timedelta, timezone

ICT = timezone(timedelta(hours=7))

GENERATOR_ID = "SUPPLY_LIVE"
SEED = 20261012
# History of the chain starts here; the E6 shops open later / Lịch sử chuỗi bắt đầu từ đây; shop E6 mở muộn hơn
CHAIN_START = date(2024, 1, 1)

# Opening stock was sized as this many days of demand / Tồn đầu kỳ được tính bằng ngần này ngày nhu cầu
OPENING_COVER_DAYS = 60

SUB_REGIONS = ("SG_CORE", "SG_OUTER", "BINH_DUONG", "BA_RIA_VUNG_TAU", "CON_DAO")


@dataclass(frozen=True)
class DcSpec:
    id: str
    name: str
    ward_id: str
    storage_m3: float
    cold_storage_l: int
    dock_slots: int


DCS = (
    DcSpec("DC01", "Kho tổng Tân Tạo", "W093", 4000.0, 20000, 12),
    DcSpec("DC02", "Kho tổng Bến Cát", "W013", 3000.0, 12000, 8),
    DcSpec("DC03", "Kho tổng Bà Rịa", "W010", 2000.0, 8000, 6),
)

# Island shops move to the mainland (no ferry lanes) / Shop ngoài đảo dời vào đất liền (không có tuyến phà)
SHOP_MOVES = {
    "SHOP0011": ("W126", "Đường Rừng Sác, Xã Bình Khánh, TP. Hồ Chí Minh"),
    "SHOP0035": ("W150", "Đường Nguyễn Bình, Xã Nhà Bè, TP. Hồ Chí Minh"),
}

# E6: shops opened during the history / Shop mở mới trong giai đoạn lịch sử
SHOP_OPEN_DATES = {
    "SHOP0045": date(2024, 6, 1),
    "SHOP0046": date(2024, 9, 15),
    "SHOP0047": date(2025, 1, 5),
    "SHOP0048": date(2025, 6, 20),
    "SHOP0049": date(2026, 1, 10),
    "SHOP0050": date(2026, 4, 1),
}

TIER_SIZES = {"A": 10, "B": 20, "C": 20}
# ISO weekdays (1 = Monday), spread so no weekday carries all tiers / Thứ ISO, rải để không ngày nào gánh mọi hạng
DELIVERY_DAYS = {"A": (1, 3, 5), "B": (2, 6), "C": (4,)}
CUTOFF_TIME = time(16, 0)

# Delivery windows: core wards only take light vans in the daytime / Khung giao: phường nội đô chỉ nhận xe nhẹ ban ngày
WINDOW_CORE = (time(9, 0), time(16, 0))
WINDOW_OTHER = (time(7, 0), time(17, 0))
CORE_MAX_GROSS_T = 2.5

# Tight shop storage so capacity binds sometimes / Kho shop chật để đôi khi vướng sức chứa
SHOP_STORAGE_FACTOR = 1.25
# Back-room floor per tier in m3 / Sàn kho sau theo tier (m3)
SHOP_STORAGE_MIN_M3 = {"A": 2.5, "B": 2.0, "C": 1.5}
SHOP_RECEIVING_SLOTS = {"A": 2, "B": 1, "C": 1}

MAX_SERVING_KM = 60.0


@dataclass(frozen=True)
class SupplierSpec:
    id: str
    name: str
    # Products routed here: supplement brand origins or medicine indication groups
    # Sản phẩm thuộc NCC: xuất xứ thương hiệu TPCN hoặc nhóm chỉ định của thuốc
    match: tuple


SUPPLIERS = (
    SupplierSpec("SUP01", "Công ty CP Dược phẩm Trung ương Codupha", ("cơ xương khớp",)),
    SupplierSpec("SUP02", "Công ty CP Dược Hậu Giang - Chi nhánh HCM", ("thần kinh",)),
    SupplierSpec("SUP03", "Công ty TNHH Zuellig Pharma Việt Nam", ("tim mạch",)),
    SupplierSpec("SUP04", "Công ty CP Dược phẩm Imexpharm", ("hô hấp",)),
    SupplierSpec("SUP05", "Công ty CP Traphaco - Chi nhánh miền Nam", ("tiêu hóa",)),
    SupplierSpec("SUP06", "Công ty TNHH DKSH Việt Nam", ("da liễu",)),
    SupplierSpec("SUP07", "Công ty TNHH Nhập khẩu TPCN Hoa Kỳ Phương Nam", ("Mỹ",)),
    SupplierSpec("SUP08", "Công ty TNHH Thương mại Nhật Việt Health", ("Nhật Bản",)),
    SupplierSpec("SUP09", "Công ty TNHH Úc Châu Pharma", ("Úc",)),
    SupplierSpec("SUP10", "Công ty CP Phân phối Dược phẩm Sài Gòn", ("Việt Nam", "Pháp")),
)


@dataclass(frozen=True)
class CarrierSpec:
    id: str
    name: str
    type: str
    fixed: int
    per_km: int
    per_kg: int
    per_stop: int
    on_time: float
    cold: bool
    weekly_kg: int
    contract_min_kg: int


CARRIERS = (
    CarrierSpec("CAR01", "Đội xe Kaizen", "INHOUSE", 600_000, 6_000, 0, 30_000, 0.95, False, 60_000, 0),
    CarrierSpec("CAR02", "Giao Nhanh Tiết Kiệm 3PL", "3PL", 250_000, 9_000, 150, 40_000, 0.85, False, 80_000, 0),
    CarrierSpec("CAR03", "Vận tải Phương Nam 3PL", "3PL", 350_000, 8_000, 100, 35_000, 0.92, False, 40_000, 5_000),
    CarrierSpec("CAR04", "Hoả Tốc Lạnh Express", "EXPRESS", 500_000, 12_000, 300, 50_000, 0.97, True, 10_000, 0),
)
ON_DEMAND_CARRIER = "CAR02"


@dataclass(frozen=True)
class VehicleType:
    code: str
    capacity_kg: int
    capacity_m3: float
    gross_t: float
    cold: bool


VEHICLE_TYPES = {
    "VAN_1T": VehicleType("VAN_1T", 1000, 6.0, 1.9, False),
    "TRUCK_2_5T": VehicleType("TRUCK_2_5T", 2500, 14.0, 4.9, False),
    "COLD_VAN": VehicleType("COLD_VAN", 800, 4.0, 1.9, True),
}

# (carrier, vehicle type, home DC or None for the shared pool, count)
# (hãng, loại xe, DC nhà hoặc None nếu dùng chung, số xe)
FLEET = (
    ("CAR01", "VAN_1T", "DC01", 3), ("CAR01", "TRUCK_2_5T", "DC01", 1),
    ("CAR01", "VAN_1T", "DC02", 2), ("CAR01", "TRUCK_2_5T", "DC02", 1),
    ("CAR01", "VAN_1T", "DC03", 1), ("CAR01", "TRUCK_2_5T", "DC03", 1),
    ("CAR02", "TRUCK_2_5T", None, 2),
    ("CAR03", "VAN_1T", None, 2),
    ("CAR04", "COLD_VAN", None, 2),
)

# Circuity (road km / straight km) and speeds in km/h (off-peak, peak), data_model_v2.md 3.6
# Hệ số đường vòng (km đường / km chim bay) và vận tốc km/h (thấp điểm, cao điểm), data_model_v2.md 3.6
CIRCUITY_SAME = {"SG_CORE": 1.35, "SG_OUTER": 1.25, "BINH_DUONG": 1.25, "BA_RIA_VUNG_TAU": 1.30}
CIRCUITY_CROSS_HIGH = 1.30
CIRCUITY_CROSS = 1.25
SPEED_URBAN = (28.0, 18.0)
SPEED_SUBURB = (40.0, 30.0)
SPEED_INTER = (50.0, 40.0)
STOP_SERVICE_MIN = 15
PEAK_HOURS = ((7, 9), (16, 19))


@dataclass(frozen=True)
class SupplyParams:
    """Tunable policy and execution parameters, stored in datagen.GeneratorConfig.
    Tham số chính sách và vận hành có thể chỉnh, lưu trong datagen.GeneratorConfig.
    """
    z: float = 1.28
    shop_lead_days: float = 1.0
    dc_cover_days: float = 14.0
    ma_window_days: int = 28
    min_history_days: int = 14
    min_shelf_life_at_receipt_days: int = 30
    # Rebaseline target = s + (S - s) * U(lo, hi) / Mức tồn sau rebaseline = s + (S - s) * U(lo, hi)
    rebaseline_lo: float = 0.3
    rebaseline_hi: float = 1.0
    # Cap DC stock after rebaseline at this many days of network demand, None = keep all
    # Giới hạn tồn DC sau rebaseline bằng ngần này ngày nhu cầu toàn mạng, None = giữ hết
    dc_cap_days: float | None = None
    depart_hour: int = 6
    pick_lead_min: int = 60
    max_stops: int = 4
    delay_threshold_min: int = 30
    partial_stop_prob: float = 0.04
    vehicle_breakdown_share: float = 0.12
    po_receipt_hours: tuple = (9, 15)
    po_short_extra: float = 2.0
    lot_life_used: tuple = (0.6, 0.9)
    extra: dict = field(default_factory=dict)

    def to_json(self) -> dict:
        return asdict(self)


PARAMS = SupplyParams()
