"""Baseline replenishment levels (supply_chain_forecast_optimization.md 4.2): deliberately naive so the
optimizer has room to improve.
Mức bổ sung baseline (supply_chain_forecast_optimization.md 4.2): cố ý "ngây thơ" để bộ tối ưu có chỗ cải thiện.
"""
import math
from dataclasses import dataclass


@dataclass(frozen=True)
class Levels:
    reorder_point: int
    order_up_to: int
    safety_stock: int


def shop_levels(mu: float, sigma: float, review_days: float, lead_days: float, z: float) -> Levels:
    """Periodic review (R, s, S): s = mu(L+R) + z sigma sqrt(L+R), S = s + mu R.
    Xem xét định kỳ (R, s, S): s = mu(L+R) + z sigma sqrt(L+R), S = s + mu R.
    """
    horizon = lead_days + review_days
    safety = z * sigma * math.sqrt(horizon)
    s = math.ceil(mu * horizon + safety)
    big_s = max(s, math.ceil(s + mu * review_days))
    return Levels(s, big_s, math.ceil(safety))


def dc_levels(mu: float, sigma: float, lead_mean: float, lead_std: float, cover_days: float, z: float) -> Levels:
    """Continuous review ROP with lead-time variance: safety = z sqrt(L sigma^2 + mu^2 sigmaL^2), S = s + cover mu.
    ROP xem xét liên tục có biến thiên lead time: safety = z sqrt(L sigma^2 + mu^2 sigmaL^2), S = s + cover mu.
    """
    safety = z * math.sqrt(lead_mean * sigma ** 2 + (mu * lead_std) ** 2)
    s = math.ceil(mu * lead_mean + safety)
    return Levels(s, max(s, math.ceil(s + mu * cover_days)), math.ceil(safety))


def demand_sigma(mu: float) -> float:
    """Default daily std when there is no history: overdispersed Poisson (variance = 2 mu).
    Độ lệch chuẩn ngày mặc định khi chưa có lịch sử: Poisson phân tán rộng (phương sai = 2 mu).
    """
    return math.sqrt(2.0 * max(mu, 0.0))


def order_qty(position: int, levels: Levels) -> int:
    """Order up to S once the inventory position is at or below s. / Đặt lên S khi vị thế tồn <= s."""
    if position > levels.reorder_point:
        return 0
    return max(0, levels.order_up_to - position)


def round_to_case(qty: int, case_pack: int, min_qty: int = 0) -> int:
    if qty <= 0:
        return 0
    qty = max(qty, min_qty)
    return int(math.ceil(qty / case_pack) * case_pack)


def scale_to_capacity(lines: dict, volume_cm3: dict, used_m3: float, capacity_m3: float) -> dict:
    """Shrink order lines proportionally so used + ordered volume fits the store; lines are product -> qty.
    Co các dòng đặt theo tỷ lệ để thể tích đang dùng + đặt thêm vừa kho; lines là sản phẩm -> số lượng.
    """
    ordered_m3 = sum(q * volume_cm3.get(p, 0) for p, q in lines.items()) / 1e6
    free = capacity_m3 - used_m3
    if ordered_m3 <= free or ordered_m3 <= 0:
        return dict(lines)
    ratio = max(free, 0.0) / ordered_m3
    return {p: int(q * ratio) for p, q in lines.items() if int(q * ratio) > 0}


def allocate(requests: dict, available: int) -> dict:
    """Share scarce DC stock across shops proportionally, largest remainders first; requests are shop -> qty.
    Chia tồn DC thiếu cho các shop theo tỷ lệ, phần dư lớn nhất nhận trước; requests là shop -> số lượng.
    """
    total = sum(requests.values())
    if total <= available:
        return dict(requests)
    if available <= 0:
        return {k: 0 for k in requests}
    exact = {k: q * available / total for k, q in requests.items()}
    grant = {k: int(v) for k, v in exact.items()}
    left = available - sum(grant.values())
    for k in sorted(exact, key=lambda k: (-(exact[k] - grant[k]), k))[:left]:
        grant[k] += 1
    return grant
