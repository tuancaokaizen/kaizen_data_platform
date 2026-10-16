"""Stateless random draws keyed by (seed, parts): the same key always gives the same value, so any step
can be re-run or caught up without storing generator state.
Phép rút ngẫu nhiên không trạng thái theo khoá (seed, parts): cùng khoá luôn cho cùng giá trị, nên mọi bước
chạy lại hoặc chạy bù được mà không cần lưu trạng thái bộ sinh.
"""
import hashlib
import math

from .settings import SEED


def unit(*parts) -> float:
    """Uniform draw in [0, 1). / Giá trị đều trong [0, 1)."""
    key = "|".join(str(p) for p in (SEED,) + parts).encode()
    return int.from_bytes(hashlib.sha256(key).digest()[:8], "big") / 2.0 ** 64


def uniform(lo: float, hi: float, *parts) -> float:
    return lo + (hi - lo) * unit(*parts)


def integer(lo: int, hi: int, *parts) -> int:
    """Integer in [lo, hi] inclusive. / Số nguyên trong [lo, hi], gồm hai đầu."""
    return lo + min(hi - lo, int(unit(*parts) * (hi - lo + 1)))


def normal(mean: float, std: float, *parts) -> float:
    """Box-Muller over two keyed uniforms. / Box-Muller trên hai giá trị đều theo khoá."""
    u1 = max(unit(*parts, "n1"), 1e-12)
    u2 = unit(*parts, "n2")
    return mean + std * math.sqrt(-2.0 * math.log(u1)) * math.cos(2.0 * math.pi * u2)


def choice(options, *parts):
    return options[integer(0, len(options) - 1, *parts)]


def chance(p: float, *parts) -> bool:
    return unit(*parts) < p
