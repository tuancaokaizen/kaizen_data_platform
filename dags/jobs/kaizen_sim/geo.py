"""Distances, travel times and point placement inside ward polygons (data_model_v2.md 3.6).
Khoảng cách, thời gian chạy và đặt điểm trong ranh giới phường (data_model_v2.md 3.6).
"""
import math
from datetime import datetime, timedelta

from . import rng
from .settings import (CIRCUITY_CROSS, CIRCUITY_CROSS_HIGH, CIRCUITY_SAME, PEAK_HOURS, SPEED_INTER,
                       SPEED_SUBURB, SPEED_URBAN)

EARTH_KM = 6371.0088
KM_PER_DEG_LAT = 110.574


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_KM * math.asin(math.sqrt(a))


def circuity(sub_a: str, sub_b: str) -> float:
    if sub_a == sub_b:
        return CIRCUITY_SAME.get(sub_a, CIRCUITY_CROSS)
    if "SG_CORE" in (sub_a, sub_b) or "BA_RIA_VUNG_TAU" in (sub_a, sub_b):
        return CIRCUITY_CROSS_HIGH
    return CIRCUITY_CROSS


def speeds(sub_a: str, sub_b: str) -> tuple:
    """(off-peak, peak) km/h. / (thấp điểm, cao điểm) km/h."""
    if sub_a != sub_b:
        return SPEED_INTER
    return SPEED_URBAN if sub_a == "SG_CORE" else SPEED_SUBURB


def leg(lat1, lon1, sub1, lat2, lon2, sub2) -> dict:
    """One DistanceMatrix row body. / Thân một dòng DistanceMatrix."""
    straight = haversine_km(lat1, lon1, lat2, lon2)
    factor = circuity(sub1, sub2)
    road = straight * factor
    off, peak = speeds(sub1, sub2)
    return {
        "straight_km": round(straight, 2),
        "road_km": round(road, 2),
        "circuity": factor,
        "off_peak_min": int(round(road / off * 60)),
        "peak_min": int(round(road / peak * 60)),
    }


def is_peak(at: datetime) -> bool:
    return any(lo <= at.hour < hi for lo, hi in PEAK_HOURS)


def drive_minutes(off_peak_min: int, peak_min: int, start: datetime) -> int:
    """Travel time chosen by the clock at departure. / Thời gian chạy theo giờ lúc xuất phát."""
    return peak_min if is_peak(start) else off_peak_min


def add_minutes(at: datetime, minutes: float) -> datetime:
    return at + timedelta(minutes=minutes)


def _in_ring(lon: float, lat: float, ring) -> bool:
    inside = False
    j = len(ring) - 1
    for i in range(len(ring)):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if (yi > lat) != (yj > lat) and lon < (xj - xi) * (lat - yi) / (yj - yi) + xi:
            inside = not inside
        j = i
    return inside


def in_geometry(lon: float, lat: float, geometry: dict) -> bool:
    """Point-in-polygon for GeoJSON Polygon / MultiPolygon with holes.
    Kiểm tra điểm trong Polygon / MultiPolygon GeoJSON, có xét lỗ thủng.
    """
    polygons = [geometry["coordinates"]] if geometry["type"] == "Polygon" else geometry["coordinates"]
    for rings in polygons:
        if _in_ring(lon, lat, rings[0]) and not any(_in_ring(lon, lat, hole) for hole in rings[1:]):
            return True
    return False


def place_in_ward(key: str, lat: float, lon: float, area_km2: float, geometry: dict | None) -> tuple:
    """A point within min(0.5 * sqrt(area / pi), 1.5) km of the ward centre and inside its polygon;
    falls back to the centre when 200 draws miss.
    Một điểm cách tâm phường không quá min(0.5 * sqrt(diện tích / pi), 1.5) km và nằm trong ranh giới;
    quay về tâm phường khi 200 lần rút đều trượt.
    """
    radius = min(0.5 * math.sqrt(max(area_km2, 0.01) / math.pi), 1.5)
    km_per_deg_lon = 111.320 * math.cos(math.radians(lat))
    for attempt in range(200):
        r = radius * math.sqrt(rng.unit(key, "r", attempt))
        theta = 2 * math.pi * rng.unit(key, "t", attempt)
        p_lat = lat + r * math.sin(theta) / KM_PER_DEG_LAT
        p_lon = lon + r * math.cos(theta) / km_per_deg_lon
        if geometry is None or in_geometry(p_lon, p_lat, geometry):
            return round(p_lat, 6), round(p_lon, 6)
    return round(lat, 6), round(lon, 6)
