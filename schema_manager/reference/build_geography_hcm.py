"""Build ward coordinates and simplified boundaries for the 168 HCMC wards.
Sinh toạ độ và ranh giới rút gọn cho 168 phường/xã của TP.HCM.

Source / Nguồn: thanglequoc/vietnamese-provinces-database (MIT), folder
json/geojson/79_ho_chi_minh/wards/, derived from the "Bản đồ hành chính
Việt Nam" reference map (2025, two-tier administration).

Download raw files first / Tải file gốc trước:
    mkdir -p .cache/hcm_wards
    curl -sSL "https://api.github.com/repos/thanglequoc/vietnamese-provinces-database/git/trees/master?recursive=1" \
      | python3 -c "import json,sys; [print(x['path']) for x in json.load(sys.stdin)['tree'] \
          if x['path'].startswith('json/geojson/79_ho_chi_minh/wards/') and x['path'].endswith('.geojson')]" \
      | xargs -P 12 -I{} sh -c 'curl -sSfL "https://raw.githubusercontent.com/thanglequoc/vietnamese-provinces-database/master/{}" -o ".cache/hcm_wards/$(basename {})"'

Run / Chạy (needs shapely):
    python schema_manager/reference/build_geography_hcm.py --src-dir .cache/hcm_wards
"""

import argparse
import csv
import glob
import json
import os
import unicodedata

from shapely.geometry import mapping, shape

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUT_DIR = os.path.dirname(os.path.abspath(__file__))
# Ordered W001..W168 names (was embedded in legacy generate_master_data.py)
# Danh sách tên W001..W168 (trước đây nhúng trong generate_master_data.py cũ)
WARD_ORDER_TXT = os.path.join(OUT_DIR, "geography_wards_168.txt")
OUT_CSV = os.path.join(OUT_DIR, "geography_hcm_2025.csv")
OUT_GEOJSON = os.path.join(OUT_DIR, "geography_hcm_2025.geojson")

# ~30 m tolerance keeps shapes usable for point-in-polygon / Sai số ~30 m, vẫn đủ cho kiểm tra điểm trong phường
SIMPLIFY_TOLERANCE_DEG = 0.0003

# Island units without road access / Đơn vị đảo, không có đường bộ
NOT_ROAD_ACCESSIBLE = {"Đặc khu Côn Đảo", "Xã Thạnh An"}

# Old-tier tone marks (hoà) vs new (hòa) / Chuẩn hoá vị trí dấu thanh kiểu cũ và mới
_TONE_FIX = {
    "oà": "òa", "oá": "óa", "oả": "ỏa", "oã": "õa", "oạ": "ọa",
    "oè": "òe", "oé": "óe", "oẻ": "ỏe", "oẽ": "õe", "oẹ": "ọe",
    "uỳ": "ùy", "uý": "úy", "uỷ": "ủy", "uỹ": "ũy", "uỵ": "ụy",
}


def normalize_name(name: str) -> str:
    """Normalize a Vietnamese unit name for matching.
    Chuẩn hoá tên đơn vị hành chính để so khớp.
    """
    s = unicodedata.normalize("NFC", name).strip().lower()
    for old, new in _TONE_FIX.items():
        s = s.replace(old, new)
    return s


def load_project_wards() -> list:
    """Read the ordered ward list used for W001..W168 ids.
    Đọc danh sách phường theo đúng thứ tự sinh mã W001..W168.
    """
    with open(WARD_ORDER_TXT, encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def legacy_province(code: int) -> str:
    """Map an official ward code to its pre-2025 province by GSO code range.
    Suy ra tỉnh cũ (trước 2025) từ dải mã đơn vị hành chính của TCTK.
    """
    if 25741 <= code <= 26038:
        return "BINH_DUONG"
    if 26506 <= code <= 26733:
        return "BA_RIA_VUNG_TAU"
    if 26734 <= code <= 27694:
        return "HO_CHI_MINH"
    raise ValueError(f"Unexpected ward code {code}")


def ward_type(full_name: str) -> str:
    if full_name.startswith("Phường"):
        return "PHUONG"
    if full_name.startswith("Xã"):
        return "XA"
    return "DAC_KHU"


def sub_region(full_name: str, wtype: str, legacy: str) -> str:
    """Planning sub-region used for circuity and speed factors.
    Tiểu vùng dùng cho hệ số đường vòng và tốc độ.
    """
    if full_name == "Đặc khu Côn Đảo":
        return "CON_DAO"
    if legacy == "HO_CHI_MINH":
        return "SG_CORE" if wtype == "PHUONG" else "SG_OUTER"
    return legacy


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", default=os.path.join(REPO_ROOT, ".cache", "hcm_wards"))
    args = parser.parse_args()

    # Index source features by normalized full name / Đánh chỉ mục nguồn theo tên đầy đủ đã chuẩn hoá
    source = {}
    for path in glob.glob(os.path.join(args.src_dir, "*.geojson")):
        feature = json.load(open(path, encoding="utf-8"))["features"][0]
        source[normalize_name(feature["properties"]["fullName"])] = feature

    wards = load_project_wards()
    missing = [w for w in wards if normalize_name(w) not in source]
    if len(wards) != 168 or missing:
        raise SystemExit(f"Ward mismatch: count={len(wards)}, missing={missing}")

    rows, features = [], []
    for idx, name in enumerate(wards, start=1):
        feature = source[normalize_name(name)]
        props = feature["properties"]
        geom = shape(feature["geometry"])

        # Centroid when it falls inside the ward, otherwise an interior point
        # Lấy trọng tâm nếu nằm trong phường, nếu không lấy điểm đại diện bên trong
        point = geom.centroid
        method = "CENTROID"
        if not geom.contains(point):
            point = geom.representative_point()
            method = "REPRESENTATIVE_POINT"

        code = int(props["code"])
        wtype = ward_type(name)
        legacy = legacy_province(code)
        ward_id = f"W{idx:03d}"
        minx, miny, maxx, maxy = geom.bounds
        rows.append({
            "Id": ward_id,
            "Name": name,
            "AdminCode": props["code"],
            "WardType": wtype,
            "ProvinceCode": "79",
            "LegacyProvince": legacy,
            "SubRegion": sub_region(name, wtype, legacy),
            "Latitude": f"{point.y:.6f}",
            "Longitude": f"{point.x:.6f}",
            "AreaKm2": props["areaKm2"],
            "IsRoadAccessible": str(name not in NOT_ROAD_ACCESSIBLE).lower(),
            "CoordMethod": method,
            "CoordSource": "thanglequoc/vietnamese-provinces-database@master (MIT)",
            "BboxMinLat": f"{miny:.6f}",
            "BboxMinLon": f"{minx:.6f}",
            "BboxMaxLat": f"{maxy:.6f}",
            "BboxMaxLon": f"{maxx:.6f}",
        })
        features.append({
            "type": "Feature",
            "id": ward_id,
            "properties": {"Id": ward_id, "Name": name, "AdminCode": props["code"]},
            "geometry": mapping(geom.simplify(SIMPLIFY_TOLERANCE_DEG, preserve_topology=True)),
        })

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    # Round coordinates to 6 decimals to keep the file small / Làm tròn 6 chữ số để file gọn
    def _round(obj):
        if isinstance(obj, float):
            return round(obj, 6)
        if isinstance(obj, (list, tuple)):
            return [_round(x) for x in obj]
        if isinstance(obj, dict):
            return {k: _round(v) for k, v in obj.items()}
        return obj

    with open(OUT_GEOJSON, "w", encoding="utf-8") as fh:
        json.dump(_round({"type": "FeatureCollection", "features": features}), fh, ensure_ascii=False, separators=(",", ":"))

    print(f"Wrote {len(rows)} rows -> {OUT_CSV}")
    print(f"Wrote {len(features)} features -> {OUT_GEOJSON}")


if __name__ == "__main__":
    main()
