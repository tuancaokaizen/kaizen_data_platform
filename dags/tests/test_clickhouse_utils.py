"""Locating the live Iceberg folder behind a ClickHouse cold table.
Tìm thư mục Iceberg đang dùng cho bảng cold trên ClickHouse.
"""
from datetime import datetime, timezone

from jobs.clickhouse_utils import folder_from_metadata_location, newest_folder, table_folder_pattern

UUID_A = "cfc2c734-fc09-4cf2-9498-2a51bf861f70"
UUID_B = "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d"


def test_folder_from_nessie_metadata_location():
    location = f"s3a://iceberg-data/serving/financial_report_{UUID_A}/metadata/00003-x.metadata.json"
    assert folder_from_metadata_location(location) == f"serving/financial_report_{UUID_A}"


def test_metadata_outside_the_bucket_or_without_metadata_dir_is_rejected():
    assert folder_from_metadata_location(f"s3a://other/serving/t_{UUID_A}/metadata/1.json") is None
    assert folder_from_metadata_location(f"s3a://iceberg-data/serving/t_{UUID_A}/data/1.parquet") is None
    assert folder_from_metadata_location(None) is None


def test_pattern_does_not_match_tables_sharing_the_prefix():
    pattern = table_folder_pattern("serving", "financial_report")
    assert pattern.match(f"serving/financial_report_{UUID_A}")
    assert pattern.match(f"serving/financial_report_{UUID_A}/")
    assert not pattern.match(f"serving/financial_report_detail_{UUID_A}")
    assert not pattern.match(f"serving/financial_report_order_{UUID_B}")
    assert not pattern.match(f"fnd/financial_report_{UUID_A}")


def test_newest_folder_wins_and_folders_without_metadata_are_ignored():
    old = datetime(2026, 10, 1, tzinfo=timezone.utc)
    new = datetime(2026, 10, 14, tzinfo=timezone.utc)
    candidates = [("serving/t_a", new), ("serving/t_b", old), ("serving/t_c", None)]
    assert newest_folder(candidates) == "serving/t_a"
    assert newest_folder([("serving/t_c", None)]) is None
    assert newest_folder([]) is None
