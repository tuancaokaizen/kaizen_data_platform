"""Voucher code top-up rules used by the daily close.
Quy tắc phát bù mã voucher dùng trong chốt sổ.
"""
from jobs.kaizen_sim.promotions import EXPIRY_DAYS, series_to_issue


def test_tops_up_to_buffer_and_continues_numbering():
    out = series_to_issue("VCH0001", 10_000, active=20, total=70, last_no=70, buffer=50)
    assert [s for s, _ in out] == [f"SR_VCH0001_{n:04d}" for n in range(71, 101)]
    assert all(e in EXPIRY_DAYS for _, e in out)


def test_never_exceeds_issued_quantity():
    assert len(series_to_issue("VCH0002", 60, active=0, total=55, last_no=55, buffer=50)) == 5
    assert series_to_issue("VCH0002", 60, active=10, total=60, last_no=60, buffer=50) == []


def test_full_buffer_issues_nothing():
    assert series_to_issue("VCH0003", 10_000, active=50, total=50, last_no=50, buffer=50) == []


def test_deterministic():
    a = series_to_issue("VCH0004", 10_000, active=0, total=0, last_no=0)
    b = series_to_issue("VCH0004", 10_000, active=0, total=0, last_no=0)
    assert a == b and len(a) == 50
    assert {e for _, e in a} == set(EXPIRY_DAYS)
