from datetime import date

from jobs.kaizen_sim.backfill import days_in_range, gap_days, hour_for_index, order_timestamp


def test_days_in_range_half_open():
    assert days_in_range(date(2026, 10, 1), date(2026, 10, 4)) == [
        date(2026, 10, 1),
        date(2026, 10, 2),
        date(2026, 10, 3),
    ]


def test_gap_days_only_shortfall():
    days = [date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 3)]
    existing = {date(2026, 10, 1): 10000, date(2026, 10, 2): 7000}
    assert gap_days(existing, days, 10000) == {
        date(2026, 10, 2): 3000,
        date(2026, 10, 3): 10000,
    }


def test_hour_curve_stays_in_open_hours():
    day = date(2026, 10, 14)
    hours = {hour_for_index(day, i, 100) for i in range(100)}
    assert hours <= set(range(7, 22))
    assert order_timestamp(day, 0, 10).hour >= 7
