"""R96 extended: edge cases and error paths in croniter parser/scheduler."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from bus_foundation.backends.croniter import (
    CroniterBackend,
    _next_cron_run,
    _parse_cron_5seg,
    _parse_field,
)

# ── _parse_field error paths ──────────────────────────────────────


def test_parse_field_empty_raises():
    with pytest.raises(ValueError, match="empty"):
        _parse_field("", 0, 59)


def test_parse_field_invalid_step():
    with pytest.raises(ValueError, match="step"):
        _parse_field("*/abc", 0, 59)


def test_parse_field_zero_step():
    with pytest.raises(ValueError, match="step"):
        _parse_field("*/0", 0, 59)


def test_parse_field_out_of_range():
    with pytest.raises(ValueError, match="out of range"):
        _parse_field("100", 0, 59)


def test_parse_field_invalid_value():
    with pytest.raises(ValueError, match="invalid value"):
        _parse_field("abc", 0, 59)


def test_parse_field_invalid_range():
    with pytest.raises(ValueError, match="invalid range"):
        _parse_field("a-b", 0, 59)


# ── _parse_cron_5seg edge cases ────────────────────────────────────


def test_parse_cron_every_minute():
    spec = _parse_cron_5seg("* * * * *")
    assert spec["minutes"] == set(range(60))
    assert spec["hours"] == set(range(24))


def test_parse_cron_specific_dow():
    """0 and 7 in day-of-week both mean Sunday."""
    spec = _parse_cron_5seg("0 0 * * 0,7")
    assert 0 in spec["dows"]


def test_parse_cron_specific_day_and_dow():
    """Specific day and day-of-week both set."""
    spec = _parse_cron_5seg("0 0 15 * 5")
    assert 15 in spec["days"]
    assert 5 in spec["dows"]


def test_parse_cron_step_in_dow():
    """Step in day-of-week field."""
    spec = _parse_cron_5seg("0 0 * * */2")
    assert 0 in spec["dows"]
    assert 2 in spec["dows"]
    assert 4 in spec["dows"]
    assert 6 in spec["dows"]


# ── _next_cron_run edge cases ──────────────────────────────────────


def test_next_run_advances_minute():
    """Basic minute advance."""
    now = datetime(2026, 7, 12, 10, 0, 0, tzinfo=UTC).timestamp()
    spec = _parse_cron_5seg("30 * * * *")  # :30 every hour
    nxt = _next_cron_run(spec, now)
    dt = datetime.fromtimestamp(nxt, tz=UTC)
    assert dt.minute == 30
    assert dt.hour == 10


def test_next_run_advances_hour():
    """If current minute > target, advance hour."""
    now = datetime(2026, 7, 12, 10, 45, 0, tzinfo=UTC).timestamp()
    spec = _parse_cron_5seg("30 * * * *")  # :30 every hour
    nxt = _next_cron_run(spec, now)
    dt = datetime.fromtimestamp(nxt, tz=UTC)
    assert dt.minute == 30
    assert dt.hour == 11


def test_next_run_advances_day():
    """If current hour > target, advance day."""
    now = datetime(2026, 7, 12, 15, 0, 0, tzinfo=UTC).timestamp()
    spec = _parse_cron_5seg("0 6 * * *")  # daily 06:00
    nxt = _next_cron_run(spec, now)
    dt = datetime.fromtimestamp(nxt, tz=UTC)
    assert dt.hour == 6
    assert dt.day == 13


def test_next_run_advances_month():
    """If current day > target, advance month."""
    now = datetime(2026, 7, 15, 0, 0, 0, tzinfo=UTC).timestamp()
    spec = _parse_cron_5seg("0 0 1 * *")  # 1st of month
    nxt = _next_cron_run(spec, now)
    dt = datetime.fromtimestamp(nxt, tz=UTC)
    assert dt.month == 8
    assert dt.day == 1


def test_next_run_advances_year():
    """If current month > target, advance year."""
    now = datetime(2026, 12, 31, 0, 0, 0, tzinfo=UTC).timestamp()
    spec = _parse_cron_5seg("0 0 1 1 *")  # Jan 1
    nxt = _next_cron_run(spec, now)
    dt = datetime.fromtimestamp(nxt, tz=UTC)
    assert dt.year == 2027
    assert dt.month == 1
    assert dt.day == 1


# ── CroniterBackend scheduler edge cases ──────────────────────────


def test_scheduler_accepts_5seg_cron():
    cb = CroniterBackend()
    cb.start()
    cb.add_cron_job("cron-test", "*/5 * * * *", lambda: None)
    assert cb.remove_cron_job("cron-test") is True
    cb.stop()


def test_scheduler_accepts_every_expr():
    cb = CroniterBackend()
    cb.start()
    cb.add_cron_job("every-test", "every 10m", lambda: None)
    assert cb.remove_cron_job("every-test") is True
    cb.stop()


def test_scheduler_remove_unknown():
    cb = CroniterBackend()
    assert cb.remove_cron_job("nonexistent") is False
