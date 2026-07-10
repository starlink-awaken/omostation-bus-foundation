"""R96: tests for 5-segment cron + timezone support."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from bus_foundation.backends.croniter import (
    CroniterBackend,
    _CRON_5SEG_RE,
    _next_cron_run,
    _parse_cron_5seg,
    _parse_every,
    _parse_field,
    _parse_tz,
    parse_schedule,
)


class TestParseEvery:
    def test_every_minutes(self):
        assert _parse_every("every 5m") == 300
        assert _parse_every("every 30min") == 1800
        assert _parse_every("every 2 hours") == 7200

    def test_every_case_insensitive(self):
        assert _parse_every("EVERY 5M") == 300
        assert _parse_every("Every 10M") == 600

    def test_every_invalid_raises(self):
        with pytest.raises(ValueError):
            _parse_every("every 5s")  # seconds not allowed
        with pytest.raises(ValueError):
            _parse_every("every 1 day")
        with pytest.raises(ValueError):
            _parse_every("invalid")
        with pytest.raises(ValueError):
            _parse_every("every -5m")


class TestParseField:
    def test_wildcard(self):
        assert _parse_field("*", 0, 23) == set(range(24))

    def test_step(self):
        assert _parse_field("*/15", 0, 59) == {0, 15, 30, 45}

    def test_list(self):
        assert _parse_field("1,3,5", 1, 12) == {1, 3, 5}

    def test_range(self):
        assert _parse_field("9-17", 0, 23) == {9, 10, 11, 12, 13, 14, 15, 16, 17}

    def test_step_on_range(self):
        assert _parse_field("0-23/2", 0, 23) == {0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22}

    def test_out_of_range_raises(self):
        with pytest.raises(ValueError):
            _parse_field("25", 0, 23)


class TestParseCron5Seg:
    def test_wildcards(self):
        spec = _parse_cron_5seg("* * * * *")
        assert spec["minutes"] == set(range(60))
        assert spec["hours"] == set(range(24))

    def test_specific_minute(self):
        spec = _parse_cron_5seg("0 9 * * *")
        assert spec["minutes"] == {0}
        assert spec["hours"] == {9}
        assert 1 in spec["days"]  # wildcard for day-of-month
        assert 1 in spec["months"]  # wildcard for month

    def test_weekday_alias(self):
        spec = _parse_cron_5seg("0 9 * * 7")
        assert 0 in spec["dows"]  # 7 normalized to 0 (Sunday)
        assert 7 not in spec["dows"]

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            _parse_cron_5seg("not a cron")
        with pytest.raises(ValueError):
            _parse_cron_5seg("0 9 * *")  # 4 fields
        with pytest.raises(ValueError):
            _parse_cron_5seg("0 9 * * * * *")  # 6 fields


class TestParseSchedule:
    def test_every_returns_interval(self):
        kind, payload = parse_schedule("every 5m")
        assert kind == "interval"
        assert payload == 300

    def test_cron_returns_spec(self):
        kind, payload = parse_schedule("0 9 * * 1")
        assert kind == "cron"
        assert payload["minutes"] == {0}
        assert payload["hours"] == {9}
        assert payload["dows"] == {1}  # Monday

    def test_tz_parsing(self):
        kind, payload = parse_schedule("0 9 * * 1", tz="Asia/Shanghai")
        assert kind == "cron"


class TestTimezone:
    def test_valid_tz(self):
        tz = _parse_tz("Asia/Shanghai")
        assert isinstance(tz, ZoneInfo)

    def test_none_tz(self):
        assert _parse_tz(None) is None

    def test_invalid_tz_raises(self):
        with pytest.raises(ValueError):
            _parse_tz("Fake/Nowhere")


class TestNextCronRun:
    def test_every_minute_from_now(self):
        spec = _parse_cron_5seg("* * * * *")
        now = time.time()
        next_run = _next_cron_run(spec, now)
        # Should be at most 60 seconds in the future
        assert 0 < next_run - now <= 60

    def test_specific_time_in_future(self):
        # 9:00 every day. If now is 8:59, next should be in ~1 minute
        spec = _parse_cron_5seg("0 9 * * *")
        # Use a synthetic "now" at 8:59 UTC
        synthetic_now = datetime(2026, 6, 1, 8, 59, 0, tzinfo=timezone.utc).timestamp()
        next_run = _next_cron_run(spec, synthetic_now)
        # Should be 9:00:00 of same day
        next_dt = datetime.fromtimestamp(next_run, tz=timezone.utc)
        assert next_dt.hour == 9
        assert next_dt.minute == 0

    def test_monday_9am_in_future(self):
        # If now is Sunday 12pm, next Monday 9am is ~21 hours
        spec = _parse_cron_5seg("0 9 * * 1")  # Monday 9am
        # Sunday 2026-06-07 12:00:00 UTC
        synthetic_now = datetime(2026, 6, 7, 12, 0, 0, tzinfo=timezone.utc).timestamp()
        next_run = _next_cron_run(spec, synthetic_now)
        next_dt = datetime.fromtimestamp(next_run, tz=timezone.utc)
        assert next_dt.weekday() == 0  # Monday
        assert next_dt.hour == 9
        assert next_dt.minute == 0


class TestBackendIntegration:
    def test_add_cron_job_with_every_syntax(self):
        """R79 BC: 'every 5m' still works."""
        backend = CroniterBackend()
        try:
            fired = []
            backend.add_cron_job("test_job", "every 1m", lambda: fired.append(1))
            # Verify job is in _jobs
            assert "test_job" in backend._jobs
        finally:
            backend.stop()

    def test_add_cron_job_with_5seg_syntax(self):
        """R96: 5-segment cron syntax works."""
        backend = CroniterBackend()
        try:
            fired = []
            backend.add_cron_job("cron_job", "*/5 * * * *", lambda: fired.append(1))
            assert "cron_job" in backend._jobs
        finally:
            backend.stop()

    def test_subscribe_with_5seg_syntax(self):
        backend = CroniterBackend()
        try:
            sub_id = backend.subscribe("0 12 * * *", lambda: None)
            assert sub_id.startswith("cron-")
        finally:
            backend.stop()

    def test_invalid_cron_raises_at_register(self):
        backend = CroniterBackend()
        try:
            with pytest.raises(ValueError):
                backend.add_cron_job("bad", "garbage", lambda: None)
        finally:
            backend.stop()
