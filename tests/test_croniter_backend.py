"""Test CroniterBackend — R79 rewrite: strict parse + event-driven tick + publish as one-shot."""

from __future__ import annotations

import time

import pytest

from bus_foundation.backends.croniter import CroniterBackend, _parse_every
from bus_foundation.envelope import BusEnvelope

# ── _parse_every grammar ──────────────────────────────────────────────


class TestParseEvery:
    def test_minutes(self) -> None:
        assert _parse_every("every 5m") == 300
        assert _parse_every("every 30min") == 1800
        assert _parse_every("every 1 minute") == 60
        assert _parse_every("every 2 minutes") == 120

    def test_hours(self) -> None:
        assert _parse_every("every 1h") == 3600
        assert _parse_every("every 2 hours") == 7200
        assert _parse_every("every 6 hour") == 21600

    def test_case_insensitive(self) -> None:
        assert _parse_every("EVERY 5M") == 300
        assert _parse_every("Every 1H") == 3600

    def test_strips_whitespace(self) -> None:
        assert _parse_every("  every  5m  ") == 300

    @pytest.mark.parametrize(
        "bad",
        [
            "every 5s",      # seconds unsupported
            "every 1d",      # days unsupported
            "every 1w",      # weeks unsupported
            "every 5",       # missing unit
            "every 5 mins foo",  # garbage suffix
            "5m",            # missing "every"
            "every 0m",      # zero
            "every -3m",     # negative
            "every 1.5m",    # fractional
            "*/5 * * * *",   # cron syntax not supported
            "",
            "   ",
        ],
    )
    def test_rejects_unsupported(self, bad: str) -> None:
        with pytest.raises(ValueError):
            _parse_every(bad)

    def test_non_str_rejected(self) -> None:
        with pytest.raises(ValueError):
            _parse_every(123)  # type: ignore[arg-type]


# ── Backend registration & tick ──────────────────────────────────────


class TestCroniterBackend:
    def test_add_cron_job_registers(self) -> None:
        b = CroniterBackend()
        called: list[int] = []

        def cb() -> None:
            called.append(1)

        b.add_cron_job("job-1", "every 5m", cb)
        assert b.remove_cron_job("job-1") is True

    def test_add_cron_job_rejects_bad_expr(self) -> None:
        b = CroniterBackend()
        with pytest.raises(ValueError):
            b.add_cron_job("job-bad", "every 5s", lambda: None)

    def test_subscribe_rejects_bad_expr(self) -> None:
        """R79: callers should see ValueError at subscribe() time."""
        b = CroniterBackend()
        with pytest.raises(ValueError):
            b.subscribe("every 1d", lambda env: None)

    def test_publish_succeeds_and_returns_id(self) -> None:
        """R79: publish() is no longer the 'not implemented' sentinel —
        it enqueues a one-shot job so the BusBackend Protocol contract
        holds and Router won't mistake this backend for broken."""
        b = CroniterBackend()
        env = BusEnvelope(type="cron:test", source="x", payload={"k": 1})
        result = b.publish(env)
        assert isinstance(result, str) and result

    def test_start_stop(self) -> None:
        b = CroniterBackend()
        b.start()
        assert b._running is True
        b.stop()
        # Daemon thread will exit; we don't join (stop is best-effort).

    def test_short_interval_actually_fires(self) -> None:
        """R79: event-driven tick + strict parse means a 'every 1m' job
        doesn't silently never run. We can't wait 60s in a unit test,
        so we exercise the firing path by calling add_cron_job with
        a small interval and running the loop briefly."""
        b = CroniterBackend()
        calls: list[int] = []

        def cb() -> None:
            calls.append(1)

        b.add_cron_job("j", "every 1m", cb)
        # Force the job to be due "now" by mutating next_run to past.
        with b._lock:
            next_run, cb_obj, interval, _ = b._jobs["j"]
            b._jobs["j"] = (time.monotonic() - 1.0, cb_obj, interval, None)
        b.start()
        try:
            # Wait for the loop to fire once.
            for _ in range(50):  # 5 seconds
                if calls:
                    break
                time.sleep(0.1)
        finally:
            b.stop()
        assert calls, "expected the scheduled job to fire within 5s"
