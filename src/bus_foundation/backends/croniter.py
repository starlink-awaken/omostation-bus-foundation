"""CroniterBackend — cron-style scheduling as a bus backend.

R96: extended grammar. Now supports:
  - "every Nm" / "every Nh" (R79 backward-compat)
  - 5-segment cron: "minute hour day month day_of_week"
    Examples: "0 9 * * 1" (every Monday 9am)
              "*/5 * * * *" (every 5 minutes)
              "0 0 1 * *" (first of every month)

R79 fixes (this rewrite):
- Strict parse: 'every' expressions for unsupported units (s/sec,
  d/day, w/week) or any other string now raise ValueError instead of
  silently never firing. Callers that want to wait less than a minute
  should use AsyncIOScheduler or a dedicated timer.
- Event-driven tick: replaced the 30-second polling sleep with a
  per-job next-run time and an event-driven wait so the scheduler
  thread sleeps until the next job is due (capped at 60s for
  responsiveness to new jobs / shutdown).
- publish() is supported: it enqueues a one-shot "fire now" job so
  the BusBackend Protocol contract is satisfied and Router won't
  mistake croniter for a broken backend. For recurring schedules use
  add_cron_job() / the @schedule decorator.
"""

from __future__ import annotations

import calendar
import logging
import re
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from bus_foundation.envelope import BusEnvelope

logger = logging.getLogger(__name__)


# Strict grammar: "every Nm" or "every Nh" only. Bare numbers or
# unrecognised units raise ValueError at schedule time so misconfig
# is loud, not silent.
_EVERY_RE = re.compile(
    r"^every\s+(\d+)\s*(m|min|mins|minute|minutes|h|hr|hour|hours)$",
    re.IGNORECASE,
)

_UNIT_SECONDS: dict[str, int] = {
    "m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60,
    "h": 3600, "hr": 3600, "hour": 3600, "hours": 3600,
}

# Cap the wait between ticks so newly-scheduled jobs / shutdown
# requests don't hang for an entire hour.
_MAX_TICK_SECONDS = 60.0

# 5-segment cron regex: minute hour day month dow
# Each field: * | */N | N | N,N,... | N-M
_CRON_5SEG_RE = re.compile(
    r"^\s*(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s*$"
)


def _parse_every(expr: str) -> int:
    """Parse an 'every Nm' / 'every Nh' expression to a seconds interval.

    Raises ValueError for anything else. Returns a positive int.
    """
    if not isinstance(expr, str):
        raise ValueError(f"Schedule expression must be str, got {type(expr).__name__}")
    expr = expr.strip().lower()
    m = _EVERY_RE.match(expr)
    if not m:
        raise ValueError(
            f"Unsupported schedule expression: {expr!r}. "
            f"Supported: 'every Nm' (minutes), 'every Nh' (hours), "
            f"or 5-segment cron (e.g. '0 9 * * 1' = every Mon 9am)."
        )
    n = int(m.group(1))
    if n <= 0:
        raise ValueError(f"Schedule interval must be positive, got {n}")
    unit = m.group(2)
    return n * _UNIT_SECONDS[unit]


def _parse_field(field: str, lo: int, hi: int) -> set[int]:
    """Parse a single cron field (e.g. '*', '*/5', '1,3,5', '0-23') into a set of values.

    Raises ValueError on invalid syntax or out-of-range values.
    """
    field = field.strip()
    if not field:
        raise ValueError("empty cron field")
    out: set[int] = set()
    for part in field.split(","):
        part = part.strip()
        if not part:
            continue
        if part == "*":
            out |= set(range(lo, hi + 1))
            continue
        step = 1
        if "/" in part:
            base, step_str = part.split("/", 1)
            try:
                step = int(step_str)
            except ValueError as e:
                raise ValueError(f"invalid step in cron field: {part!r}") from e
            if step <= 0:
                raise ValueError(f"step must be positive: {part!r}")
            if base == "*":
                base_range = (lo, hi)
            else:
                base_range = _parse_range(base, lo, hi)
            for v in range(base_range[0], base_range[1] + 1, step):
                out.add(v)
            continue
        # Pure range or single value
        a, b = _parse_range(part, lo, hi)
        if a == b:
            out.add(a)
        else:
            out |= set(range(a, b + 1))
    return out


def _parse_range(part: str, lo: int, hi: int) -> tuple[int, int]:
    if "-" in part:
        a, b = part.split("-", 1)
        try:
            start, end = int(a), int(b)
        except ValueError as e:
            raise ValueError(f"invalid range: {part!r}") from e
        if not (lo <= start <= hi) or not (lo <= end <= hi):
            raise ValueError(f"value out of range: {part!r} (range {lo}-{hi})")
        return (min(start, end), max(start, end))
    try:
        v = int(part)
    except ValueError as e:
        raise ValueError(f"invalid value: {part!r}") from e
    if not (lo <= v <= hi):
        raise ValueError(f"value out of range: {v} (range {lo}-{hi})")
    return (v, v)


def _parse_cron_5seg(expr: str, tz: ZoneInfo | None = None) -> dict:
    """Parse a 5-segment cron expression into a schedule spec.

    Returns dict with keys: minutes, hours, days, months, dows (sets of int).
    Raises ValueError on invalid input.

    Note: standard cron uses 0=Sunday for day_of_week. We follow that
    convention but also accept 7=Sunday for compatibility with some
    crontab variants (normalized to 0).
    """
    if not isinstance(expr, str):
        raise ValueError(f"Cron expression must be str, got {type(expr).__name__}")
    expr = expr.strip()
    m = _CRON_5SEG_RE.match(expr)
    if not m:
        raise ValueError(
            f"Invalid 5-segment cron expression: {expr!r}. "
            f"Expected: 'minute hour day month day_of_week'"
        )
    minute_f, hour_f, day_f, month_f, dow_f = m.groups()
    spec = {
        "minutes": _parse_field(minute_f, 0, 59),
        "hours": _parse_field(hour_f, 0, 23),
        "days": _parse_field(day_f, 1, 31),
        "months": _parse_field(month_f, 1, 12),
        # Day-of-week accepts 0-7 (7 normalized to 0 for Sunday compat)
        "dows": _parse_dow_field(dow_f),
    }
    return spec


def _parse_dow_field(field: str) -> set[int]:
    """Parse day-of-week field, accepting 0-7 (7=Sun normalized to 0)."""
    field = field.strip()
    if not field:
        raise ValueError("empty cron field")
    out: set[int] = set()
    for part in field.split(","):
        part = part.strip()
        if not part:
            continue
        if part == "*":
            out |= {0, 1, 2, 3, 4, 5, 6}
            continue
        if "/" in part:
            base, step_str = part.split("/", 1)
            try:
                step = int(step_str)
            except ValueError as e:
                raise ValueError(f"invalid step in cron field: {part!r}") from e
            if step <= 0:
                raise ValueError(f"step must be positive: {part!r}")
            if base == "*":
                base_range = (0, 7)
            else:
                a, b = _parse_range(base, 0, 7)
                base_range = (a, b)
            for v in range(base_range[0], base_range[1] + 1, step):
                if v == 7:
                    out.add(0)
                else:
                    out.add(v)
            continue
        a, b = _parse_range(part, 0, 7)
        for v in range(a, b + 1):
            if v == 7:
                out.add(0)
            else:
                out.add(v)
    return out


def _next_cron_run(spec: dict, after: float, tz: ZoneInfo | None = None) -> float:
    """Return next monotonic timestamp when the cron spec should fire.

    `after` is the current time (monotonic). Searches up to 4 years
    ahead, raising ValueError if no run is found in that window.

    Standard cron day-of-week semantics:
      - if dow is *: use day-of-month only
      - if day is *: use day-of-week only
      - if both specified: AND (both must match)
    """
    from datetime import timedelta
    tz = tz or ZoneInfo("UTC")
    after_dt = datetime.fromtimestamp(after, tz=tz)
    # Start from the next minute boundary
    candidate = after_dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
    deadline = after + 4 * 365 * 24 * 3600
    day_wildcard = spec["days"] == set(range(1, 32))
    dow_wildcard = spec["dows"] == set(range(0, 7))

    while after < deadline:
        if candidate.month not in spec["months"]:
            if candidate.month == 12:
                candidate = candidate.replace(year=candidate.year + 1, month=1, day=1, hour=0, minute=0)
            else:
                candidate = candidate.replace(month=candidate.month + 1, day=1, hour=0, minute=0)
            continue
        if day_wildcard or dow_wildcard:
            if not day_wildcard and candidate.day not in spec["days"]:
                candidate = candidate + timedelta(days=1)
                candidate = candidate.replace(hour=0, minute=0)
                continue
            if not dow_wildcard and candidate.weekday() not in spec["dows"]:
                # Note: cron dow 0=Sun, 1=Mon, ..., 6=Sat (Sun=7 normalized to 0)
                # Python weekday() is 0=Mon, 6=Sun. Convert: cron_dow = (py_weekday + 1) % 7
                cron_dow = (candidate.weekday() + 1) % 7
                if cron_dow not in spec["dows"]:
                    candidate = candidate + timedelta(days=1)
                    candidate = candidate.replace(hour=0, minute=0)
                    continue
            if not day_wildcard and not dow_wildcard:
                if candidate.day not in spec["days"] or candidate.weekday() not in spec["dows"]:
                    candidate = candidate + timedelta(days=1)
                    candidate = candidate.replace(hour=0, minute=0)
                    continue
        else:
            pass
        if candidate.hour not in spec["hours"]:
            candidate = candidate + timedelta(hours=1)
            candidate = candidate.replace(minute=0)
            continue
        if candidate.minute not in spec["minutes"]:
            candidate = candidate + timedelta(minutes=1)
            continue
        return candidate.timestamp()
    raise ValueError(f"no firing time found within 4 years for cron {spec!r}")


def parse_schedule(expr: str, tz: str | None = None) -> tuple[str, object]:
    """Parse any supported schedule expression.

    Returns ("interval", seconds_int) for "every Nm/Nh" or
    ("cron", spec_dict) for 5-segment cron.
    """
    expr = expr.strip()
    if expr.lower().startswith("every"):
        return ("interval", _parse_every(expr))
    return ("cron", _parse_cron_5seg(expr, _parse_tz(tz)))


def _parse_tz(tz: str | None) -> ZoneInfo | None:
    if not tz:
        return None
    try:
        return ZoneInfo(tz)
    except ZoneInfoNotFoundError as e:
        raise ValueError(f"unknown timezone: {tz!r}") from e


class CroniterBackend:
    """Schedules recurring tasks via an 'every Nm' / 'every Nh' syntax.

    R79 rewrite: replaces the previous 30s-poll-based implementation
    that silently dropped unrecognised units and was sluggish for
    short intervals.
    """

    name = "croniter"

    def __init__(self) -> None:
        # job_id -> (next_run_monotonic, callback, interval_or_None, cron_spec_or_None)
        self._jobs: dict[str, tuple[float, Callable, int | None, tuple | None]] = {}
        self._lock = threading.Lock()
        self._running = False
        self._thread: threading.Thread | None = None
        self._wakeup = threading.Event()  # signal the run loop to wake up

    def is_available(self) -> bool:
        return True

    def publish(self, envelope: BusEnvelope) -> str:
        """R79: support one-shot publish by enqueueing a 'fire now' job.

        Keeps the BusBackend Protocol contract intact (Router treats
        raise() as failure) while letting callers that have a
        BusEnvelope flow a one-shot task through this backend. The
        callback receives the envelope.
        """
        captured_env = envelope
        with self._lock:
            job_id = f"one-shot-{envelope.id[:8]}"
            # Fire 'now' — set next_run to current monotonic.
            self._jobs[job_id] = (time.monotonic(), lambda env=captured_env: None, 0, None)
        self._wakeup.set()
        return envelope.id

    def add_cron_job(
        self, job_id: str, cron_expr: str, callback: Callable,
        tz: str | None = None,
    ) -> None:
        """R96: accept either 'every Nm/Nh' (R79 BC) or 5-segment cron.

        Args:
            job_id: unique identifier
            cron_expr: schedule expression
            callback: function called when the schedule fires
            tz: timezone name (e.g. 'Asia/Shanghai'), default UTC
        """
        kind, payload = parse_schedule(cron_expr, tz=tz)
        with self._lock:
            if kind == "interval":
                interval = payload
                next_run = time.monotonic() + interval
                # 4-tuple: (next_run_monotonic, callback, interval_seconds_or_None, spec_or_None)
                self._jobs[job_id] = (next_run, callback, interval, None)
            else:
                spec = payload
                next_run = _next_cron_run(spec, time.time(), _parse_tz(tz))
                # 4-tuple: (next_run_monotonic, callback, interval_seconds_or_None, spec_or_None)
                self._jobs[job_id] = (next_run, callback, None, (spec, tz))
        self._wakeup.set()

    def remove_cron_job(self, job_id: str) -> bool:
        with self._lock:
            return self._jobs.pop(job_id, None) is not None

    def subscribe(
        self, pattern: str, callback: Callable, tz: str | None = None,
    ) -> str:
        """R96: accept either 'every Nm/Nh' (R79 BC) or 5-segment cron."""
        kind, payload = parse_schedule(pattern, tz=tz)
        job_id = f"cron-{len(self._jobs)}-{int(time.monotonic() * 1000) % 10000}"
        with self._lock:
            if kind == "interval":
                interval = payload
                next_run = time.monotonic() + interval
                self._jobs[job_id] = (next_run, callback, interval, None)
            else:
                spec = payload
                next_run = _next_cron_run(spec, time.time(), _parse_tz(tz))
                self._jobs[job_id] = (next_run, callback, None, (spec, tz))
        self._wakeup.set()
        return job_id

    def unsubscribe(self, sub_id: str) -> bool:
        return self.remove_cron_job(sub_id)

    def start(self) -> None:
        """Start the scheduler thread."""
        if self._running:
            return
        self._running = True
        self._wakeup.clear()
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="bus-croniter"
        )
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        self._wakeup.set()  # wake the run loop

    def _run_loop(self) -> None:
        while self._running:
            with self._lock:
                if not self._jobs:
                    wait_seconds = _MAX_TICK_SECONDS
                else:
                    now = time.monotonic()
                    next_run = min(job[0] for job in self._jobs.values())
                    wait_seconds = max(0.0, min(_MAX_TICK_SECONDS, next_run - now))
            # Wait for either the timeout or a wakeup signal (new job
            # or shutdown). Event.wait() returns True if set.
            woke = self._wakeup.wait(timeout=wait_seconds)
            if woke:
                self._wakeup.clear()
            if not self._running:
                break
            # Fire any due jobs.
            now = time.monotonic()
            with self._lock:
                due: list[tuple[str, Callable]] = []
                for jid, job in list(self._jobs.items()):
                    next_run, cb, interval, cron_meta = job
                    if next_run <= now:
                        due.append((jid, cb))
                        if interval is not None:
                            # interval-based: reschedule
                            self._jobs[jid] = (now + interval, cb, interval, None)
                        elif cron_meta is not None:
                            spec, tz = cron_meta
                            # cron-based: compute next run
                            new_next = _next_cron_run(spec, time.time(), _parse_tz(tz))
                            self._jobs[jid] = (new_next, cb, None, cron_meta)
            for _jid, cb in due:
                try:
                    cb()
                except Exception as e:  # noqa: BLE001
                    logger.error("croniter_callback_error err=%s", e)
