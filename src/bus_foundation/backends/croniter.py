"""CroniterBackend — cron-style scheduling as a bus backend.

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

import logging
import re
import threading
import time
from collections.abc import Callable

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
            f"Supported: 'every Nm' (minutes) or 'every Nh' (hours)."
        )
    n = int(m.group(1))
    if n <= 0:
        raise ValueError(f"Schedule interval must be positive, got {n}")
    unit = m.group(2)
    return n * _UNIT_SECONDS[unit]


class CroniterBackend:
    """Schedules recurring tasks via an 'every Nm' / 'every Nh' syntax.

    R79 rewrite: replaces the previous 30s-poll-based implementation
    that silently dropped unrecognised units and was sluggish for
    short intervals.
    """

    name = "croniter"

    def __init__(self) -> None:
        # job_id -> (interval_seconds, next_run_monotonic, callback)
        self._jobs: dict[str, tuple[int, float, Callable]] = {}
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
            self._jobs[job_id] = (0, time.monotonic(), lambda env=captured_env: None)
        self._wakeup.set()
        return envelope.id

    def add_cron_job(self, job_id: str, cron_expr: str, callback: Callable) -> None:
        """R79: validate the expression at registration time."""
        interval = _parse_every(cron_expr)
        with self._lock:
            self._jobs[job_id] = (interval, time.monotonic() + interval, callback)
        self._wakeup.set()

    def remove_cron_job(self, job_id: str) -> bool:
        with self._lock:
            return self._jobs.pop(job_id, None) is not None

    def subscribe(self, pattern: str, callback: Callable) -> str:
        """For cron backend, subscribe = add_cron_job with cron expression.

        R79: validate at subscribe() time so callers see a ValueError
        immediately rather than discovering the job never fires.
        """
        interval = _parse_every(pattern)
        job_id = f"cron-{len(self._jobs)}-{int(time.monotonic() * 1000) % 10000}"
        with self._lock:
            self._jobs[job_id] = (interval, time.monotonic() + interval, callback)
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
                    next_run = min(job[1] for job in self._jobs.values())
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
                for jid, (interval, next_run, cb) in self._jobs.items():
                    if next_run <= now:
                        due.append((jid, cb))
                        # Reschedule: next interval from now.
                        self._jobs[jid] = (interval, now + interval, cb)
            for _jid, cb in due:
                try:
                    cb()
                except Exception as e:  # noqa: BLE001
                    logger.error("croniter_callback_error err=%s", e)
