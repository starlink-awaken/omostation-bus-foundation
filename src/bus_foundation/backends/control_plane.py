"""ControlPlaneBackend — Durable, reliable task execution with ACK and DLQ.

Uses SQLite WAL for strong consistency.
Designed for tasks, cron jobs, and webhooks that require exactly-once or at-least-once semantics.

R79 fixes (this rewrite):
- `subscribe()` now actually works: a daemon poller thread queries
  PENDING tasks and dispatches them to the subscriber callback. The
  previous implementation returned the literal string
  "control-sub-unsupported" and silently dropped all events.
- `ack()` updates updated_at (it did) and returns whether the row
  existed, so the caller can detect unknown task ids.
- `claim_pending()` is a new public API for callers that want to
  run their own daemon without using the in-process poller.
- `MAX_RETRIES` is configurable per-instance for callers that need
  different retry budgets.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path

from bus_foundation.backends.base import safe_call
from bus_foundation.backends.pattern_match import match_pattern
from bus_foundation.backends.sqlite_base import SqliteBase
from bus_foundation.envelope import OmniEnvelope

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path.home() / ".runtime" / "bus_control.db"
DEFAULT_MAX_RETRIES = 3
DEFAULT_POLL_INTERVAL = 0.5


class ControlPlaneBackend(SqliteBase):
    """SQLite-backed Control Plane with ACK, NACK, and DLQ support."""

    name = "control_plane"

    def __init__(
        self,
        db_path: Path | str | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
    ) -> None:
        db_path = Path(db_path or DEFAULT_DB_PATH)
        SqliteBase.__init__(self, db_path)
        self._max_retries = max_retries
        self._poll_interval = poll_interval
        self._init_schema()
        # R79: per-subscriber poller threads.
        self._subscribers: dict[str, tuple[str, Callable]] = {}
        self._poll_stops: dict[str, threading.Event] = {}
        self._poll_threads: dict[str, threading.Thread] = {}
        # Track task_ids we've already dispatched to a given subscriber
        # so the same task isn't dispatched twice if it stays PENDING
        # across polls.
        self._dispatched: dict[str, set[str]] = {}

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS control_tasks (
                    id TEXT PRIMARY KEY,
                    topic TEXT NOT NULL,
                    source TEXT,
                    status TEXT NOT NULL, -- PENDING, ACKED, DLQ
                    retry_count INTEGER DEFAULT 0,
                    envelope_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
            """)
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS control_tasks_status_updated ON control_tasks(status, updated_at)"
            )
            self._conn.commit()

    def is_available(self) -> bool:
        try:
            return self.db_path.parent.exists()
        except Exception:  # noqa: BLE001
            return False

    def publish(self, envelope: OmniEnvelope) -> str:
        """Submit a task to the control plane.

        Resets retry_count to 0 and sets plane='CONTROL' so the row is
        visible to the poller.
        """
        envelope.retry_count = 0
        envelope.plane = "CONTROL"
        now = time.time()
        with self._lock:
            self._conn.execute(
                "INSERT INTO control_tasks (id, topic, source, status, retry_count, envelope_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (envelope.id, envelope.topic, envelope.source_uri, "PENDING", 0, envelope.to_json(), now, now),
            )
            self._conn.commit()
        return envelope.id

    def subscribe(self, pattern: str, callback: Callable) -> str:
        """R79: subscribe = poll-PENDING + dispatch.

        Spawns a daemon thread that polls for PENDING tasks matching
        `pattern` and dispatches them. Use `ack()` / `nack()` to
        complete the task. The subscriber never sees a task twice
        (we track dispatched task_ids in-process; cross-process
        dedup is the caller's responsibility).
        """
        sub_id = f"control-sub-{uuid.uuid4().hex[:8]}"
        with self._lock:
            self._subscribers[sub_id] = (pattern, callback)
            self._dispatched[sub_id] = set()
        stop = threading.Event()
        with self._lock:
            self._poll_stops[sub_id] = stop
            self._poll_threads[sub_id] = threading.Thread(
                target=self._poll_loop,
                args=(sub_id, pattern, callback, stop),
                daemon=True,
                name=f"control-poll-{sub_id}",
            )
            self._poll_threads[sub_id].start()
        return sub_id

    def unsubscribe(self, sub_id: str) -> bool:
        with self._lock:
            present = sub_id in self._subscribers
            if present:
                del self._subscribers[sub_id]
                self._dispatched.pop(sub_id, None)
            stop = self._poll_stops.pop(sub_id, None)
            self._poll_threads.pop(sub_id, None)
        if stop is not None:
            stop.set()
        return present

    def _poll_loop(self, sub_id: str, pattern: str, callback: Callable, stop: threading.Event) -> None:
        """R79: poll PENDING tasks and dispatch to the callback.

        Each task is dispatched at most once per subscriber. To re-dispatch
        a NACK'd task (now back to PENDING), call `redeliver(task_id)`.
        """
        try:
            while not stop.is_set():
                try:
                    pending = self._fetch_pending(pattern)
                except sqlite3.OperationalError:
                    stop.wait(self._poll_interval)
                    continue
                with self._lock:
                    seen = self._dispatched.get(sub_id, set())
                for row in pending:
                    if row["id"] in seen:
                        continue
                    try:
                        env = OmniEnvelope.from_json(row["envelope_json"])
                    except Exception as e:  # noqa: BLE001
                        logger.error("control_decode_error task_id=%s err=%s", row["id"], e)
                        continue
                    safe_call(callback, env, ctx="control_plane", sub_id=sub_id)
                    with self._lock:
                        self._dispatched.setdefault(sub_id, set()).add(row["id"])
                stop.wait(self._poll_interval)
        except Exception as e:  # noqa: BLE001
            logger.error("control_poll_crashed sub_id=%s err=%s", sub_id, e)

    def _fetch_pending(self, pattern: str) -> list[sqlite3.Row]:
        """Return PENDING tasks whose topic matches `pattern`.

        Uses the pattern_match helper so callers can subscribe with
        `*` / `foo:*` / exact topics just like the other backends.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM control_tasks WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT 100"
            ).fetchall()
        return [r for r in rows if match_pattern(pattern, r["topic"])]

    def claim_pending(self, pattern: str = "*", limit: int = 100) -> list[dict]:
        """Public polling API for callers that want to run their own
        daemon loop (e.g. a long-lived worker process).

        Returns a list of dicts shaped like the SQLite row, with
        envelope_json included. The caller is responsible for calling
        ack() / nack() and tracking which task_ids have been processed.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM control_tasks WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows if match_pattern(pattern, r["topic"])]

    def ack(self, task_id: str) -> bool:
        """Acknowledge a task is successfully completed.

        Returns True if a row was updated, False if the task_id was
        unknown (R79: was previously None, callers couldn't tell).
        """
        with self._lock:
            cur = self._conn.execute(
                "UPDATE control_tasks SET status = 'ACKED', updated_at = ? WHERE id = ?",
                (time.time(), task_id),
            )
            self._conn.commit()
            return cur.rowcount > 0

    def nack(self, task_id: str, error: str) -> bool:
        """Negative acknowledge a task, incrementing retries or moving to DLQ.

        Returns True if a row was updated.
        """
        with self._lock:
            row = self._conn.execute("SELECT retry_count FROM control_tasks WHERE id = ?", (task_id,)).fetchone()
            if not row:
                return False
            retry_count = row["retry_count"] + 1
            status = "DLQ" if retry_count >= self._max_retries else "PENDING"
            self._conn.execute(
                "UPDATE control_tasks SET status = ?, retry_count = ?, updated_at = ? WHERE id = ?",
                (status, retry_count, time.time(), task_id),
            )
            self._conn.commit()
            moved = status == "DLQ"
        if moved:
            logger.error(
                "Task %s moved to DLQ after %d retries. Last error: %s",
                task_id,
                retry_count,
                error,
            )
            try:
                from bus_foundation.facade.event import publish as publish_event

                publish_event(
                    topic="system.fatal.dlq",
                    payload={
                        "task_id": task_id,
                        "error": error,
                        "retry_count": retry_count,
                        "backend": "control_plane",
                    },
                    source_uri="bos://capability/bus/control_plane",
                )
            except Exception as ex:  # noqa: BLE001
                logger.critical("Failed to emit system.fatal.dlq event: %s", ex)
        return True

    def redeliver(self, sub_id: str, task_id: str) -> bool:
        """Re-arm a task so a given subscriber will re-dispatch it.

        Useful after nack(): the task goes back to PENDING, but the
        in-process dispatched set keeps it from being re-sent unless
        you call this method.
        """
        with self._lock:
            if sub_id not in self._dispatched:
                return False
            self._dispatched[sub_id].discard(task_id)
            return True

    def close(self) -> None:
        # Stop all pollers.
        with self._lock:
            stops = list(self._poll_stops.values())
        for s in stops:
            s.set()
        with self._lock:
            self._conn.close()
