"""PersistentBusBackend — durable in-process pub/sub via SQLite.

Phase B R73: NEW backend. Cross-process pub/sub for low-throughput,
durable-delivery scenarios (audit logs, task notifications, workflows
that need resume-on-restart).

Use case: when you want the fire-and-forget semantics of EventBus but
need the events to survive a process restart. NOT for high-throughput
production pub/sub (use Kafka/NATS for that).

Design (intentionally simple):
- SQLite table with WAL mode for concurrent reads + 1 writer
- Insert on publish, fanout via in-process subscriber list
- Optional retention: keep last N events (default: 10000)
- Subscriber register_hook for in-process delivery (reuses BusBackend
  protocol contract)
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from bus_foundation.backends.base import safe_call
from bus_foundation.backends.pattern_match import match_pattern
from bus_foundation.backends.sqlite_base import SqliteBase
from bus_foundation.envelope import BusEnvelope

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path.home() / ".runtime" / "bus_persistent.db"
MAX_EVENTS = 10_000
SUBSCRIBER_TTL_SECONDS = 24 * 3600
# R75-LOW-1: rate-limit subscriber cleanup (was O(N) per publish)
CLEANUP_EVERY_N_PUBLISHES = 100


class PersistentBusBackend(SqliteBase):
    """SQLite-backed durable pub/sub.

    One SQLite DB, WAL mode. Subscribers are in-process callbacks
    (cross-process delivery is via the SQLite file itself, polled by
    other processes if needed).
    """

    name = "persistent"

    def __init__(self, db_path: Path | str | None = None, poll_interval: float = 0.5) -> None:
        db_path = Path(db_path or DEFAULT_DB_PATH)
        SqliteBase.__init__(self, db_path)
        self._init_schema()
        self._subscribers: dict[str, tuple[str, float, Callable]] = {}
        # R79: per-subscriber tail threads for cross-process delivery.
        self._tail_stops: dict[str, threading.Event] = {}
        self._tail_threads: dict[str, threading.Thread] = {}
        # R79: per-subscriber watermark = the highest time value seen,
        # so a polling subscriber only sees new events.
        self._watermarks: dict[str, str] = {}
        # R79: per-subscriber dedup of delivered envelope ids. Used to
        # ensure in-process publish() and the tail thread don't both
        # deliver the same envelope.
        self._delivered_ids: dict[str, set[str]] = {}
        self._poll_interval = poll_interval
        # R75-LOW-1: rate-limit counter (cleanup runs every Nth publish,
        # not every publish). Avoids O(N) per publish for high-subscriber
        # counts.
        self._publish_count = 0
        self._gc()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS persistent_events (
                    id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    source TEXT,
                    time TEXT NOT NULL,
                    envelope_json TEXT NOT NULL
                )
            """)
            self._conn.execute("""
                CREATE INDEX IF NOT EXISTS persistent_events_type_time
                ON persistent_events(event_type, time)
            """)
            self._conn.commit()

    def _gc(self) -> None:
        """Trim to MAX_EVENTS (FIFO)."""
        with self._lock:
            count_row = self._conn.execute("SELECT COUNT(*) AS n FROM persistent_events").fetchone()
            n = count_row["n"] if count_row else 0
            if n > MAX_EVENTS:
                excess = n - MAX_EVENTS
                self._conn.execute(
                    """
                    DELETE FROM persistent_events
                    WHERE id IN (
                        SELECT id FROM persistent_events ORDER BY time ASC LIMIT ?
                    )
                    """,
                    (excess,),
                )
                self._conn.commit()
                logger.info("persistent_gc trimmed=%d", excess)

    def is_available(self) -> bool:
        try:
            return self.db_path.parent.exists()
        except Exception:  # noqa: BLE001  # defensive fallback
            return False

    def publish(self, envelope: BusEnvelope) -> str:
        now = self._now()
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO persistent_events (id, event_type, source, time, envelope_json) VALUES (?, ?, ?, ?, ?)",
                (envelope.id, envelope.type, envelope.source, now, envelope.to_json()),
            )
            self._conn.commit()
            # R75-LOW-1: rate-limit cleanup (was every publish, now every Nth)
            self._publish_count += 1
            if self._publish_count % CLEANUP_EVERY_N_PUBLISHES == 0:
                self._cleanup_subs_locked()
        # R79: the in-process tail thread will also pick this up via
        # the SQLite poll (this is what makes cross-process delivery
        # work). We dedup by envelope id so each callback fires at
        # most once per subscriber across both paths.
        for sub_id, (pattern, _last_seen, callback) in self._subscribers.items():
            if not self._match(pattern, envelope.type):
                continue
            with self._lock:
                if envelope.id in self._delivered_ids.get(sub_id, set()):
                    continue
            safe_call(callback, envelope, ctx="persistent", sub_id=sub_id)
            with self._lock:
                self._delivered_ids.setdefault(sub_id, set()).add(envelope.id)
        return envelope.id

    def subscribe(self, pattern: str, callback: Callable) -> str:
        """Subscribe + start a per-subscriber tail thread.

        R79: the previous implementation only fan-outed to in-process
        subscribers, contradicting the docstring claim of "cross-process
        delivery via the SQLite file itself, polled by other processes".
        We now spawn a daemon tail thread that polls the SQLite log for
        new events matching the pattern and dispatches them. This
        delivers events across processes that share the same db_path.
        """
        sub_id = f"persistent-{uuid.uuid4().hex[:8]}"
        with self._lock:
            self._subscribers[sub_id] = (pattern, time.monotonic(), callback)
        # Start the per-subscriber tail thread that polls for new events
        # from other processes. Each subscriber has its own thread so
        # unsubscribe cleanly stops the loop.
        stop_event = threading.Event()
        # R79: catch-up window — when a subscriber joins, replay events
        # written in the last 5 seconds so cross-process subscribers
        # don't miss events written just before they subscribed.
        catchup_window = 5.0
        with self._lock:
            self._tail_stops[sub_id] = stop_event
            self._tail_threads[sub_id] = threading.Thread(
                target=self._tail_loop,
                args=(sub_id, pattern, callback, stop_event, catchup_window),
                daemon=True,
                name=f"bus-tail-{sub_id}",
            )
            self._tail_threads[sub_id].start()
        return sub_id

    def unsubscribe(self, sub_id: str) -> bool:
        """Stop the tail thread (if any) and remove the subscriber."""
        with self._lock:
            present = sub_id in self._subscribers
            if present:
                del self._subscribers[sub_id]
            stop = self._tail_stops.pop(sub_id, None)
            self._tail_threads.pop(sub_id, None)
            self._watermarks.pop(sub_id, None)
        if stop is not None:
            stop.set()
        return present

    def _tail_loop(
        self,
        sub_id: str,
        pattern: str,
        callback: Callable,
        stop_event: threading.Event,
        catchup_window: float,
    ) -> None:
        """R79: per-subscriber tail thread.

        Polls the persistent_events table for new rows (time >
        watermark) matching the pattern, dispatches them, and advances
        the watermark. The watermark is initialized to
        (now - catchup_window) so events written in the catch-up window
        BEFORE this subscriber joined are also delivered. Exits when
        stop_event is set.
        """
        # R79: initialize watermark to now - catchup_window. This relies
        # on a time-orderable string format (ISO 8601 UTC with Z
        # suffix); we slice the first 19 chars (YYYY-MM-DDTHH:MM:SS) and
        # subtract seconds.
        from datetime import UTC, datetime, timedelta

        init_watermark = (datetime.now(UTC) - timedelta(seconds=catchup_window)).strftime("%Y-%m-%dT%H:%M:%SZ")
        with self._lock:
            self._watermarks[sub_id] = init_watermark
        try:
            while not stop_event.is_set():
                with self._lock:
                    watermark = self._watermarks.get(sub_id, "")
                # Use a separate short-lived connection for the poll so
                # we don't fight the main writer for the lock. SQLite WAL
                # allows concurrent readers.
                try:
                    poll_conn = sqlite3.connect(str(self.db_path), timeout=1.0, check_same_thread=False)
                    poll_conn.execute("PRAGMA journal_mode=WAL")
                    poll_conn.row_factory = sqlite3.Row
                    try:
                        rows = poll_conn.execute(
                            """
                            SELECT id, event_type, source, time, envelope_json
                            FROM persistent_events
                            WHERE time > ?
                            ORDER BY time ASC
                            LIMIT 100
                            """,
                            (watermark,),
                        ).fetchall()
                    finally:
                        poll_conn.close()
                except sqlite3.OperationalError:
                    # DB locked by writer; back off and retry.
                    stop_event.wait(self._poll_interval)
                    continue
                if rows:
                    new_watermark = watermark
                    for row in rows:
                        if not self._match(pattern, row["event_type"]):
                            continue
                        with self._lock:
                            if row["id"] in self._delivered_ids.get(sub_id, set()):
                                # Already delivered in-process; skip.
                                if row["time"] > new_watermark:
                                    new_watermark = row["time"]
                                continue
                        try:
                            env = BusEnvelope.from_json(row["envelope_json"])
                        except Exception as e:  # noqa: BLE001
                            logger.error(
                                "persistent_tail_decode_error sub_id=%s err=%s",
                                sub_id,
                                e,
                            )
                            continue
                        safe_call(callback, env, ctx="persistent_tail", sub_id=sub_id)
                        with self._lock:
                            self._delivered_ids.setdefault(sub_id, set()).add(row["id"])
                        if row["time"] > new_watermark:
                            new_watermark = row["time"]
                    if new_watermark != watermark:
                        with self._lock:
                            self._watermarks[sub_id] = new_watermark
                stop_event.wait(self._poll_interval)
        except Exception as e:  # noqa: BLE001
            logger.error("persistent_tail_crashed sub_id=%s err=%s", sub_id, e)

    def _cleanup_subs(self) -> None:
        """Public wrapper: acquires the lock."""
        with self._lock:
            self._cleanup_subs_locked()

    def _cleanup_subs_locked(self) -> None:
        """Drop subscribers that haven't been seen in SUBSCRIBER_TTL_SECONDS.

        R74 fix: use time.monotonic() for TTL delta.
        R75-LOW-1: caller is rate-limited (every Nth publish); this
        function assumes the lock is already held.
        """
        now = time.monotonic()
        expired = [sid for sid, (_, _last, _) in self._subscribers.items() if now - _last > SUBSCRIBER_TTL_SECONDS]
        for sid in expired:
            del self._subscribers[sid]

    @staticmethod
    def _match(pattern: str, event_type: str) -> bool:
        # R74 LOW fix: delegate to shared helper (deduplicates 6 copies)
        return match_pattern(pattern, event_type)

    def get_recent(self, event_type: str = "", limit: int = 100) -> list[dict[str, Any]]:
        """Query historical events (for catch-up on restart)."""
        with self._lock:
            if event_type:
                rows = self._conn.execute(
                    "SELECT * FROM persistent_events WHERE event_type = ? ORDER BY time DESC LIMIT ?",
                    (event_type, limit),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM persistent_events ORDER BY time DESC LIMIT ?",
                    (limit,),
                ).fetchall()
        return [dict(r) for r in rows]
