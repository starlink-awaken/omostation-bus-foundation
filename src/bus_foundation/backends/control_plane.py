"""ControlPlaneBackend — Durable, reliable task execution with ACK and DLQ.

Uses SQLite WAL for strong consistency.
Designed for tasks, cron jobs, and webhooks that require exactly-once or at-least-once semantics.
"""

import logging
import sqlite3
import threading
from collections.abc import Callable
from pathlib import Path

from bus_foundation.envelope import OmniEnvelope

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path.home() / ".runtime" / "bus_control.db"
MAX_RETRIES = 3

class ControlPlaneBackend:
    """SQLite-backed Control Plane with ACK, NACK, and DLQ support."""

    name = "control_plane"

    def __init__(self, db_path: Path | str | None = None) -> None:
        self.db_path = Path(db_path or DEFAULT_DB_PATH)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = self._open_conn()
        self._init_schema()

    def _open_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.row_factory = sqlite3.Row
        return conn

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
            self._conn.commit()

    def is_available(self) -> bool:
        try:
            return self.db_path.parent.exists()
        except Exception:  # noqa: BLE001  # defensive fallback
            return False

    def publish(self, envelope: OmniEnvelope) -> str:
        """Submit a task to the control plane."""
        import time
        now = time.time()

        # Ensure envelope reflects proper state
        envelope.retry_count = 0
        envelope.plane = "CONTROL"

        with self._lock:
            self._conn.execute(
                "INSERT INTO control_tasks (id, topic, source, status, retry_count, envelope_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (envelope.id, envelope.topic, envelope.source_uri, "PENDING", 0, envelope.to_json(), now, now),
            )
            self._conn.commit()

        return envelope.id

    def subscribe(self, pattern: str, callback: Callable) -> str:
        """Not traditionally used in control plane via subscribe decorators directly,
        but we support it by polling or manual trigger. A real Daemon would query PENDING tasks.
        """
        return "control-sub-unsupported"

    def unsubscribe(self, sub_id: str) -> bool:
        return True

    def ack(self, task_id: str) -> None:
        """Acknowledge a task is successfully completed."""
        import time
        with self._lock:
            self._conn.execute(
                "UPDATE control_tasks SET status = 'ACKED', updated_at = ? WHERE id = ?",
                (time.time(), task_id)
            )
            self._conn.commit()

    def nack(self, task_id: str, error: str) -> None:
        """Negative acknowledge a task, incrementing retries or moving to DLQ."""
        import time
        with self._lock:
            row = self._conn.execute("SELECT retry_count FROM control_tasks WHERE id = ?", (task_id,)).fetchone()
            if not row:
                return

            retry_count = row["retry_count"] + 1
            status = "DLQ" if retry_count >= MAX_RETRIES else "PENDING"

            self._conn.execute(
                "UPDATE control_tasks SET status = ?, retry_count = ?, updated_at = ? WHERE id = ?",
                (status, retry_count, time.time(), task_id)
            )
            self._conn.commit()

            if status == "DLQ":
                logger.error("Task %s moved to DLQ after %d retries. Last error: %s", task_id, retry_count, error)
                # Emit DLQ fatal event to event plane
                try:
                    from bus_foundation.facade.event import publish as publish_event
                    publish_event(
                        topic="system.fatal.dlq",
                        payload={
                            "task_id": task_id,
                            "error": error,
                            "retry_count": retry_count,
                            "backend": "control_plane"
                        },
                        source_uri="bos://capability/bus/control_plane"
                    )
                except Exception as ex:  # noqa: BLE001  # defensive fallback
                    logger.critical("Failed to emit system.fatal.dlq event: %s", ex)

    def close(self) -> None:
        with self._lock:
            self._conn.close()
