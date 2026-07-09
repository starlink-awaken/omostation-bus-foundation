"""SqliteBase — shared SQLite infrastructure for bus-foundation.

R86: extracts the common SQLite WAL connection setup, schema init,
threading lock pattern, and timestamp helpers that were duplicated
between PersistentBusBackend and ControlPlaneBackend. Both backends
now inherit from this base class, reducing duplication by ~80 lines.

Each subclass owns its own table schema and methods; only the
infrastructure is shared. The subclasses remain independent so a
caller that wants only control-plane semantics (tasks + ACK) does
not inherit event-bus SQLite schema or tail threads.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class SqliteBase:
    """Shared SQLite infra: connection, WAL, lock, GC, timestamps.

    Usage:
        class MyBackend(SqliteBase):
            def _init_schema(self):
                self.execute("CREATE TABLE ...")
    """

    def __init__(self, db_path: Path, busy_timeout: int = 5000):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = self._open_conn(busy_timeout)
        self._execute("PRAGMA journal_mode=WAL")
        self._execute(f"PRAGMA busy_timeout={busy_timeout}")

    def _open_conn(self, busy_timeout: int = 5000) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False, timeout=busy_timeout / 1000)
        conn.row_factory = sqlite3.Row
        return conn

    # ── Thread-safe helpers ────────────────────────────────────

    def _execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        with self._lock:
            return self._conn.execute(sql, params)

    def _executemany(self, sql: str, seq: list[tuple]) -> None:
        with self._lock:
            self._conn.executemany(sql, seq)
            self._conn.commit()

    def _commit(self) -> None:
        with self._lock:
            self._conn.commit()

    def _fetchone(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(sql, params).fetchone()

    def _fetchall(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    def close(self) -> None:
        with self._lock:
            self._conn.close()
