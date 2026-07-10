"""SQLite DLQ — dead letter queue for failed bus events.

Phase B (R66): WAL mode + busy_timeout 5000.
R83: replace hard-rename + unlink with tar.gz archive + retention.

When the DLQ database reaches DLQ_MAX_SIZE_MB, instead of renaming
it to .db.old and unlinking (data loss), we now:
  1. Close the current connection
  2. Create a timestamped tar.gz archive of the full db
  3. Delete the archive only when it exceeds DLQ_RETENTION_DAYS
     (default 7), removed oldest first
  4. Open a fresh empty database

This preserves forensic access to dead-letter events for at least
the retention window, matching the operational SLA of the control
plane backend.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import tarfile
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

DLQ_MAX_SIZE_MB = 50
DLQ_MAX_RETRIES = 3
DLQ_RETENTION_DAYS = 7
DEFAULT_DB_PATH = Path(os.environ.get("BUS_DLQ_PATH", str(Path.home() / ".runtime" / "bus_dlq.db")))

# Archives written to the same directory as the db file.
ARCHIVE_GLOB = "bus_dlq_archive_*.tar.gz"


class DLQ:
    """Thread-safe SQLite DLQ with WAL + tar.gz archive + retention."""

    def __init__(
        self,
        db_path: Path | str = DEFAULT_DB_PATH,
        max_size_mb: int = DLQ_MAX_SIZE_MB,
        retention_days: int = DLQ_RETENTION_DAYS,
    ):
        self.db_path = Path(db_path)
        self.max_size_mb = max_size_mb
        self.retention_days = retention_days
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = self._open_conn()
        self._init_schema()
        self._maybe_rotate()
        self._purge_expired_archives()

    def _open_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS dlq (
                    event_id TEXT PRIMARY KEY,
                    backend TEXT NOT NULL,
                    envelope_json TEXT NOT NULL,
                    error TEXT,
                    retries INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'PENDING',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            self._conn.execute("""
                CREATE INDEX IF NOT EXISTS dlq_status_idx
                ON dlq(status)
            """)
            self._conn.commit()

    def _maybe_rotate(self) -> None:
        """Close the current DB, compress it to a tar.gz archive, and start fresh.

        R83: was hard-rename + unlink (data loss). Now preserves the
        archive for DLQ_RETENTION_DAYS before cleanup.
        """
        if not self.db_path.exists():
            return
        size_mb = self.db_path.stat().st_size / (1024 * 1024)
        if size_mb < self.max_size_mb:
            return

        # Flush WAL to the main file so the archive is consistent.
        self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self._conn.close()

        archive_name = f"bus_dlq_archive_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.tar.gz"
        archive_path = self.db_path.parent / archive_name
        # Vacuum first to shrink the db before archiving.
        try:
            import subprocess
            subprocess.run(
                ["sqlite3", str(self.db_path), "VACUUM;"],
                capture_output=True,
                timeout=30,
            )
        except Exception:  # noqa: BLE001
            pass
        # Create the archive.
        try:
            with tarfile.open(str(archive_path), "w:gz") as tar:
                tar.add(str(self.db_path), arcname=self.db_path.name)
        except Exception as e:  # noqa: BLE001
            logger.error("dlq_archive_failed path=%s err=%s", archive_path, e)
            # Fallback: hard rename (same as the old behaviour).
            old = self.db_path.with_suffix(".db.old")
            old.unlink(missing_ok=True)
            self.db_path.rename(old)
        else:
            # Delete the original db file now that it's archived.
            self.db_path.unlink(missing_ok=True)
            logger.warning(
                "dlq_rotated size_mb=%.1f archive=%s", size_mb, archive_name
            )

        # Open fresh connection.
        self._conn = self._open_conn()
        self._init_schema()

    def _purge_expired_archives(self) -> None:
        """Delete archives older than retention_days."""
        now = time.time()
        cutoff = now - (self.retention_days * 86400)
        archive_dir = self.db_path.parent
        for p in sorted(archive_dir.glob(ARCHIVE_GLOB)):
            try:
                mtime = p.stat().st_mtime
                if mtime < cutoff:
                    p.unlink()
                    logger.warning("dlq_archive_purged path=%s age_days=%.1f", p, (now - mtime) / 86400)
            except OSError as e:
                logger.error("dlq_archive_purge_failed path=%s err=%s", p, e)

    def list_archives(self) -> list[dict]:
        """Return metadata about all retained archive files."""
        now = time.time()
        result = []
        for p in sorted(self.db_path.parent.glob(ARCHIVE_GLOB)):
            try:
                stat = p.stat()
                result.append({
                    "path": p.name,
                    "size_bytes": stat.st_size,
                    "age_days": round((now - stat.st_mtime) / 86400, 2),
                    "created_at": datetime.fromtimestamp(stat.st_mtime, UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                })
            except OSError:
                continue
        return result

    def _now(self) -> str:
        return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    def enqueue(
        self,
        event_id: str,
        backend: str,
        envelope_json: str,
        error: str,
    ) -> None:
        # R91: apply redaction before persisting
        from .dlq_redaction import Redactor, load_user_config
        redactor = getattr(self, "_redactor", None) or Redactor(load_user_config())
        self._redactor = redactor  # cache
        redacted_json = redactor.redact_json(envelope_json, backend=backend)

        now = self._now()
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO dlq
                    (event_id, backend, envelope_json, error, retries, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, 0, 'PENDING', ?, ?)
                """,
                (event_id, backend, redacted_json, error, now, now),
            )
            self._conn.commit()

    def requeue(self, event_id: str, error: str) -> None:
        now = self._now()
        with self._lock:
            row = self._conn.execute("SELECT retries FROM dlq WHERE event_id = ?", (event_id,)).fetchone()
            if row is None:
                logger.warning("dlq_requeue_missing", event_id)
                return
            new_retries = row["retries"] + 1
            new_status = "DLQ" if new_retries >= DLQ_MAX_RETRIES else "PENDING"
            self._conn.execute(
                """
                UPDATE dlq
                SET retries = ?, status = ?, error = ?, updated_at = ?
                WHERE event_id = ?
                """,
                (new_retries, new_status, error, now, event_id),
            )
            self._conn.commit()

    def list_all(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM dlq ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
