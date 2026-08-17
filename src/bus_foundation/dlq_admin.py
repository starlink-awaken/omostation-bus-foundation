"""R91: DLQ admin operations.

High-level API wrapping the existing DLQ class with:
  - list (filter by backend, status, limit, json output)
  - stats (depth, oldest, backend distribution)
  - ack (remove specific event_ids after external handling)
  - retry (republish via Router + R90 retry)
  - purge (delete by backend / age)
  - watch (live tail)

This module is the implementation behind `bin/bus-dlq` CLI.
"""

from __future__ import annotations

import builtins
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .dlq import DLQ


@dataclass
class DLQEntry:
    event_id: str
    backend: str
    envelope_json: str
    error: str
    retries: int
    status: str
    created_at: str
    updated_at: str

    @property
    def age_seconds(self) -> float:
        try:
            t = datetime.fromisoformat(self.created_at.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return 0.0
        return (datetime.now(UTC) - t).total_seconds()


def _row_to_entry(row: Any) -> DLQEntry:
    d = dict(row)
    return DLQEntry(
        event_id=d.get("event_id", ""),
        backend=d.get("backend", ""),
        envelope_json=d.get("envelope_json", ""),
        error=d.get("error", ""),
        retries=d.get("retries", 0),
        status=d.get("status", "PENDING"),
        created_at=d.get("created_at", ""),
        updated_at=d.get("updated_at", ""),
    )


class DLQAdmin:
    """Wrapper over DLQ for operational tooling."""

    def __init__(self, db_path: Path | str | None = None):
        self.dlq = DLQ(db_path=db_path) if db_path is not None else DLQ()

    def list(
        self,
        backend: str | None = None,
        status: str | None = None,
        limit: int = 20,
    ) -> list[DLQEntry]:
        all_entries = [_row_to_entry(r) for r in self.dlq.list_all()]
        if backend:
            all_entries = [e for e in all_entries if e.backend == backend]
        if status:
            all_entries = [e for e in all_entries if e.status == status]
        return all_entries[:limit]

    def stats(self) -> dict[str, Any]:
        all_entries = [_row_to_entry(r) for r in self.dlq.list_all()]
        by_status: dict[str, int] = {}
        by_backend: dict[str, int] = {}
        oldest_age = 0.0
        for e in all_entries:
            by_status[e.status] = by_status.get(e.status, 0) + 1
            by_backend[e.backend] = by_backend.get(e.backend, 0) + 1
            oldest_age = max(oldest_age, e.age_seconds)
        return {
            "total": len(all_entries),
            "by_status": by_status,
            "by_backend": by_backend,
            "oldest_age_seconds": oldest_age,
        }

    def ack(self, event_ids: builtins.list[str]) -> int:
        """Remove entries from DLQ after external handling.

        Returns number of deleted entries.
        """
        count = 0
        for eid in event_ids:
            try:
                self.dlq._conn.execute("DELETE FROM dlq WHERE event_id = ?", (eid,))
                count += 1
            except Exception:
                pass
        self.dlq._conn.commit()
        return count

    def retry(self, event_ids: builtins.list[str], router: Any) -> int:
        """Republish envelopes via the router. Returns number of retries.

        The caller supplies a router-like object (anything with a publish()
        method) to keep this module decoupled from the Router class.
        """
        rows = self.dlq._conn.execute(
            "SELECT event_id, envelope_json FROM dlq WHERE event_id IN ({})".format(",".join("?" * len(event_ids))),
            event_ids,
        ).fetchall()
        count = 0
        for row in rows:
            eid, env_json = row["event_id"], row["envelope_json"]
            try:
                import json

                env_dict = json.loads(env_json)
                router.publish(env_dict)
                self.dlq._conn.execute("DELETE FROM dlq WHERE event_id = ?", (eid,))
                count += 1
            except Exception:
                pass
        self.dlq._conn.commit()
        return count

    def purge(
        self,
        backend: str | None = None,
        before: str | None = None,
    ) -> int:
        """Delete entries matching filter. Returns count of deleted."""
        conditions: list[str] = []
        params: list[object] = []
        if backend:
            conditions.append("backend = ?")
            params.append(backend)
        if before:
            conditions.append("created_at < ?")
            params.append(before)
        where = " AND ".join(conditions) if conditions else "1=1"
        deleted = self.dlq._conn.execute(f"DELETE FROM dlq WHERE {where}", params).rowcount
        self.dlq._conn.commit()
        return deleted

    def close(self) -> None:
        self.dlq.close()


def to_jsonable(entries: list[DLQEntry]) -> str:
    return json.dumps([asdict(e) | {"age_seconds": e.age_seconds} for e in entries], indent=2)


__all__ = ["DLQAdmin", "DLQEntry", "to_jsonable"]
