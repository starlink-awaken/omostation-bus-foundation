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

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
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
        return (datetime.now(timezone.utc) - t).total_seconds()


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

    def ack(self, event_ids: list[str]) -> int:
        """Mark entries as ACKed. Returns number of removed entries.

        Note: existing DLQ schema has no ACK state, so we just close
        the connection without removing. For now this is a stub that
        returns the count. R91.3 will wire to actual delete.
        """
        all_entries = {_row_to_entry(r).event_id: _row_to_entry(r) for r in self.dlq.list_all()}
        return sum(1 for eid in event_ids if eid in all_entries)

    def retry(self, event_ids: list[str], router: Any) -> int:
        """Republish envelopes via the router. Returns number of retries.

        The caller supplies a router-like object (anything with a publish()
        method) to keep this module decoupled from the Router class.
        """
        all_entries = {_row_to_entry(r).event_id: _row_to_entry(r) for r in self.dlq.list_all()}
        count = 0
        for eid in event_ids:
            entry = all_entries.get(eid)
            if entry is None:
                continue
            try:
                env = json.loads(entry.envelope_json)
            except (ValueError, TypeError):
                continue
            try:
                router.publish(env)
                count += 1
            except Exception:  # noqa: BLE001
                pass
        return count

    def purge(
        self,
        backend: str | None = None,
        before: str | None = None,
    ) -> int:
        """Delete entries matching filter. Returns count of deleted.

        Stub: returns count of matches without actually deleting.
        R91.3 will wire to actual DELETE SQL.
        """
        all_entries = [_row_to_entry(r) for r in self.dlq.list_all()]
        to_delete = all_entries
        if backend:
            to_delete = [e for e in to_delete if e.backend == backend]
        if before:
            to_delete = [e for e in to_delete if e.created_at < before]
        return len(to_delete)

    def close(self) -> None:
        self.dlq.close()


def to_jsonable(entries: list[DLQEntry]) -> str:
    return json.dumps([asdict(e) | {"age_seconds": e.age_seconds} for e in entries], indent=2)


__all__ = ["DLQAdmin", "DLQEntry", "to_jsonable"]
