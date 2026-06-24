"""Omni-Bus Unified Envelope implementation.

Implements the L0 Protocol (OmniEnvelope) unifying data, event, and control planes.
"""

from __future__ import annotations

import json
import time
import uuid
from enum import StrEnum
from typing import Any


class OmniPlane(StrEnum):
    DATA = "DATA"
    EVENT = "EVENT"
    CONTROL = "CONTROL"

class EventType(StrEnum):
    """Canonical event types for backward compatibility."""
    PIPELINE_COMPLETED = "pipeline:completed"
    PIPELINE_STARTED = "pipeline:started"
    MESSAGE_RECEIVED = "message:received"


class OmniEnvelope:
    def __init__(
        self,
        plane: str | OmniPlane | None = None,
        topic: str | None = None,
        source_uri: str | None = None,
        payload: dict[str, Any] | None = None,
        trace_id: str | None = None,
        id: str | None = None,
        timestamp: float | None = None,
        version: str = "1.0",
        signature: str | None = None,
        seq_id: int | None = None,
        retry_count: int = 0,
        # Legacy mapping fields for backward compatibility
        type: str | None = None,
        source: str | None = None,
        backend: str | None = None,
        schema_version: int | None = None,
    ):
        if plane is None:
            plane = OmniPlane.EVENT.value
        elif isinstance(plane, OmniPlane):
            plane = plane.value

        topic = topic or type
        if not topic:
            raise ValueError(f"OmniEnvelope.topic (or type) must be non-empty str, got {topic!r}")

        source_uri = source_uri or source
        if not source_uri:
            raise ValueError(f"OmniEnvelope.source_uri (or source) must be non-empty str, got {source_uri!r}")

        self.id = id or uuid.uuid4().hex
        self.trace_id = trace_id
        self.timestamp = timestamp or time.time()
        self.version = version if schema_version is None else str(schema_version)
        self.schema_version = schema_version or 1
        self.backend = backend

        self.plane = plane
        # Fallback to type if topic not provided (legacy compat)
        self.topic = topic
        self.source_uri = source_uri

        self.payload = payload or {}
        self.signature = signature
        self.seq_id = seq_id
        self.retry_count = retry_count

        # Legacy compatibility wrappers
        self.type = self.topic
        self.source = self.source_uri

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "trace_id": self.trace_id,
            "timestamp": self.timestamp,
            "version": self.version,
            "schema_version": self.schema_version,
            "plane": self.plane,
            "topic": self.topic,
            "source_uri": self.source_uri,
            "payload": self.payload,
            "signature": self.signature,
            "seq_id": self.seq_id,
            "retry_count": self.retry_count,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, ensure_ascii=False)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> OmniEnvelope:
        return cls(
            id=d.get("id"),
            trace_id=d.get("trace_id"),
            timestamp=d.get("timestamp"),
            version=d.get("version", "1.0"),
            schema_version=d.get("schema_version", 1),
            plane=d.get("plane", OmniPlane.EVENT.value),
            topic=d.get("topic") or d.get("type", ""),
            source_uri=d.get("source_uri") or d.get("source", ""),
            payload=d.get("payload", {}),
            signature=d.get("signature"),
            seq_id=d.get("seq_id"),
            retry_count=d.get("retry_count", 0),
        )

    @classmethod
    def from_json(cls, s: str) -> OmniEnvelope:
        return cls.from_dict(json.loads(s))

    def __repr__(self) -> str:
        return f"OmniEnvelope(id={self.id!r}, plane={self.plane!r}, topic={self.topic!r})"

# Legacy Alias for backward compatibility
BusEnvelope = OmniEnvelope
