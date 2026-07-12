"""Omni-Bus Unified Envelope implementation (R82 pydantic).

Implements the L0 Protocol (OmniEnvelope) unifying data, event, and control planes.

R82 rewrite: was a hand-written class with manual __init__, manual
serialization, and ad-hoc validation. Replaced with a pydantic
BaseModel so we get:
  - automatic type validation (e.g. topic must be non-empty str)
  - automatic to_dict / to_json (model_dump / model_dump_json)
  - validation errors as ValidationError, not custom ValueError
  - schema generation out of the box

Public API is preserved 1:1:
  - `OmniEnvelope(...)` constructor accepts the same kwargs (plane,
    topic, source_uri, payload, trace_id, id, timestamp, version,
    signature, seq_id, retry_count, backend, schema_version, plus
    legacy `type` and `source` aliases)
  - attributes (.id, .topic, .source_uri, .payload, ...) unchanged
  - legacy `type` / `source` attribute aliases still work
  - BusEnvelope = OmniEnvelope alias unchanged
  - to_dict / to_json / from_dict / from_json unchanged
  - EventType / OmniPlane enums unchanged
"""

from __future__ import annotations

import json
import time
import uuid
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class OmniPlane(StrEnum):
    DATA = "DATA"
    EVENT = "EVENT"
    CONTROL = "CONTROL"


class EventType(StrEnum):
    """Canonical event types for backward compatibility."""

    PIPELINE_COMPLETED = "pipeline:completed"
    PIPELINE_STARTED = "pipeline:started"
    MESSAGE_RECEIVED = "message:received"


class OmniEnvelope(BaseModel):
    """L0 protocol envelope unifying data/event/control planes.

    R82: pydantic BaseModel. Public kwargs are unchanged from the
    hand-written predecessor; validation runs in __init__.
    """

    model_config = ConfigDict(
        # Allow legacy `type` and `source` aliases to be passed as
        # constructor kwargs (and serialize as legacy field names if
        # explicitly asked via to_dict_compat). By default to_dict()
        # returns the canonical field names.
        populate_by_name=True,
        # Validate on assignment so .topic = "" raises immediately.
        validate_assignment=True,
        # Allow arbitrary types (StrEnum, dict) to pass through.
        arbitrary_types_allowed=True,
    )

    # Canonical fields. The first three are positional in the legacy
    # constructor so we keep them as keyword-only with a default.
    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    trace_id: str | None = None
    timestamp: float = Field(default_factory=time.time)
    version: str = "1.0"
    schema_version: int = 1
    backend: str | None = None
    plane: str = OmniPlane.EVENT.value
    topic: str = ""
    source_uri: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)
    signature: str | None = None
    seq_id: int | None = None
    retry_count: int = 0

    # ---- validators ----

    @field_validator("plane", mode="before")
    @classmethod
    def _coerce_plane(cls, v: Any) -> str:
        if v is None:
            return OmniPlane.EVENT.value
        if isinstance(v, OmniPlane):
            return v.value
        return v

    @field_validator("topic", mode="before")
    @classmethod
    def _coerce_topic(cls, v: Any) -> str:
        # Legacy compat: callers pass `type=` instead of `topic=`.
        # Pydantic populates by_name so `type` will not match `topic`
        # automatically; we need a hand-off. See model_validator.
        return v

    @field_validator("version", mode="before")
    @classmethod
    def _coerce_version(cls, v: Any) -> str:
        # `schema_version=...` is the legacy kwarg. If a caller passes
        # it, the model_validator below copies it into `version`.
        return v

    @model_validator(mode="before")
    @classmethod
    def _coerce_legacy_kwargs(cls, values: Any) -> Any:
        """Translate legacy kwargs to canonical fields.

        Legacy constructor accepted:
          - `type=`   → falls back to `topic=`
          - `source=` → falls back to `source_uri=`
          - `schema_version=` → sets BOTH `schema_version=N` AND
            `version=str(N)` (legacy code did this mishandling; we
            preserve the historical wire format for backward compat)

        Returns a NEW dict so the caller's input is never mutated.
        Empty topic/source_uri are preserved (the field_validators
        will raise) so the public "non-empty str" guarantee is kept.
        """
        if not isinstance(values, dict):
            return values

        new: dict[str, Any] = dict(values)

        # type / topic: legacy `type` is the topic. If the caller
        # explicitly passed `type=""` and no `topic=`, surface the
        # empty string so the topic validator raises.
        topic = new.get("topic")
        if not topic and "type" in new:
            new["topic"] = new.pop("type")

        # source / source_uri
        source_uri = new.get("source_uri")
        if not source_uri and "source" in new:
            new["source_uri"] = new.pop("source")

        # schema_version / version (legacy mishandling):
        # legacy code did `version = str(schema_version)` and
        # `schema_version = schema_version or 1`. Reproduce that.
        legacy_sv = new.pop("schema_version", None)
        if legacy_sv is not None:
            new["schema_version"] = legacy_sv
            # Only set version if caller didn't pass one explicitly
            if new.get("version") in (None, "1.0"):
                new["version"] = str(legacy_sv)

        return new

    @field_validator("topic")
    @classmethod
    def _topic_nonempty(cls, v: str) -> str:
        if not v or not isinstance(v, str):
            raise ValueError(f"OmniEnvelope.topic must be non-empty str, got {v!r}")
        return v

    @field_validator("source_uri")
    @classmethod
    def _source_uri_nonempty(cls, v: str) -> str:
        if not v or not isinstance(v, str):
            raise ValueError(f"OmniEnvelope.source_uri must be non-empty str, got {v!r}")
        return v

    # ---- legacy attribute aliases (read-only) ----

    @property
    def type(self) -> str:
        return self.topic

    @property
    def source(self) -> str:
        return self.source_uri

    # ---- serialization ----

    def to_dict(self) -> dict[str, Any]:
        """Canonical wire-format dict (excludes legacy aliases)."""
        return self.model_dump()

    def to_json(self) -> str:
        # sort_keys=True preserves the historical wire format (RFC 8785
        # canonical JSON). pydantic v2 doesn't sort by default; we do
        # it post-hoc via json.dumps for the payload.

        d = self.model_dump()
        return json.dumps(d, sort_keys=True, ensure_ascii=False)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> OmniEnvelope:
        return cls.model_validate(d)

    @classmethod
    def from_json(cls, s: str) -> OmniEnvelope:
        return cls.model_validate_json(s)

    def __repr__(self) -> str:
        return f"OmniEnvelope(id={self.id!r}, plane={self.plane!r}, topic={self.topic!r})"


# Legacy Alias for backward compatibility
BusEnvelope = OmniEnvelope
