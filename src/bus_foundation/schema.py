"""R92: Schema registry for bus-foundation envelope.

Allows registering multiple envelope schema versions (v1, v2, ...)
and validating that a given envelope is compatible with a target
version. Supports 3 compatibility modes:

  - strict:    fields must match exactly
  - additive:  new fields may be added, no removal/rename
  - breaking:  any change is allowed (developer override)

Default behavior: backward compatible only (additive).

Public API:
    from bus_foundation.schema import register, validate
    register("v1", OmniEnvelopeV1, compatibility="additive")
    register("v2", OmniEnvelopeV2)
    validate(envelope, target_version="v2")
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from enum import Enum
from typing import Any, Type


class CompatibilityMode(str, Enum):
    STRICT = "strict"
    ADDITIVE = "additive"
    BREAKING = "breaking"


@dataclass
class SchemaEntry:
    name: str
    model: Type
    compatibility: CompatibilityMode
    field_set: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        # Cache field set on first registration
        try:
            self.field_set = frozenset(self.model.model_fields.keys())
        except AttributeError:
            self.field_set = frozenset(dir(self.model))


class SchemaRegistry:
    """Process-global registry. Thread-safe."""

    _schemas: dict[str, SchemaEntry] = {}
    _lock = threading.Lock()

    @classmethod
    def register(
        cls,
        name: str,
        model: Type,
        compatibility: CompatibilityMode = CompatibilityMode.ADDITIVE,
    ) -> SchemaEntry:
        with cls._lock:
            existing = cls._schemas.get(name)
            if existing is not None:
                return existing
            entry = SchemaEntry(name=name, model=model, compatibility=compatibility)
            cls._schemas[name] = entry
            return entry

    @classmethod
    def get(cls, name: str) -> SchemaEntry | None:
        with cls._lock:
            return cls._schemas.get(name)

    @classmethod
    def all(cls) -> dict[str, SchemaEntry]:
        with cls._lock:
            return dict(cls._schemas)

    @classmethod
    def reset(cls) -> None:
        """Test helper."""
        with cls._lock:
            cls._schemas = {}


def register(
    name: str,
    model: Type,
    compatibility: CompatibilityMode | str = CompatibilityMode.ADDITIVE,
) -> SchemaEntry:
    """Public registration helper."""
    if isinstance(compatibility, str):
        compatibility = CompatibilityMode(compatibility)
    return SchemaRegistry.register(name, model, compatibility)


def validate(envelope: Any, target_version: str) -> None:
    """Validate an envelope object against the target schema version.

    Raises:
        SchemaError if envelope is not compatible with target version.
    """
    target = SchemaRegistry.get(target_version)
    if target is None:
        raise SchemaError(f"unknown schema version: {target_version}")

    # Get the envelope's schema version (string)
    actual_version = getattr(envelope, "schema_version", None)
    if actual_version is None:
        # Treat as "v1" (legacy)
        actual_version = "v1"
    # Normalize numeric versions (1 → v1, 2 → v2)
    if isinstance(actual_version, int):
        actual_version = f"v{actual_version}"
    elif actual_version.isdigit():
        actual_version = f"v{actual_version}"

    actual = SchemaRegistry.get(actual_version)
    if actual is None:
        raise SchemaError(f"unknown envelope schema_version: {actual_version}")

    # Same version: pass through
    if actual_version == target_version:
        return

    # Strict: must be exact match
    if target.compatibility == CompatibilityMode.STRICT:
        if actual.field_set != target.field_set:
            raise SchemaError(
                f"strict compatibility: v{actual_version} fields {actual.field_set} "
                f"!= v{target_version} fields {target.field_set}"
            )
        return

    # Additive: target must have at least all fields of actual
    if target.compatibility == CompatibilityMode.ADDITIVE:
        missing = actual.field_set - target.field_set
        if missing:
            raise SchemaError(
                f"additive compatibility: v{target_version} is missing "
                f"fields {missing} from v{actual_version}"
            )
        return

    # Breaking: anything goes
    if target.compatibility == CompatibilityMode.BREAKING:
        return


class SchemaError(ValueError):
    """Raised when an envelope is incompatible with the target schema version."""


# ── Default registrations ────────────────────────────────────────

def _register_default_schemas() -> None:
    """Register v1 / v2 schemas on first import."""
    # Lazy import to avoid circular dependencies
    try:
        from bus_foundation.envelope import BusEnvelope
    except ImportError:
        return
    # v1 = original envelope (current 0.2.0 R82)
    if SchemaRegistry.get("v1") is None:
        register("v1", BusEnvelope, compatibility=CompatibilityMode.ADDITIVE)
    # v2 = placeholder for next major bump (same model for now,
    # but separate entry to prove registry works)
    if SchemaRegistry.get("v2") is None:
        register("v2", BusEnvelope, compatibility=CompatibilityMode.STRICT)


_register_default_schemas()


__all__ = [
    "CompatibilityMode",
    "SchemaEntry",
    "SchemaRegistry",
    "SchemaError",
    "register",
    "validate",
]
