"""R92: tests for schema registry + compatibility check."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from bus_foundation.envelope import BusEnvelope
from bus_foundation.schema import (
    CompatibilityMode,
    SchemaError,
    SchemaRegistry,
    register,
    validate,
)


@pytest.fixture(autouse=True)
def _reset_registry():
    from bus_foundation.schema import _register_default_schemas

    SchemaRegistry.reset()
    _register_default_schemas()
    yield
    SchemaRegistry.reset()


class V1Like(BaseModel):
    """Mimics a v1 envelope (smaller field set)."""

    id: str = "x"
    topic: str = "t"


class V2Like(BaseModel):
    """Mimics a v2 envelope (adds a new field)."""

    id: str = "x"
    topic: str = "t"
    priority: int = 5  # NEW field


class V3Like(BaseModel):
    """Mimics a v3 envelope (renames a field)."""

    id: str = "x"
    subject: str = "t"  # RENAMED from 'topic'


class TestRegistry:
    def test_register_and_get(self):
        e = register("v1", V1Like)
        assert e.name == "v1"
        assert SchemaRegistry.get("v1") is e

    def test_register_idempotent(self):
        e1 = register("v1", V1Like)
        e2 = register("v1", V1Like)
        assert e1 is e2

    def test_get_unknown_returns_none(self):
        assert SchemaRegistry.get("v999") is None


class TestCompatibility:
    def test_strict_same_version_passes(self):
        register("v1", V1Like, compatibility=CompatibilityMode.STRICT)
        env = BusEnvelope(topic="t", payload={"x": 1})
        validate(env, "v1")  # no raise

    def test_strict_different_models_fails(self):
        # Use two unrelated models with disjoint field sets
        class ModelA(BaseModel):
            id: str = "x"
            field_a: str = "a"

        class ModelB(BaseModel):
            id: str = "x"
            field_b: int = 1

        register("va", ModelA, compatibility=CompatibilityMode.STRICT)
        register("vb", ModelB, compatibility=CompatibilityMode.STRICT)
        # Build envelope manually with only ModelA fields
        env = BusEnvelope.model_construct(
            id="x",
            topic="t",
            payload={},
            schema_version=1,
        )
        with pytest.raises(SchemaError, match="strict"):
            validate(env, "vb")

    def test_additive_target_superset_passes(self):
        # Register v1 (small) and v2 (big), additive
        register("v1", V1Like, compatibility=CompatibilityMode.ADDITIVE)
        register("v2", V2Like, compatibility=CompatibilityMode.ADDITIVE)
        # BusEnvelope is bigger than V1Like, so it's safe to treat as v2
        # (and validate against v2 will pass since BusEnvelope has all V2 fields)
        env = BusEnvelope(topic="t", payload={"x": 1})
        # First need to set schema_version to v1
        env_v1 = env.model_copy(update={"schema_version": "v1"})
        validate(env_v1, "v2")  # should pass: v1 fields subset of v2

    def test_breaking_accepts_anything(self):
        register("v1", V1Like, compatibility=CompatibilityMode.BREAKING)
        register("v2", V2Like, compatibility=CompatibilityMode.BREAKING)
        env = BusEnvelope.model_construct(
            id="x",
            topic="t",
            payload={},
            schema_version=1,
        )
        # Even with mismatched fields, breaking allows it
        validate(env, "v2")

    def test_unknown_target_raises(self):
        env = BusEnvelope(topic="t", payload={"x": 1})
        with pytest.raises(SchemaError, match="unknown schema version"):
            validate(env, "v999")

    def test_unknown_actual_version_raises(self):
        register("v2", V2Like, compatibility=CompatibilityMode.STRICT)
        env = BusEnvelope.model_construct(
            id="x",
            topic="t",
            payload={},
            schema_version=999,
        )
        with pytest.raises(SchemaError, match="unknown envelope schema_version"):
            validate(env, "v2")


class TestPublicAPI:
    def test_validate_envelope_via_bus_foundation(self):
        import bus_foundation

        # Default v1 / v2 should be registered
        env = BusEnvelope(topic="t", payload={"x": 1})
        # Should not raise
        bus_foundation.validate_envelope(env, "v2")
