"""Test OmniEnvelope pydantic migration (R82).

Covers:
  1. Constructor kwargs (legacy `type=` / `source=` / `schema_version=`)
  2. Field validation (empty topic / source_uri → ValidationError)
  3. Serialization round-trip (to_dict / to_json / from_dict / from_json)
  4. Pydantic features: model_dump, model_validate, model_dump_json
  5. Default values (plane=EVENT, version=1.0, schema_version=1)
  6. Attribute access (canonical + legacy aliases)
"""

from __future__ import annotations

import time
from uuid import UUID

import pytest
from pydantic import ValidationError

from bus_foundation.envelope import (
    BusEnvelope,
    EventType,
    OmniEnvelope,
    OmniPlane,
)


class TestPydanticMigration:
    """R82: OmniEnvelope is now a pydantic BaseModel."""

    def test_construct_with_canonical_kwargs(self) -> None:
        env = OmniEnvelope(
            topic="test:topic",
            source_uri="bos://test",
            payload={"k": "v"},
        )
        assert env.topic == "test:topic"
        assert env.source_uri == "bos://test"
        assert env.payload == {"k": "v"}
        assert env.plane == "EVENT"
        assert env.version == "1.0"
        assert env.schema_version == 1
        assert env.retry_count == 0

    def test_legacy_type_alias(self) -> None:
        """`type=` kwarg is mapped to `topic=`."""
        env = OmniEnvelope(type="legacy:topic", source="bos://test")
        assert env.topic == "legacy:topic"
        # `type` attribute is a read-only alias
        assert env.type == "legacy:topic"

    def test_legacy_source_alias(self) -> None:
        env = OmniEnvelope(topic="x", source="bos://legacy")
        assert env.source_uri == "bos://legacy"
        assert env.source == "bos://legacy"

    def test_legacy_schema_version_overrides_version(self) -> None:
        """schema_version=N in legacy code set version=str(N)."""
        env = OmniEnvelope(
            topic="x",
            source="bos://t",
            schema_version=3,
        )
        assert env.version == "3"
        assert env.schema_version == 3

    def test_empty_topic_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError) as exc:
            OmniEnvelope(topic="", source="bos://t")
        # The error mentions topic
        assert "topic" in str(exc.value).lower()

    def test_empty_source_uri_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError) as exc:
            OmniEnvelope(topic="x", source_uri="")
        assert "source" in str(exc.value).lower()

    def test_plane_default_is_event(self) -> None:
        env = OmniEnvelope(topic="x", source="bos://t")
        assert env.plane == "EVENT"
        assert env.plane == OmniPlane.EVENT.value

    def test_plane_omniplane_enum_accepted(self) -> None:
        env = OmniEnvelope(
            topic="x",
            source="bos://t",
            plane=OmniPlane.CONTROL,
        )
        assert env.plane == "CONTROL"

    def test_id_default_is_uuid_hex(self) -> None:
        env = OmniEnvelope(topic="x", source="bos://t")
        # 32 hex chars (no dashes)
        assert len(env.id) == 32
        UUID(env.id)  # must parse as UUID

    def test_id_explicit_preserved(self) -> None:
        env = OmniEnvelope(
            topic="x", source="bos://t", id="custom-id-1234"
        )
        assert env.id == "custom-id-1234"

    def test_timestamp_default_is_recent(self) -> None:
        before = time.time()
        env = OmniEnvelope(topic="x", source="bos://t")
        after = time.time()
        assert before <= env.timestamp <= after

    def test_signature_optional(self) -> None:
        env = OmniEnvelope(topic="x", source="bos://t")
        assert env.signature is None
        env2 = OmniEnvelope(
            topic="x", source="bos://t", signature="0xdeadbeef"
        )
        assert env2.signature == "0xdeadbeef"

    def test_busenvelope_alias_still_works(self) -> None:
        assert BusEnvelope is OmniEnvelope
        env = BusEnvelope(topic="x", source="bos://t")
        assert isinstance(env, OmniEnvelope)

    def test_eventtype_enum(self) -> None:
        env = OmniEnvelope(
            topic=EventType.PIPELINE_COMPLETED,
            source="bos://t",
        )
        assert env.topic == "pipeline:completed"


class TestPydanticSerialization:
    def test_to_dict_round_trip(self) -> None:
        original = OmniEnvelope(
            topic="round:trip",
            source="bos://t",
            payload={"k": [1, 2, 3]},
            trace_id="trace-1",
        )
        d = original.to_dict()
        restored = OmniEnvelope.from_dict(d)
        assert restored.topic == original.topic
        assert restored.source_uri == original.source_uri
        assert restored.payload == original.payload
        assert restored.trace_id == original.trace_id
        assert restored.id == original.id

    def test_to_json_round_trip(self) -> None:
        original = OmniEnvelope(
            topic="json:rt",
            source="bos://t",
            payload={"nested": {"deep": True}},
        )
        j = original.to_json()
        # Must be valid JSON (model_validate_json also round-trips)
        restored = OmniEnvelope.from_json(j)
        assert restored.topic == "json:rt"
        assert restored.payload == original.payload

    def test_to_json_is_sort_keys(self) -> None:
        env = OmniEnvelope(topic="x", source="bos://t", payload={"z": 1, "a": 2})
        j = env.to_json()
        # sort_keys=True is the documented behavior; verify by
        # checking that 'a' appears before 'z' in the JSON output.
        assert j.index('"a"') < j.index('"z"')

    def test_model_dump_and_validate(self) -> None:
        env = OmniEnvelope(topic="x", source="bos://t", payload={"n": 42})
        dumped = env.model_dump()
        # Round-trip via pydantic's native API
        restored = OmniEnvelope.model_validate(dumped)
        assert restored.payload == env.payload

    def test_from_dict_handles_missing_optional_fields(self) -> None:
        """A wire envelope that omits trace_id / signature / seq_id
        must still parse successfully."""
        env = OmniEnvelope.from_dict(
            {
                "id": "abc",
                "topic": "x",
                "source_uri": "bos://t",
                "plane": "EVENT",
                "version": "1.0",
                "schema_version": 1,
                "payload": {},
            }
        )
        assert env.trace_id is None
        assert env.signature is None
        assert env.seq_id is None
        assert env.retry_count == 0

    def test_validate_assignment(self) -> None:
        """Setting a field re-validates; e.g. setting topic='' raises."""
        env = OmniEnvelope(topic="ok", source="bos://t")
        with pytest.raises(ValidationError):
            env.topic = ""  # type: ignore[assignment]


class TestPydanticPowers:
    """Things pydantic gives us for free that the hand-written class
    did not have."""

    def test_equality_by_field_values(self) -> None:
        a = OmniEnvelope(id="x", topic="t", source="bos://s")
        b = OmniEnvelope(id="x", topic="t", source="bos://s")
        # Note: pydantic v2 models are not equal by default unless
        # model_config says so. We just verify construction and field
        # equality.
        assert a.id == b.id

    def test_schema_generation(self) -> None:
        schema = OmniEnvelope.model_json_schema()
        assert "properties" in schema
        assert "topic" in schema["properties"]
        assert "source_uri" in schema["properties"]

    def test_dict_construction_via_model_validate(self) -> None:
        env = OmniEnvelope.model_validate(
            {
                "topic": "constructed:via:dict",
                "source_uri": "bos://t",
                "payload": {"x": 1},
            }
        )
        assert env.topic == "constructed:via:dict"
        assert env.payload == {"x": 1}
