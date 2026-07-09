"""Test DataPlaneBackend — R80 pattern filtering + per-sub matchers."""

from __future__ import annotations

import time

from bus_foundation.backends.data_plane import DataPlaneBackend
from bus_foundation.envelope import OmniEnvelope


def _env(topic: str, source: str = "bos://test") -> OmniEnvelope:
    return OmniEnvelope(topic=topic, source_uri=source, payload={"topic": topic})


class TestDataPlanePatternFiltering:
    """R80: subscribe(pattern, cb) only delivers matching envelopes."""

    def test_pattern_filters_exact_match(self) -> None:
        be = DataPlaneBackend(flush_interval=0.1, capacity=100)
        seen: list[str] = []

        def cb(batch: list[OmniEnvelope]) -> None:
            seen.extend(e.type for e in batch)

        be.subscribe("telemetry:cpu", cb)
        # Publish a mix.
        be.publish(_env("telemetry:cpu"))
        be.publish(_env("telemetry:mem"))
        be.publish(_env("telemetry:cpu"))
        time.sleep(0.3)  # let the flush loop run
        # Subscriber should have only seen telemetry:cpu, twice.
        assert seen == ["telemetry:cpu", "telemetry:cpu"]
        be.unsubscribe_sub_id_safe() if False else None
        be.close()

    def test_pattern_wildcard_catches_all(self) -> None:
        be = DataPlaneBackend(flush_interval=0.1, capacity=100)
        seen: list[str] = []

        def cb(batch: list[OmniEnvelope]) -> None:
            seen.extend(e.type for e in batch)

        be.subscribe("*", cb)
        be.publish(_env("anything:goes"))
        be.publish(_env("foo:bar"))
        time.sleep(0.3)
        assert seen == ["anything:goes", "foo:bar"]
        be.close()

    def test_pattern_prefix(self) -> None:
        be = DataPlaneBackend(flush_interval=0.1, capacity=100)
        seen: list[str] = []

        def cb(batch: list[OmniEnvelope]) -> None:
            seen.extend(e.type for e in batch)

        be.subscribe("telemetry:*", cb)
        be.publish(_env("telemetry:cpu"))
        be.publish(_env("telemetry:mem"))
        be.publish(_env("metric:cpu"))  # not telemetry
        time.sleep(0.3)
        assert sorted(seen) == ["telemetry:cpu", "telemetry:mem"]
        be.close()

    def test_empty_pattern_treated_as_catchall(self) -> None:
        """R80: empty pattern (legacy callers) must not silently drop."""
        be = DataPlaneBackend(flush_interval=0.1, capacity=100)
        seen: list[str] = []

        def cb(batch: list[OmniEnvelope]) -> None:
            seen.extend(e.type for e in batch)

        be.subscribe("", cb)
        be.publish(_env("legacy:topic"))
        time.sleep(0.3)
        assert seen == ["legacy:topic"]
        be.close()

    def test_two_subscribers_independent_patterns(self) -> None:
        be = DataPlaneBackend(flush_interval=0.1, capacity=100)
        seen_a: list[str] = []
        seen_b: list[str] = []

        be.subscribe("a:*", lambda batch: seen_a.extend(e.type for e in batch))
        be.subscribe("b:*", lambda batch: seen_b.extend(e.type for e in batch))

        be.publish(_env("a:1"))
        be.publish(_env("b:1"))
        be.publish(_env("c:1"))  # neither
        time.sleep(0.3)
        assert seen_a == ["a:1"]
        assert seen_b == ["b:1"]
        be.close()

    def test_unsubscribe_stops_delivery(self) -> None:
        be = DataPlaneBackend(flush_interval=0.1, capacity=100)
        seen: list[str] = []
        sub = be.subscribe("*", lambda batch: seen.extend(e.type for e in batch))
        be.publish(_env("a"))
        time.sleep(0.3)
        assert seen == ["a"]
        # Unsubscribe.
        assert be.unsubscribe(sub) is True
        be.publish(_env("b"))
        time.sleep(0.3)
        # b must NOT be delivered.
        assert seen == ["a"]
        be.close()
