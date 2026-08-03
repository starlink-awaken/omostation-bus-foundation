# pyright: reportCallIssue=false

"""Benchmark baselines for bus-foundation 0.3.0.

Usage:
    uv run --with "pytest-benchmark" pytest tests/benchmarks/ -v --benchmark-only
    uv run --with "pytest-benchmark" pytest tests/benchmarks/ --benchmark-histogram
"""

from __future__ import annotations

import pytest

from bus_foundation import BusEnvelope, EventType
from bus_foundation.backends.eventbus import EventBusBackend
from bus_foundation.dlq import DLQ
from bus_foundation.envelope import EventType as ET
from bus_foundation.metrics import Registry as MetricsRegistry

# ── Fixtures ───────────────────────────────────────────────────────


@pytest.fixture
def eventbus() -> EventBusBackend:
    return EventBusBackend()


@pytest.fixture
def sample_envelope() -> BusEnvelope:
    return BusEnvelope(type=EventType.PIPELINE_COMPLETED, source="bench", payload={"x": 1})


# ── EventBus benchmarks ────────────────────────────────────────────


def test_eventbus_publish_throughput(eventbus: EventBusBackend, sample_envelope: BusEnvelope, benchmark) -> None:
    """P50 < 5µs, P99 < 20µs (EventBus in-process dict fanout)."""

    def _publish() -> str:
        return eventbus.publish(sample_envelope)

    result = benchmark(_publish)
    assert result == sample_envelope.id


def test_eventbus_subscribe_throughput(eventbus: EventBusBackend, benchmark) -> None:
    """Subscribe + pattern match dispatch."""
    received: list[BusEnvelope] = []
    eventbus.subscribe("*", lambda e: received.append(e))

    def _publish() -> str:
        return eventbus.publish(BusEnvelope(type=ET.PIPELINE_COMPLETED, source="b"))

    benchmark(_publish)
    assert len(received) >= 1


def test_eventbus_pattern_match(eventbus: EventBusBackend, benchmark) -> None:
    """Pattern matching overhead with wildcard and exact patterns."""
    eventbus.subscribe("pipeline:*", lambda e: None)
    eventbus.subscribe("metric:*", lambda e: None)
    eventbus.subscribe("specific.event", lambda e: None)

    def _publish_and_filter() -> str:
        return eventbus.publish(BusEnvelope(type="pipeline:completed", source="b"))

    benchmark(_publish_and_filter)


# ── Envelope benchmarks ────────────────────────────────────────────


def test_envelope_create(benchmark) -> None:
    """BusEnvelope creation overhead."""

    def _create() -> BusEnvelope:
        return BusEnvelope(type=ET.PIPELINE_COMPLETED, source="bench", payload={"id": 1, "name": "test"})

    env = benchmark(_create)
    assert env.id is not None


def test_envelope_serialize(benchmark) -> None:
    """envelope.to_json() overhead."""
    env = BusEnvelope(type=ET.PIPELINE_COMPLETED, source="bench", payload={"x": 1})

    def _serialize() -> str:
        return env.to_json()

    result = benchmark(_serialize)
    assert isinstance(result, str)


# ── DLQ benchmarks ─────────────────────────────────────────────────


def test_dlq_enqueue_throughput(tmp_path, benchmark) -> None:
    """DLQ INSERT overhead."""
    import json

    dlq = DLQ(db_path=tmp_path / "bench_dlq.db")

    def _enqueue() -> None:
        dlq.enqueue("evt-1", "eventbus", json.dumps({"x": 1}), "bench error")

    benchmark(_enqueue)
    dlq.close()


# ── Metrics overhead benchmark ──────────────────────────────────────


def test_metrics_disabled_overhead(eventbus: EventBusBackend, benchmark) -> None:
    """Publish throughput with metrics DISABLED (default, zero-overhead)."""
    MetricsRegistry.disable()

    def _publish() -> str:
        return eventbus.publish(BusEnvelope(type=ET.PIPELINE_COMPLETED, source="b"))

    benchmark(_publish)


def test_metrics_enabled_overhead(eventbus: EventBusBackend, benchmark) -> None:
    """Publish throughput with metrics ENABLED (no HTTP server)."""
    MetricsRegistry.enable()

    def _publish() -> str:
        return eventbus.publish(BusEnvelope(type=ET.PIPELINE_COMPLETED, source="b"))

    benchmark(_publish)
    MetricsRegistry.disable()
