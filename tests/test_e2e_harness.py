# pyright: reportCallIssue=false

"""Cross-backend e2e harness for bus-foundation (Round 0).

Pattern: given a backend A and backend B (possibly different types),
verify that an envelope published on A is delivered to a subscriber
on B (via a shared Router + DLQ + retry layer). This is the
foundation for Round 5's 13-scenario cross-project e2e suite.

We test 3 in-process backends (eventbus, messagebus, sse) plus the
Router+DLQ wiring. asyncio is excluded here because its subscribe
requires a running event loop (it logs asyncio_subscribe_no_loop
otherwise). Use tests/test_asyncio_backend.py for asyncio coverage.

For real cross-process e2e (Redis/ZMQ/WS), use the bin/bus-e2e-harness.py
script (added in Round 5).
"""

from __future__ import annotations

import time
from collections.abc import Callable

import pytest

from bus_foundation.backends.eventbus import EventBusBackend
from bus_foundation.backends.messagebus import MessageBusBackend
from bus_foundation.backends.sse import SSEBackend
from bus_foundation.dlq import DLQ
from bus_foundation.envelope import BusEnvelope
from bus_foundation.router import Router


def _env(topic: str = "e2e:ping", **payload: object) -> BusEnvelope:
    return BusEnvelope(
        type=topic,
        source="e2e-harness",
        payload={"ts": time.time(), **payload},
    )


def _wait_for(predicate: Callable[[], bool], timeout: float = 2.0) -> bool:
    """Poll a condition until it becomes true or timeout elapses."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


@pytest.fixture
def dlq(tmp_path) -> DLQ:
    return DLQ(db_path=tmp_path / "dlq.db")


class TestCrossBackendE2E:
    """Smoke: every in-process backend routes a publish to a subscriber."""

    def test_eventbus_publish_subscribe(self) -> None:
        be = EventBusBackend()
        received: list[BusEnvelope] = []
        be.subscribe("e2e:eventbus:*", lambda env: received.append(env))
        env = _env("e2e:eventbus:hello")
        be.publish(env)
        assert _wait_for(lambda: len(received) == 1)
        assert received[0].id == env.id

    def test_messagebus_publish_subscribe(self) -> None:
        be = MessageBusBackend()
        received: list[BusEnvelope] = []
        be.subscribe("e2e:messagebus:*", lambda env: received.append(env))
        env = _env("e2e:messagebus:hello")
        be.publish(env)
        assert _wait_for(lambda: len(received) == 1)

    def test_sse_publish_subscribe(self) -> None:
        be = SSEBackend()
        received: list[BusEnvelope] = []
        be.subscribe("e2e:sse:*", lambda env: received.append(env))
        env = _env("e2e:sse:hello")
        be.publish(env)
        assert _wait_for(lambda: len(received) == 1)


class TestRouterWithBackend:
    """Router + DLQ must work with any backend."""

    def test_router_routes_to_backend(self, dlq: DLQ) -> None:
        be = EventBusBackend()
        router = Router(backend=be, dlq=dlq)
        received: list[BusEnvelope] = []
        be.subscribe("e2e:router:*", lambda env: received.append(env))
        env = _env("e2e:router:hello")
        event_id = router.publish(env)
        assert event_id == env.id
        assert _wait_for(lambda: len(received) == 1)

    def test_router_dlq_on_unavailable_backend(self, dlq: DLQ) -> None:
        """When backend reports unavailable, router writes to DLQ."""

        class _BrokenBackend(EventBusBackend):
            def is_available(self) -> bool:
                return False

        broken = _BrokenBackend()
        router = Router(backend=broken, dlq=dlq)
        env = _env("e2e:dlq:hello")
        event_id = router.publish(env)
        # Even when backend is unavailable, the envelope's id is returned
        assert event_id == env.id
        # Verify it landed in DLQ
        all_entries = dlq.list_all()
        assert any(e["event_id"] == env.id for e in all_entries), (
            f"envelope {env.id} should be in DLQ, got: {[e['event_id'] for e in all_entries]}"
        )


class TestTopicSSOTUsage:
    """Smoke: topics.py constants can be used as publish topics and patterns."""

    def test_publish_with_topic_constant(self) -> None:
        from bus_foundation import topics
        from bus_foundation.backends.eventbus import EventBusBackend

        be = EventBusBackend()
        received: list[BusEnvelope] = []
        # Subscribe to the constant pattern
        be.subscribe(topics.PATTERN_PIPELINE_ALL, lambda env: received.append(env))
        # Publish with the constant topic
        env = BusEnvelope(
            type=topics.PIPELINE_COMPLETED,
            source="e2e-topic-ssot",
            payload={"id": "x"},
        )
        be.publish(env)
        assert _wait_for(lambda: len(received) == 1)
        assert received[0].topic == topics.PIPELINE_COMPLETED


class TestPatternMatching:
    """Pattern matching: a:* matches a:b, a:b:c, etc."""

    def test_colon_wildcard_matches(self) -> None:
        from bus_foundation.backends.eventbus import EventBusBackend

        be = EventBusBackend()
        received: list[BusEnvelope] = []
        be.subscribe("foo:*", lambda env: received.append(env))
        for topic in ["foo:a", "foo:b:c", "foo:bar:baz:qux"]:
            be.publish(BusEnvelope(type=topic, source="test"))
        assert _wait_for(lambda: len(received) == 3)

    def test_pattern_does_not_match_unrelated(self) -> None:
        from bus_foundation.backends.eventbus import EventBusBackend

        be = EventBusBackend()
        received: list[BusEnvelope] = []
        be.subscribe("foo:*", lambda env: received.append(env))
        be.publish(BusEnvelope(type="bar:thing", source="test"))
        be.publish(BusEnvelope(type="foo:thing", source="test"))
        # Should only receive the foo one
        assert _wait_for(lambda: len(received) == 1, timeout=0.5)
        assert received[0].topic == "foo:thing"
