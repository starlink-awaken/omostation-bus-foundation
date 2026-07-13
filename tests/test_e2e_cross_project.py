"""Cross-project e2e for bus-foundation Round 5.

Simulates the omo → agora → cockpit event flow end-to-end through the
in-process eventbus. Validates that topics published by one consumer
flow through Router + DLQ + retry layer to another consumer.

Why in-process: this test must run in CI without external services
(Redis, ZMQ, websockets). The same publisher/subscriber contract
applies to all 8 backends; the cross-process test lives in
``bin/bus-e2e-harness.py`` (Round 5 follow-up).

Scenarios:
  1. omo publishes omo:audit:completed → cockpit receives
  2. kairon publishes kairon:source:ingested → omo (governance) receives
  3. metaos publishes metaos:node:completed → cockpit receives
  4. aetherforge publishes swarm:hatcher:initialized → omo receives
  5. runtime publishes runtime:cron:fired → cockpit receives
"""

from __future__ import annotations

import time
from collections.abc import Callable

import pytest

from bus_foundation import _backends
from bus_foundation.backends.eventbus import EventBusBackend
from bus_foundation.dlq import DLQ
from bus_foundation.envelope import BusEnvelope
from bus_foundation.router import Router


def _wait_for(predicate: Callable[[], bool], timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


class _BusHarness:
    """Single-bus harness for cross-project e2e.

    All 5 publishers and 5 subscribers share the same in-process
    eventbus. The Router + DLQ provide the same surface a real
    distributed deployment would.
    """

    def __init__(self, tmp_path) -> None:
        self.be = EventBusBackend()
        self.dlq = DLQ(db_path=tmp_path / "dlq.db")
        self.router = Router(backend=self.be, dlq=self.dlq)
        # Replace the global default backend with our harness backend
        # so facade.event.publish(...) lands here.
        self._saved_eventbus = _backends.get("eventbus")
        _backends["eventbus"] = self.be

    def close(self) -> None:
        if self._saved_eventbus is not None:
            _backends["eventbus"] = self._saved_eventbus

    def subscribe(self, pattern: str) -> list[BusEnvelope]:
        received: list[BusEnvelope] = []
        self.be.subscribe(pattern, lambda env: received.append(env))
        return received


@pytest.fixture
def harness(tmp_path) -> _BusHarness:
    h = _BusHarness(tmp_path)
    yield h
    h.close()


def test_omo_audit_completed_reaches_cockpit(harness: _BusHarness) -> None:
    """omo:audit:completed → cockpit (or any subscriber of omo:*)."""
    from bus_foundation.facade import event as bus_event

    cockpit_inbox = harness.subscribe("omo:*")
    bus_event.publish(
        topic="omo:audit:completed",
        payload={"audit_score": 95.0, "audit_grade": "A"},
        source_uri="bos://capability/omo/governance",
    )
    assert _wait_for(lambda: len(cockpit_inbox) >= 1)
    env = cockpit_inbox[0]
    assert env.topic == "omo:audit:completed"
    assert env.payload["audit_score"] == 95.0


def test_kairon_source_ingested_reaches_omo(harness: _BusHarness) -> None:
    """kairon:source:ingested → omo (governance) audit pipeline."""
    from bus_foundation.facade import event as bus_event

    omo_inbox = harness.subscribe("kairon:*")
    bus_event.publish(
        topic="kairon:source:ingested",
        payload={"source": "arxiv", "records": 42},
        source_uri="bos://capability/pipeline/kairon",
    )
    assert _wait_for(lambda: len(omo_inbox) >= 1)
    assert omo_inbox[0].topic == "kairon:source:ingested"


def test_metaos_node_completed_reaches_cockpit(harness: _BusHarness) -> None:
    """metaos:node:completed → cockpit dashboard."""
    from bus_foundation.facade import event as bus_event

    cockpit_inbox = harness.subscribe("metaos:*")
    bus_event.publish(
        topic="metaos:node:completed",
        payload={"workflow_id": "wf-1", "node_id": "n1"},
        source_uri="bos://capability/workflow/metaos",
    )
    assert _wait_for(lambda: len(cockpit_inbox) >= 1)


def test_aetherforge_hatcher_reaches_omo(harness: _BusHarness) -> None:
    """swarm:hatcher:initialized → omo_mesh_event_handler."""
    from bus_foundation.facade import event as bus_event

    omo_inbox = harness.subscribe("swarm:*")
    bus_event.publish(
        topic="swarm:hatcher:initialized",
        payload={"hatcher_id": "h1"},
        source_uri="bos://capability/aetherforge/hatcher",
    )
    assert _wait_for(lambda: len(omo_inbox) >= 1)


def test_runtime_cron_fired_reaches_cockpit(harness: _BusHarness) -> None:
    """runtime:cron:fired → cockpit cron dashboard."""
    from bus_foundation.facade import event as bus_event

    cockpit_inbox = harness.subscribe("runtime:cron:*")
    bus_event.publish(
        topic="runtime:cron:fired",
        payload={"job_id": "j1", "status": "ok"},
        source_uri="bos://capability/runtime/cron",
    )
    assert _wait_for(lambda: len(cockpit_inbox) >= 1)


def test_full_round_trip_5_publishers_one_subscriber(harness: _BusHarness) -> None:
    """All 5 consumer projects publish simultaneously; one observer sees all."""
    from bus_foundation.facade import event as bus_event

    observer = harness.subscribe("*")

    # 5 publishers, each in a different namespace
    topics = [
        "omo:audit:completed",
        "kairon:source:ingested",
        "metaos:node:completed",
        "swarm:hatcher:initialized",
        "runtime:cron:fired",
    ]
    for t in topics:
        bus_event.publish(topic=t, payload={}, source_uri="bos://test")

    assert _wait_for(lambda: len(observer) >= 5)
    seen = {env.topic for env in observer}
    assert seen == set(topics)


def test_unavailable_backend_routes_to_dlq(tmp_path) -> None:
    """When the global eventbus reports unavailable, Router writes to DLQ."""

    class _Broken:
        name = "eventbus"

        def is_available(self) -> bool:
            return False

        def publish(self, env: BusEnvelope) -> str:
            raise RuntimeError("unreachable")

    dlq = DLQ(db_path=tmp_path / "dlq2.db")
    router = Router(backend=_Broken(), dlq=dlq)
    env = BusEnvelope(
        type="system:bus:health",
        source="e2e-cross",
        payload={"k": "v"},
    )
    eid = router.publish(env)
    assert eid == env.id
    entries = dlq.list_all()
    assert any(e["event_id"] == env.id for e in entries)
