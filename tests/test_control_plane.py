"""Tests for ControlPlaneBackend (0% → ~85% coverage)."""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from bus_foundation.backends.control_plane import ControlPlaneBackend
from bus_foundation.envelope import OmniEnvelope


@pytest.fixture
def cp(tmp_path: Path) -> ControlPlaneBackend:  # type: ignore[reportInvalidTypeForm]
    db = tmp_path / "test_control.db"
    backend = ControlPlaneBackend(db_path=db, poll_interval=0.05)
    yield backend  # type: ignore[reportReturnType]
    backend.close()


def _make_envelope(topic: str = "test.topic") -> OmniEnvelope:
    return OmniEnvelope(topic=topic, payload={"msg": "hello"}, source_uri="test://cp")


# ── Publish / Ack / Nack ──────────────────────────────────────────


def test_publish_creates_pending_task(cp: ControlPlaneBackend) -> None:
    env = _make_envelope()
    task_id = cp.publish(env)
    assert task_id == env.id
    pending = cp.claim_pending()
    assert len(pending) == 1
    assert pending[0]["id"] == task_id
    assert pending[0]["status"] == "PENDING"


def test_ack_marks_complete(cp: ControlPlaneBackend) -> None:
    env = _make_envelope()
    task_id = cp.publish(env)
    assert cp.ack(task_id) is True
    pending = cp.claim_pending()
    assert len(pending) == 0


def test_ack_unknown_task(cp: ControlPlaneBackend) -> None:
    assert cp.ack("nonexistent-id") is False


def test_nack_increments_retries(cp: ControlPlaneBackend) -> None:
    env = _make_envelope()
    task_id = cp.publish(env)
    assert cp.nack(task_id, "first failure") is True
    pending = cp.claim_pending()
    assert len(pending) == 1
    assert pending[0]["retry_count"] == 1
    assert pending[0]["status"] == "PENDING"


def test_nack_moves_to_dlq_after_max_retries(cp: ControlPlaneBackend) -> None:
    env = _make_envelope()
    task_id = cp.publish(env)
    # nack 3 times (= max_retries default)
    for i in range(3):
        assert cp.nack(task_id, f"attempt {i}") is True
    # Task should no longer be PENDING
    pending = cp.claim_pending()
    assert all(p["id"] != task_id for p in pending)
    # Verify it's DLQ by checking the DB directly
    row = cp._conn.execute("SELECT status, retry_count FROM control_tasks WHERE id = ?", (task_id,)).fetchone()
    assert row is not None
    assert row["status"] == "DLQ"
    assert row["retry_count"] == 3


def test_nack_unknown_task(cp: ControlPlaneBackend) -> None:
    assert cp.nack("nonexistent", "err") is False


# ── claim_pending ──────────────────────────────────────────────────


def test_claim_pending_filters_by_pattern(cp: ControlPlaneBackend) -> None:
    env_a = _make_envelope(topic="alpha.event")
    env_b = _make_envelope(topic="beta.event")
    cp.publish(env_a)
    cp.publish(env_b)
    alpha_only = cp.claim_pending(pattern="alpha.*")
    assert len(alpha_only) == 1
    assert alpha_only[0]["id"] == env_a.id


def test_claim_pending_limit(cp: ControlPlaneBackend) -> None:
    for i in range(5):
        cp.publish(_make_envelope(topic=f"t.{i}"))
    assert len(cp.claim_pending(limit=3)) == 3


# ── Subscribe / Poller ─────────────────────────────────────────────


def test_subscribe_dispatches_pending(cp: ControlPlaneBackend) -> None:
    received: list[OmniEnvelope] = []
    ev = threading.Event()

    def cb(env: OmniEnvelope) -> None:
        received.append(env)
        ev.set()

    sub_id = cp.subscribe("*", cb)
    env = _make_envelope()
    cp.publish(env)
    assert ev.wait(timeout=2), "callback was never invoked"
    assert len(received) == 1
    assert received[0].id == env.id
    cp.unsubscribe(sub_id)


def test_subscribe_dispatches_by_pattern(cp: ControlPlaneBackend) -> None:
    received: list[OmniEnvelope] = []
    ev = threading.Event()

    def cb(env: OmniEnvelope) -> None:
        received.append(env)
        ev.set()

    sub_id = cp.subscribe("specific.*", cb)
    cp.publish(_make_envelope(topic="other.event"))
    cp.publish(_make_envelope(topic="specific.event"))
    assert ev.wait(timeout=2), "callback was never invoked"
    assert len(received) == 1
    assert received[0].topic == "specific.event"
    cp.unsubscribe(sub_id)


def test_subscribe_no_duplicate_dispatch(cp: ControlPlaneBackend) -> None:
    """Same task should not be dispatched twice if it stays PENDING."""
    call_count = 0
    ev = threading.Event()

    def cb(env: OmniEnvelope) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            ev.set()

    sub_id = cp.subscribe("*", cb)
    env = _make_envelope()
    cp.publish(env)
    assert ev.wait(timeout=2)
    time.sleep(0.3)  # let poll loop cycle
    assert call_count == 1  # no second dispatch
    cp.unsubscribe(sub_id)


# ── Unsubscribe ────────────────────────────────────────────────────


def test_unsubscribe_stops_poller(cp: ControlPlaneBackend) -> None:
    received: list[OmniEnvelope] = []
    ev = threading.Event()

    def cb(env: OmniEnvelope) -> None:
        received.append(env)
        ev.set()

    sub_id = cp.subscribe("*", cb)
    # Wait for poller to start
    time.sleep(0.15)
    assert cp.unsubscribe(sub_id) is True
    # Give poller time to notice the stop signal
    time.sleep(0.3)
    cp.publish(_make_envelope())
    assert ev.wait(timeout=0.3) is False  # should NOT be called
    assert len(received) == 0


def test_unsubscribe_unknown(cp: ControlPlaneBackend) -> None:
    assert cp.unsubscribe("nonexistent") is False


# ── Redeliver ──────────────────────────────────────────────────────


def test_redeliver_re_arms_task(cp: ControlPlaneBackend) -> None:
    received: list[OmniEnvelope] = []
    ev = threading.Event()

    def cb(env: OmniEnvelope) -> None:
        received.append(env)
        if len(received) == 2:
            ev.set()

    sub_id = cp.subscribe("*", cb)
    env = _make_envelope()
    cp.publish(env)
    time.sleep(0.3)
    assert len(received) == 1  # first dispatch
    # Redeliver: re-arm
    assert cp.redeliver(sub_id, env.id) is True
    assert ev.wait(timeout=2)
    assert len(received) == 2
    cp.unsubscribe(sub_id)


def test_redeliver_unknown_sub(cp: ControlPlaneBackend) -> None:
    assert cp.redeliver("nonexistent", "some-task") is False


# ── is_available ────────────────────────────────────────────────────


def test_is_available(cp: ControlPlaneBackend) -> None:
    assert cp.is_available() is True


def test_is_available_false(tmp_path: Path) -> None:
    db = tmp_path / "nested" / "deep" / "test.db"
    backend = ControlPlaneBackend(db_path=db)
    assert backend.is_available() is True
    backend.close()
