"""Tests for facade modules (control, data, event, __init__).

These complement the existing test_facade.py which tests the top-level
bus_foundation publish/subscribe/schedule API.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from bus_foundation.backends.control_plane import ControlPlaneBackend

# ── Control facade ─────────────────────────────────────────────────


def test_control_submit_task_produces_id(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from bus_foundation.facade.control import submit_task

    cp = ControlPlaneBackend(db_path=tmp_path / "ctl.db")
    monkeypatch.setattr("bus_foundation.facade.control._control_backend", cp)
    task_id = submit_task(topic="test.work", payload={"key": "val"})
    assert task_id and isinstance(task_id, str)
    pending = cp.claim_pending()
    assert any(p["id"] == task_id for p in pending)
    cp.close()


def test_control_submit_and_ack(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from bus_foundation.facade.control import ack, submit_task

    cp = ControlPlaneBackend(db_path=tmp_path / "ctl2.db")
    monkeypatch.setattr("bus_foundation.facade.control._control_backend", cp)
    task_id = submit_task(topic="test.work", payload={"x": 1})
    ack(task_id)
    pending = cp.claim_pending()
    assert all(p["id"] != task_id for p in pending)
    cp.close()


def test_control_submit_and_nack(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from bus_foundation.facade.control import nack, submit_task

    cp = ControlPlaneBackend(db_path=tmp_path / "ctl3.db")
    monkeypatch.setattr("bus_foundation.facade.control._control_backend", cp)
    task_id = submit_task(topic="test.work", payload={"x": 1})
    nack(task_id, "oops")
    pending = cp.claim_pending()
    assert any(p["id"] == task_id for p in pending)
    cp.close()


def test_control_schedule_returns_string(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from bus_foundation.facade.control import schedule

    cp = ControlPlaneBackend(db_path=tmp_path / "ctl4.db")
    monkeypatch.setattr("bus_foundation.facade.control._control_backend", cp)
    result = schedule(topic="test.work", payload={}, schedule_at="2026-12-01T00:00:00Z")
    assert result == "scheduled"
    cp.close()


# ── Data facade ────────────────────────────────────────────────────


def test_data_emit_calls_publish(monkeypatch: pytest.MonkeyPatch) -> None:
    from bus_foundation.facade.data import emit

    calls: list[str] = []

    class FakeBackend:
        name = "data_test"

        def publish(self, envelope: object) -> str:
            calls.append(envelope.topic if hasattr(envelope, "topic") else "?")
            return "ok"

        def is_available(self) -> bool:
            return True

        def subscribe(self, pattern: str, callback: object) -> str:
            return "sub"

        def unsubscribe(self, sub_id: str) -> bool:
            return True

    monkeypatch.setattr("bus_foundation.facade.data._data_backend", FakeBackend())
    emit(topic="sensor.temp", payload={"value": 36.5})
    assert calls == ["sensor.temp"]


def test_data_subscribe_returns_id(monkeypatch: pytest.MonkeyPatch) -> None:
    from bus_foundation.backends.data_plane import DataPlaneBackend
    from bus_foundation.facade.data import subscribe

    dp = DataPlaneBackend()
    monkeypatch.setattr("bus_foundation.facade.data._data_backend", dp)
    sub_id = subscribe("*", lambda e: None)
    assert sub_id.startswith("data-sub-")
    dp.unsubscribe(sub_id)


# ── Event facade ───────────────────────────────────────────────────


def test_event_publish_dispatches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify event facade publish calls the module-level publish."""
    from bus_foundation.facade.event import publish as facade_publish

    calls: list[object] = []

    def fake_publish(env: object) -> str:
        calls.append(env)
        return "ok"

    monkeypatch.setattr("bus_foundation.facade.event._bus_publish", fake_publish)
    facade_publish(topic="test.event", payload={"n": 42})
    assert len(calls) == 1


def test_event_subscribe_is_decorator(monkeypatch: pytest.MonkeyPatch) -> None:
    """event facade subscribe wraps the bus.subscribe and can be used as @decorator."""
    from bus_foundation.facade.event import subscribe as facade_subscribe

    captured: list[str] = []

    def fake_subscribe(pattern: str) -> object:
        captured.append(pattern)

        def inner(fn: object) -> object:
            return fn

        return inner

    monkeypatch.setattr("bus_foundation.facade.event._bus_subscribe", fake_subscribe)

    @facade_subscribe("test:*")
    def handler() -> None:
        pass

    assert callable(handler)
    assert captured == ["test:*"]


def test_event_outbox_emit_writes_to_db(monkeypatch: pytest.MonkeyPatch) -> None:
    from bus_foundation.facade.event import outbox_emit

    executed: list[str] = []

    class FakeSession:
        def execute(self, sql: str, params: dict) -> None:
            executed.append(sql)
            executed.append(params["id"])

    outbox_emit(db_session=FakeSession(), topic="test.event", payload={"k": "v"})
    assert len(executed) == 2
    assert "INSERT INTO _bus_outbox" in executed[0]
    assert executed[1] and isinstance(executed[1], str)


# ── facade __init__ exports ────────────────────────────────────────


def test_facade_init_exports() -> None:
    from bus_foundation.facade import __all__ as facade_exports

    expected = {"emit_data", "publish", "subscribe", "submit_task", "schedule", "ack", "nack"}
    assert set(facade_exports) == expected
