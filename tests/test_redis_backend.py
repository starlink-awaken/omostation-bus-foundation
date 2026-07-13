"""Tests for RedisBackend (mocked Redis connection).

These tests mock _get_conn() to avoid requiring a running Redis server.
Subscribe tests are limited since subscribe() spawns a thread.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from bus_foundation.backends.redis import RedisBackend
from bus_foundation.envelope import BusEnvelope, EventType


@pytest.fixture
def mock_conn() -> MagicMock:
    conn = MagicMock()
    conn.ping.return_value = True
    conn.publish.return_value = 1
    return conn


@pytest.fixture
def rb(monkeypatch: pytest.MonkeyPatch, mock_conn: MagicMock) -> RedisBackend:
    def _fake_get_conn(self: object) -> MagicMock:
        return mock_conn

    def _fake_lazy_import(self: object) -> MagicMock:
        return MagicMock()

    monkeypatch.setattr(RedisBackend, "_get_conn", _fake_get_conn)
    monkeypatch.setattr(RedisBackend, "_lazy_import", _fake_lazy_import)
    return RedisBackend()


# ── BusBackend protocol ────────────────────────────────────────────


def test_implements_protocol(rb: RedisBackend) -> None:
    from bus_foundation.backends.base import BusBackend

    assert isinstance(rb, BusBackend)


# ── is_available ───────────────────────────────────────────────────


def test_is_available_true(rb: RedisBackend, mock_conn: MagicMock) -> None:
    assert rb.is_available() is True
    mock_conn.ping.assert_called_once()


def test_is_available_false(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = MagicMock()
    conn.ping.side_effect = ConnectionError("no server")

    def _fake_gc(self: object) -> MagicMock:
        return conn

    monkeypatch.setattr(RedisBackend, "_get_conn", _fake_gc)
    rb = RedisBackend()
    assert rb.is_available() is False


# ── publish ────────────────────────────────────────────────────────


def test_publish_success(rb: RedisBackend, mock_conn: MagicMock) -> None:
    env = BusEnvelope(type=EventType.PIPELINE_COMPLETED, source="ut", payload={"x": 1})
    result = rb.publish(env)
    assert result == env.id
    mock_conn.publish.assert_called_once()


def test_publish_raises_on_failure(rb: RedisBackend, mock_conn: MagicMock) -> None:
    mock_conn.publish.side_effect = RuntimeError("redis down")
    env = BusEnvelope(type="test:event", source="ut")
    with pytest.raises(RuntimeError, match="redis down"):
        rb.publish(env)


# ── subscribe / unsubscribe (light) ───────────────────────────────


def test_unsubscribe_unknown(rb: RedisBackend) -> None:
    assert rb.unsubscribe("nonexistent") is False


def test_close_no_subscribers(rb: RedisBackend) -> None:
    rb.close()  # should not raise


# ── lazy_import ────────────────────────────────────────────────────


def test_lazy_import_mocked(rb: RedisBackend) -> None:
    """_lazy_import returns a mock when mocked."""
    mod = rb._lazy_import()
    assert mod is not None
