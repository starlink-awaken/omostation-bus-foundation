"""Tests for ZeroMQBackend — cross-process pub/sub via ZMQ inproc transport.

Uses ``inproc://`` addresses so no real TCP ports are needed.

These tests require the optional ``[zmq]`` extra. They are auto-skipped
when ``pyzmq`` is not installed (e.g. in CI runners that don't ship the
zmq extra).
"""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

# Skip the entire module if pyzmq is not installed. pytest.importorskip
# raises a Skip at collection time, so the rest of the file can assume
# `zmq` is importable. We use the standard `import zmq` so type checkers
# (pyright / mypy) can resolve the type. The skip annotation is a
# docstring + a `pytest.importorskip` invocation to actually trigger
# the skip behavior.
try:
    import zmq as _zmq_module  # type: ignore[import-not-found]
except ImportError:
    pytest.importorskip("zmq", reason="pyzmq not installed; install bus-foundation[zmq]")

import zmq  # type: ignore[import-untyped,import-not-found]

from bus_foundation.backends.base import BusBackend
from bus_foundation.backends.zmq import ZeroMQBackend
from bus_foundation.envelope import BusEnvelope, EventType


@pytest.fixture
def ctx() -> zmq.Context:
    return zmq.Context()


def test_implements_protocol(ctx: zmq.Context) -> None:
    b = ZeroMQBackend(port=0, context=ctx, peers=[])
    assert isinstance(b, BusBackend)
    b.close()


def test_is_available(ctx: zmq.Context) -> None:
    b = ZeroMQBackend(port=0, context=ctx, peers=[])
    assert b.is_available() is True
    b.close()


def test_publish_and_subscribe_inproc(ctx: zmq.Context) -> None:
    """Publisher and subscriber connected via inproc, message delivered."""
    pub = ZeroMQBackend(port=0, context=ctx, peers=[])
    addr = f"inproc://pub-{id(pub)}"
    pub.bind_pub(addr)

    received: list[BusEnvelope] = []
    ev = threading.Event()

    sub = ZeroMQBackend(port=0, context=ctx, peers=[addr])
    sub.subscribe("*", lambda e: (received.append(e), ev.set()))
    time.sleep(0.15)  # let SUB connect + listener start

    env = BusEnvelope(type=EventType.PIPELINE_COMPLETED, source="ut", payload={"x": 1})
    pub.publish(env)
    assert ev.wait(timeout=3), "subscriber never received the message"
    assert len(received) == 1
    assert received[0].payload == {"x": 1}
    sub.close()
    pub.close()


def test_subscribe_pattern_filter(ctx: zmq.Context) -> None:
    """Only matching patterns fire the callback."""
    pub = ZeroMQBackend(port=0, context=ctx, peers=[])
    addr = f"inproc://filter-{id(pub)}"
    pub.bind_pub(addr)

    received: list[BusEnvelope] = []
    ev = threading.Event()

    sub = ZeroMQBackend(port=0, context=ctx, peers=[addr])
    sub.subscribe("foo:*", lambda e: (received.append(e), ev.set()))
    time.sleep(0.15)

    pub.publish(BusEnvelope(type="bar:event", source="ut"))
    time.sleep(0.2)
    assert len(received) == 0, "non-matching should not dispatch"

    pub.publish(BusEnvelope(type="foo:event", source="ut"))
    assert ev.wait(timeout=3), "matching event not received"
    assert len(received) == 1
    assert received[0].type == "foo:event"
    sub.close()
    pub.close()


def test_unsubscribe_removes_callback(ctx: zmq.Context) -> None:
    b = ZeroMQBackend(port=0, context=ctx, peers=[])
    sub_id = b.subscribe("*", lambda e: None)
    assert b.unsubscribe(sub_id) is True
    assert b.unsubscribe(sub_id) is False
    b.close()


def test_publish_returns_envelope_id(ctx: zmq.Context) -> None:
    b = ZeroMQBackend(port=0, context=ctx, peers=[])
    env = BusEnvelope(type="t", source="ut")
    result = b.publish(env)
    assert result == env.id
    b.close()
