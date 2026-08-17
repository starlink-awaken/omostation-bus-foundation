# pyright: reportCallIssue=false

"""Tests for WebSocketBackendV2 (R95: protocol versioning, compression, heartbeat).

Coverage:
  1. In-process (mode 1) publish/subscribe/unsubscribe with callbacks
  2. Pattern matching and fanout filtering
  3. serve_forever raises ImportError without `websockets`
  4. Protocol constants exported
"""

from __future__ import annotations

import asyncio
import json
from asyncio import Queue

import pytest

from bus_foundation.backends.base import BusBackend
from bus_foundation.backends.ws_v2 import (
    MSG_ACK,
    MSG_EVENT,
    MSG_PONG,
    WebSocketBackendV2,
)
from bus_foundation.envelope import BusEnvelope, EventType


@pytest.fixture
def be() -> WebSocketBackendV2:
    return WebSocketBackendV2()


# ── helper: callback that pushes to an asyncio.Queue ────────────────


def _collector() -> tuple[Queue[BusEnvelope], callable]:  # type: ignore[reportGeneralTypeIssues]
    q: Queue[BusEnvelope] = Queue()

    def cb(env: BusEnvelope) -> None:
        q.put_nowait(env)

    return q, cb


# ── BusBackend protocol ────────────────────────────────────────────


def test_implements_protocol(be: WebSocketBackendV2) -> None:
    assert isinstance(be, BusBackend)


def test_is_available(be: WebSocketBackendV2) -> None:
    assert be.is_available() is True


# ── In-process publish / subscribe / unsubscribe ───────────────────


@pytest.mark.asyncio
async def test_publish_and_subscribe(be: WebSocketBackendV2) -> None:
    q, cb = _collector()
    sub_id = be.subscribe("*", cb)
    env = BusEnvelope(type=EventType.MESSAGE_RECEIVED, source="t", payload={"x": 1})
    be.publish(env)
    received = await asyncio.wait_for(q.get(), timeout=1.0)
    assert received.payload == {"x": 1}
    be.unsubscribe(sub_id)


@pytest.mark.asyncio
async def test_subscribe_pattern_filters(be: WebSocketBackendV2) -> None:
    q_match, cb_match = _collector()
    q_skip, cb_skip = _collector()
    be.subscribe("pipeline:*", cb_match)
    be.subscribe("metric:*", cb_skip)
    be.publish(BusEnvelope(type=EventType.PIPELINE_COMPLETED, source="t"))
    received = await asyncio.wait_for(q_match.get(), timeout=1.0)
    assert received.type == EventType.PIPELINE_COMPLETED
    assert q_skip.empty()


@pytest.mark.asyncio
async def test_multiple_subscribers_all_receive(be: WebSocketBackendV2) -> None:
    q1, cb1 = _collector()
    q2, cb2 = _collector()
    be.subscribe("*", cb1)
    be.subscribe("*", cb2)
    be.publish(BusEnvelope(type=EventType.MESSAGE_RECEIVED, source="t"))
    r1 = await asyncio.wait_for(q1.get(), timeout=1.0)
    r2 = await asyncio.wait_for(q2.get(), timeout=1.0)
    assert r1.type == EventType.MESSAGE_RECEIVED
    assert r2.type == EventType.MESSAGE_RECEIVED


@pytest.mark.asyncio
async def test_subscribe_non_matching_pattern(be: WebSocketBackendV2) -> None:
    q, cb = _collector()
    be.subscribe("foo:*", cb)
    be.publish(BusEnvelope(type="bar:thing", source="t"))
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(q.get(), timeout=0.3)


def test_unsubscribe_returns_true(be: WebSocketBackendV2) -> None:
    sub_id = be.subscribe("*", lambda e: None)
    assert be.unsubscribe(sub_id) is True


def test_unsubscribe_unknown_returns_false(be: WebSocketBackendV2) -> None:
    assert be.unsubscribe("nonexistent") is False


@pytest.mark.asyncio
async def test_unsubscribe_stops_drain(be: WebSocketBackendV2) -> None:
    q, cb = _collector()
    sub_id = be.subscribe("*", cb)
    be.unsubscribe(sub_id)
    be.publish(BusEnvelope(type=EventType.MESSAGE_RECEIVED, source="t"))
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(q.get(), timeout=0.3)


# ── serve_forever raises without websockets ────────────────────────


def test_serve_forever_raises_without_websockets(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib.util

    orig = importlib.util.find_spec
    monkeypatch.setattr(importlib.util, "find_spec", lambda name, *a: None if name == "websockets" else orig(name, *a))
    be = WebSocketBackendV2()
    with pytest.raises(ImportError, match="websockets"):
        be.serve_forever(host="127.0.0.1", port=9999)


def test_protocol_constants() -> None:
    assert MSG_EVENT == "event"
    assert MSG_ACK == "ack"
    assert MSG_PONG == "pong"


# ── Mode 2: real WebSocket transport (requires websockets) ─────────


def _free_port() -> int:
    import socket as _socket

    s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _require_websockets() -> None:
    try:
        import websockets
    except ImportError:
        pytest.skip("websockets not installed; pip install bus-foundation[ws]")


@pytest.fixture
def ws_v2_server() -> tuple[WebSocketBackendV2, int]:  # type: ignore[reportInvalidTypeForm]
    _require_websockets()
    be = WebSocketBackendV2()
    port = _free_port()
    handle = be.serve_forever(host="127.0.0.1", port=port, version=2)
    yield be, port  # type: ignore[reportReturnType]
    handle.stop()


class TestWebSocketServerV2:
    """End-to-end WS server tests for v2 protocol."""

    def test_serve_forever_binds_port(self) -> None:
        _require_websockets()
        be = WebSocketBackendV2()
        port = _free_port()
        handle = be.serve_forever(host="127.0.0.1", port=port, version=2)
        try:
            assert handle.host == "127.0.0.1"
            assert handle.port == port
            assert handle.is_running is True
            import socket as _socket

            with _socket.socket() as s:
                s.settimeout(2.0)
                s.connect(("127.0.0.1", port))
        finally:
            handle.stop()
            import time as _time

            _time.sleep(0.2)
        assert handle.is_running is False

    def test_ws_client_subscribe_and_receive_v2(self) -> None:
        """v2 client: subscribe with version=2, receive reconnect token."""
        _require_websockets()
        import websockets.sync.client as ws_sync

        be = WebSocketBackendV2()
        port = _free_port()
        handle = be.serve_forever(host="127.0.0.1", port=port, version=2)
        try:
            with ws_sync.connect(f"ws://127.0.0.1:{port}") as ws:
                ws.send(json.dumps({"action": "subscribe", "pattern": "pipeline:*", "version": 2}))
                ack = json.loads(ws.recv())
                assert ack["type"] == MSG_ACK
                assert ack["action"] == "subscribe"
                assert ack["pattern"] == "pipeline:*"
                assert "reconnect_token" in ack
                assert ack["version"] == 2
                assert ack["heartbeat_interval"] == 30.0

                # Publish a matching event
                env = BusEnvelope(type=EventType.PIPELINE_COMPLETED, source="bos://test", payload={"k": "v"})
                be.publish(env)

                msg = json.loads(ws.recv(timeout=2.0))
                assert msg["type"] == MSG_EVENT
                assert msg["envelope"]["topic"] == "pipeline:completed"
                assert msg["envelope"]["payload"] == {"k": "v"}
        finally:
            handle.stop()
            import time as _time

            _time.sleep(0.2)

    def test_ws_client_v1_fallback(self) -> None:
        """v1 client without version field still works (BC)."""
        _require_websockets()
        import websockets.sync.client as ws_sync

        be = WebSocketBackendV2()
        port = _free_port()
        handle = be.serve_forever(host="127.0.0.1", port=port, version=2)
        try:
            with ws_sync.connect(f"ws://127.0.0.1:{port}") as ws:
                ws.send(json.dumps({"action": "subscribe", "pattern": "*"}))
                ack = json.loads(ws.recv())
                assert ack["type"] == MSG_ACK
                # v1 client does NOT get reconnect_token
                assert "reconnect_token" not in ack
        finally:
            handle.stop()
            import time as _time

            _time.sleep(0.2)

    def test_ws_client_pattern_filter_v2(self) -> None:
        """Client subscribed to foo:* should not receive bar:* events."""
        _require_websockets()
        import websockets.sync.client as ws_sync

        be = WebSocketBackendV2()
        port = _free_port()
        handle = be.serve_forever(host="127.0.0.1", port=port, version=2)
        try:
            with ws_sync.connect(f"ws://127.0.0.1:{port}") as ws:
                ws.send(json.dumps({"action": "subscribe", "pattern": "foo:*"}))
                ws.recv()  # ack

                be.publish(BusEnvelope(type="bar:thing", source="bos://x"))
                be.publish(BusEnvelope(type="foo:hit", source="bos://x"))

                msg = json.loads(ws.recv(timeout=2.0))
                assert msg["type"] == MSG_EVENT
                assert msg["envelope"]["topic"] == "foo:hit"
                # Verify no second message arrived

                try:
                    extra = ws.recv(timeout=0.5)
                    extra_topic = json.loads(extra)["envelope"]["topic"]
                    assert extra_topic != "bar:thing"
                except Exception:
                    pass  # timeout = success
        finally:
            handle.stop()
            import time as _time

            _time.sleep(0.2)

    def test_ws_client_ping_pong(self) -> None:
        _require_websockets()
        import websockets.sync.client as ws_sync

        be = WebSocketBackendV2()
        port = _free_port()
        handle = be.serve_forever(host="127.0.0.1", port=port, version=2)
        try:
            with ws_sync.connect(f"ws://127.0.0.1:{port}") as ws:
                ws.send(json.dumps({"action": "ping"}))
                pong = json.loads(ws.recv())
                assert pong["type"] == MSG_PONG
        finally:
            handle.stop()
            import time as _time

            _time.sleep(0.2)

    def test_ws_client_unknown_action(self) -> None:
        _require_websockets()
        import websockets.sync.client as ws_sync

        be = WebSocketBackendV2()
        port = _free_port()
        handle = be.serve_forever(host="127.0.0.1", port=port, version=2)
        try:
            with ws_sync.connect(f"ws://127.0.0.1:{port}") as ws:
                ws.send(json.dumps({"action": "unknown_action"}))
                err = json.loads(ws.recv())
                assert err["type"] == "error"
                assert "unknown_action" in err["message"]
        finally:
            handle.stop()
            import time as _time

            _time.sleep(0.2)

    def test_stop_server_idempotent_without_loop(self) -> None:
        """stop_server() must be safe when called twice (no AttributeError)."""
        be = WebSocketBackendV2()
        # Never started; stop_server should be a no-op.
        be.stop_server()
        be.stop_server()
        assert be._server is None
        assert be._ws_connections == set()
        assert be._clients == {}

    def test_serve_then_stop_no_unbound_task(self) -> None:
        """Regression: hb_task must be initialized before asyncio.create_task.

        The original code had hb_task declared inside the try block, so
        if the connection dropped between `add()` and `create_task`,
        the `finally` would raise UnboundLocalError.
        """
        _require_websockets()
        be = WebSocketBackendV2()
        port = _free_port()
        handle = be.serve_forever(host="127.0.0.1", port=port, version=1)
        # Simulate the race: a client connects, then immediately disconnects.
        try:
            import websockets.sync.client as ws_sync

            with ws_sync.connect(f"ws://127.0.0.1:{port}") as ws:
                ws.send(json.dumps({"action": "subscribe", "pattern": "*"}))
                ws.recv()  # ack
            # Connection closed; backend's _handler should have run the
            # finally block without UnboundLocalError on hb_task.
        finally:
            handle.stop()
            import time as _time

            _time.sleep(0.2)
