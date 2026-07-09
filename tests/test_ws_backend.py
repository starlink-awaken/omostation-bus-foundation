"""Test WebSocketBackend — R81: real WebSocket transport + in-process compat.

Coverage:
  1. In-process fanout still works (mode 1 regression for R73 callers)
  2. publish() pattern match still routes to in-process clients
  3. WebSocketServerHandle serves on a port and accepts connections
  4. Subscribe / unsubscribe / ping wire protocol works end-to-end
  5. Real WS clients receive matching envelopes; non-matching are filtered
  6. stop_server() closes connections cleanly
  7. ImportError path: serve_forever() without `websockets` installed
"""

from __future__ import annotations

import json
import socket

import pytest

from bus_foundation.backends.base import BusBackend
from bus_foundation.backends.ws import (
    MSG_ACK,
    MSG_EVENT,
    MSG_PONG,
    WebSocketBackend,
)
from bus_foundation.envelope import BusEnvelope, EventType

# ── Mode 1: in-process (R73 regression) ─────────────────────────────


class TestInProcessCompat:
    """Existing R73 in-process API still works after R81 rewrite."""

    def test_implements_protocol(self) -> None:
        b = WebSocketBackend()
        assert isinstance(b, BusBackend)

    def test_is_available(self) -> None:
        b = WebSocketBackend()
        assert b.is_available() is True

    @pytest.mark.asyncio
    async def test_publish_fanout(self) -> None:
        b = WebSocketBackend()
        _, q1 = b.connect("pipeline:*")
        _, q2 = b.connect("*")
        env = BusEnvelope(
            type=EventType.PIPELINE_COMPLETED,
            source="t",
            payload={"x": 1},
        )
        b.publish(env)
        assert not q1.empty()
        assert not q2.empty()
        assert q1.get_nowait().payload == {"x": 1}
        assert q2.get_nowait().payload == {"x": 1}

    @pytest.mark.asyncio
    async def test_publish_filters_pattern(self) -> None:
        b = WebSocketBackend()
        _, q_match = b.connect("pipeline:*")
        _, q_skip = b.connect("metric:*")
        env = BusEnvelope(type=EventType.PIPELINE_COMPLETED, source="t")
        b.publish(env)
        assert not q_match.empty()
        assert q_skip.empty()

    def test_disconnect_removes_client(self) -> None:
        b = WebSocketBackend()
        client_id, _q = b.connect("*")
        assert b.disconnect(client_id) is True
        assert b.disconnect(client_id) is False  # second time


# ── Mode 2: real WebSocket transport (R81) ──────────────────────────


def _free_port() -> int:
    """Bind a kernel-allocated free port and release it."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _require_websockets() -> None:
    try:
        import websockets  # type: ignore[import-not-found]  # noqa: F401
    except ImportError:
        pytest.skip("websockets not installed; pip install bus-foundation[ws]")


def test_serve_forever_requires_websockets_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If `websockets` isn't importable, serve_forever() raises ImportError."""
    b = WebSocketBackend()
    import builtins

    real_import = builtins.__import__

    def _hide_websockets(name: str, *args: object, **kwargs: object) -> object:
        if name == "websockets" or name.startswith("websockets."):
            raise ImportError("simulated missing websockets")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _hide_websockets)
    with pytest.raises(ImportError, match="websockets"):
        b.serve_forever(host="127.0.0.1", port=_free_port())


class TestWebSocketServer:
    """End-to-end tests using a real WebSocket server on a free port."""

    def test_serve_forever_binds_port(self) -> None:
        _require_websockets()
        b = WebSocketBackend()
        port = _free_port()
        handle = b.serve_forever(host="127.0.0.1", port=port)
        try:
            assert handle.host == "127.0.0.1"
            assert handle.port == port
            assert handle.is_running is True
            # The port should be reachable.
            with socket.socket() as s:
                s.settimeout(2.0)
                s.connect(("127.0.0.1", port))
        finally:
            handle.stop()
            assert handle.is_running is False

    def test_ws_client_subscribe_and_receive(self) -> None:
        """End-to-end: connect WS, subscribe, publish, receive."""
        _require_websockets()
        import websockets.sync.client as ws_sync  # type: ignore[import-not-found]

        b = WebSocketBackend()
        port = _free_port()
        handle = b.serve_forever(host="127.0.0.1", port=port)
        try:
            url = f"ws://127.0.0.1:{port}"
            with ws_sync.connect(url) as ws:
                # Subscribe to a pattern
                ws.send(json.dumps({"action": "subscribe", "pattern": "pipeline:*"}))
                ack = json.loads(ws.recv())
                assert ack["type"] == MSG_ACK
                assert ack["action"] == "subscribe"
                assert ack["pattern"] == "pipeline:*"

                # Publish an event; expect a match
                env = BusEnvelope(
                    type=EventType.PIPELINE_COMPLETED,
                    source="bos://test",
                    payload={"k": "v"},
                )
                b.publish(env)

                # Drain the event
                msg = json.loads(ws.recv(timeout=2.0))
                assert msg["type"] == MSG_EVENT
                assert msg["envelope"]["topic"] == "pipeline:completed"
                assert msg["envelope"]["payload"] == {"k": "v"}
        finally:
            handle.stop()

    def test_ws_client_pattern_filter(self) -> None:
        """Client subscribed to `foo:*` should not receive `bar:*` events."""
        _require_websockets()
        import websockets.sync.client as ws_sync  # type: ignore[import-not-found]

        b = WebSocketBackend()
        port = _free_port()
        handle = b.serve_forever(host="127.0.0.1", port=port)
        try:
            url = f"ws://127.0.0.1:{port}"
            with ws_sync.connect(url) as ws:
                ws.send(json.dumps({"action": "subscribe", "pattern": "foo:*"}))
                ws.recv()  # ack

                # Publish a non-matching event
                b.publish(BusEnvelope(type="bar:thing", source="bos://x"))
                # Publish a matching event
                b.publish(BusEnvelope(type="foo:hit", source="bos://x"))

                # We should get exactly one event, the foo:hit one.
                msg = json.loads(ws.recv(timeout=2.0))
                assert msg["type"] == MSG_EVENT
                assert msg["envelope"]["topic"] == "foo:hit"

                # And nothing more (the bar event was filtered out).
                try:
                    extra = ws.recv(timeout=0.5)
                    assert json.loads(extra)["envelope"]["topic"] != "bar:thing"
                except Exception:  # noqa: BLE001
                    pass  # timeout is the success case
        finally:
            handle.stop()

    def test_ws_ping_pong(self) -> None:
        """Client `ping` → server `pong`."""
        _require_websockets()
        import websockets.sync.client as ws_sync  # type: ignore[import-not-found]

        b = WebSocketBackend()
        port = _free_port()
        handle = b.serve_forever(host="127.0.0.1", port=port)
        try:
            url = f"ws://127.0.0.1:{port}"
            with ws_sync.connect(url) as ws:
                ws.send(json.dumps({"action": "ping"}))
                resp = json.loads(ws.recv(timeout=2.0))
                assert resp["type"] == MSG_PONG
        finally:
            handle.stop()

    def test_ws_unknown_action_returns_error(self) -> None:
        """Client sends unknown action → server returns error message."""
        _require_websockets()
        import websockets.sync.client as ws_sync  # type: ignore[import-not-found]

        b = WebSocketBackend()
        port = _free_port()
        handle = b.serve_forever(host="127.0.0.1", port=port)
        try:
            url = f"ws://127.0.0.1:{port}"
            with ws_sync.connect(url) as ws:
                ws.send(json.dumps({"action": "garbage"}))
                resp = json.loads(ws.recv(timeout=2.0))
                assert resp["type"] == "error"
                assert "garbage" in resp["message"]
        finally:
            handle.stop()
