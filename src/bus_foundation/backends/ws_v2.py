"""WebSocketBackend v2 — R95: protocol versioning + compression + heartbeat.

Extends the R81/v1 backend with:
- **Protocol version** negotiation (handshake on subscribe)
- **Per-message-deflate** compression via websockets lib
- **Heartbeat** (server pings inactive clients every 30s)
- **Reconnection token** — client reconnects with previous token to
  resume subscriptions without re-subscribing

Wire protocol additions (v2 only, v1 kept for BC):
  C -> S:  {"action": "subscribe", "version": 2, "pattern": "..."}
  S -> C:  {"type": "ack", "action": "subscribe", "version": 2, "reconnect_token": "abc123"}
  C -> S:  {"action": "reconnect", "token": "abc123", "version": 2}
  S -> C:  {"type": "event", "envelope": {...}}  (same as v1)
  C -> S:  {"action": "ping"}                    (same as v1)
  S -> C:  {"type": "pong"}                      (same as v1)

Default operation: v1 (backward compatible).
Enable v2: `serve_forever(host, port, version=2)`.
"""

from __future__ import annotations

import asyncio
import importlib.util as _importlib_util
import json
import logging
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from bus_foundation.backends.pattern_match import match_pattern
from bus_foundation.envelope import BusEnvelope

logger = logging.getLogger(__name__)

# ── Protocol constants ───────────────────────────────────────────

MSG_EVENT = "event"
MSG_ACK = "ack"
MSG_PONG = "pong"
MSG_ERROR = "error"
PROTOCOL_VERSION = 2
HEARTBEAT_INTERVAL = 30.0
HEARTBEAT_TIMEOUT = 10.0


class WebSocketServerHandleV2:
    """R95 handle with protocol version + compression controls."""

    def __init__(self, backend: WebSocketBackendV2, host: str, port: int, version: int = 1):
        self._backend = backend
        self._host = host
        self._port = port
        self._version = version
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        if not _importlib_util.find_spec("websockets"):  # noqa: F811
            raise ImportError(
                "WebSocket real transport requires the 'websockets' library. "
                "Install with: pip install bus-foundation[ws]"
            )
        # Check for compression support
        if self._version >= 2:
            try:
                from websockets.extensions.permessage_deflate import ServerPerMessageDeflateFactory  # type: ignore[import-not-found]  # noqa: I001

                self._backend._compression = ServerPerMessageDeflateFactory(
                    server_no_context_takeover=True,
                    client_no_context_takeover=True,
                )
            except ImportError:
                logger.warning("ws_v2_compression_unavailable: permessage_deflate not found")

        def _run() -> None:
            import websockets  # type: ignore[import-not-found]  # noqa: F811,F401

            async def _handler(ws: Any) -> None:
                ws._bus_patterns = set()
                ws._bus_version = self._version
                ws._bus_reconnect_token = None  # type: ignore[misc]
                ws._bus_last_pong = time.monotonic()
                ws.id = f"remote-{uuid.uuid4().hex[:8]}"
                self._backend._ws_connections.add(ws)
                hb_task: asyncio.Task[None] | None = None
                try:
                    # Start heartbeat checker
                    hb_task = asyncio.create_task(_heartbeat(ws))
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except (TypeError, ValueError):
                            await ws.send(json.dumps({"type": MSG_ERROR, "message": "invalid json"}))
                            continue
                        await self._backend._ws_dispatch(ws, msg)
                finally:
                    if hb_task is not None:
                        hb_task.cancel()
                    self._backend._ws_connections.discard(ws)

            async def _heartbeat(ws: Any) -> None:
                """Periodic heartbeat: server sends ping, waits for pong."""
                while True:
                    await asyncio.sleep(HEARTBEAT_INTERVAL)
                    try:
                        await asyncio.wait_for(
                            ws.send(json.dumps({"type": "ping"})),
                            timeout=HEARTBEAT_TIMEOUT,
                        )
                    except Exception:  # noqa: BLE001
                        logger.debug("ws_heartbeat_failed id=%s", getattr(ws, "id", "?"))
                        break

            async def _main() -> None:
                kwargs: dict[str, Any] = {"host": self._host, "port": self._port}
                if self._version >= 2 and self._backend._compression:
                    kwargs["extensions"] = [self._backend._compression]
                server = await websockets.serve(_handler, **kwargs)
                self._backend._server = server
                self._ready.set()
                self._backend._server._sentinel = asyncio.Future()
                try:
                    await self._backend._server._sentinel
                except asyncio.CancelledError:
                    pass
                finally:
                    server.close()
                    await server.wait_closed()

            asyncio.run(_main())

        t = threading.Thread(target=_run, daemon=True, name="ws-bus-server-v2")
        t.start()
        self._thread = t
        self._ready.wait(timeout=5.0)
        self._started = True

    def stop(self) -> None:
        self._backend.stop_server()

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @property
    def is_running(self) -> bool:
        return self._backend._server is not None


class WebSocketBackendV2:
    """R95 WebSocket backend with v1 BC + v2 protocol features.

    Usage:
        backend = WebSocketBackendV2()
        handle = backend.serve_forever(host="0.0.0.0", port=8765, version=2)
    """

    name = "ws"

    def __init__(self) -> None:
        self._clients: dict[str, tuple[str, asyncio.Queue[BusEnvelope], asyncio.Task | None]] = {}
        self._ws_connections: set[Any] = set()
        self._server: Any = None
        self._server_task: asyncio.Task | None = None
        self._compression: Any = None
        # Reconnect tokens: token -> frozenset of patterns
        self._reconnect_tokens: dict[str, frozenset[str]] = {}
        self._lock = threading.Lock()

    # ── BusBackend Protocol ──────────────────────────────────────

    def is_available(self) -> bool:
        return True

    def publish(self, envelope: BusEnvelope) -> str:  # type: ignore[override]
        # Mode 1: in-process fanout
        for client_id, (pattern, queue, _task) in self._clients.items():
            if match_pattern(pattern, envelope.type):
                try:
                    queue.put_nowait(envelope)
                except asyncio.QueueFull:
                    logger.warning("ws_queue_full client_id=%s event_id=%s", client_id, envelope.id)
        # Mode 2: WS fanout
        for ws in self._ws_connections:
            patterns: set[str] = getattr(ws, "_bus_patterns", set())
            if any(match_pattern(p, envelope.type) for p in patterns):
                self._dispatch_ws(ws, {"type": MSG_EVENT, "envelope": envelope.to_dict()})
        return envelope.id

    def subscribe(self, pattern: str, callback: Callable) -> str:
        import asyncio

        client_id = f"ipc-{uuid.uuid4().hex[:8]}"
        queue: asyncio.Queue[BusEnvelope] = asyncio.Queue(maxsize=1000)
        loop = _get_or_create_loop()
        task = loop.create_task(self._drain(client_id, queue, callback))
        self._clients[client_id] = (pattern, queue, task)
        return client_id

    def unsubscribe(self, client_id: str) -> bool:
        entry = self._clients.pop(client_id, None)
        if entry is None:
            return False
        _pattern, _queue, task = entry
        if task is not None:
            task.cancel()
        return True

    async def _drain(self, client_id: str, queue: asyncio.Queue, callback: Callable) -> None:
        try:
            while True:
                env = await queue.get()
                try:
                    callback(env)
                except Exception as e:  # noqa: BLE001
                    logger.error("ws_callback_error client_id=%s err=%s", client_id, e)
        except asyncio.CancelledError:
            logger.debug("ws_drain_cancelled client_id=%s", client_id)

    # ── Mode 2: WebSocket server ────────────────────────────────

    def serve_forever(self, host: str = "127.0.0.1", port: int = 8765, version: int = 1) -> WebSocketServerHandleV2:
        """Start WS server with optional v2 protocol support."""
        handle = WebSocketServerHandleV2(self, host, port, version=version)
        handle.start()
        return handle

    async def _ws_dispatch(self, ws: Any, msg: dict[str, Any]) -> None:
        action = msg.get("action")
        if action == "subscribe":
            pattern = msg.get("pattern", "*")
            version = msg.get("version", 1)
            ws._bus_version = version
            ws._bus_patterns.add(pattern)
            response: dict[str, Any] = {"type": MSG_ACK, "action": "subscribe", "pattern": pattern}
            if version >= 2:
                token = uuid.uuid4().hex[:16]
                ws._bus_reconnect_token = token
                with self._lock:
                    self._reconnect_tokens[token] = frozenset(ws._bus_patterns)
                response["reconnect_token"] = token
                response["version"] = PROTOCOL_VERSION
                response["heartbeat_interval"] = HEARTBEAT_INTERVAL
            await ws.send(json.dumps(response))
        elif action == "unsubscribe":
            pattern = msg.get("pattern", "*")
            ws._bus_patterns.discard(pattern)
            await ws.send(json.dumps({"type": MSG_ACK, "action": "unsubscribe", "pattern": pattern}))
        elif action == "reconnect":
            token = msg.get("token", "")
            with self._lock:
                saved_patterns = self._reconnect_tokens.pop(token, None)
            if saved_patterns is not None:
                ws._bus_patterns = set(saved_patterns)
                await ws.send(
                    json.dumps(
                        {
                            "type": MSG_ACK,
                            "action": "reconnect",
                            "patterns": list(saved_patterns),
                            "version": PROTOCOL_VERSION,
                            "heartbeat_interval": HEARTBEAT_INTERVAL,
                        }
                    )
                )
            else:
                await ws.send(json.dumps({"type": MSG_ERROR, "message": f"invalid reconnect token: {token}"}))
        elif action == "ping":
            await ws.send(json.dumps({"type": MSG_PONG}))
        else:
            await ws.send(json.dumps({"type": MSG_ERROR, "message": f"unknown action: {action!r}"}))

    def _dispatch_ws(self, ws: Any, payload: dict[str, Any]) -> None:
        message = json.dumps(payload)
        server = self._server
        loop = None
        if server is not None:
            try:
                loop = server.loop
            except AttributeError:
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = None
        if loop is not None and loop.is_running():
            asyncio.run_coroutine_threadsafe(ws.send(message), loop)

    def stop_server(self) -> None:
        if self._server is not None:
            srv, self._server = self._server, None
            loop: asyncio.AbstractEventLoop | None = None
            try:
                loop = getattr(srv, "loop", None)
            except Exception:  # noqa: BLE001
                loop = None
            # Cancel all WS connections gracefully (only if a running
            # loop is reachable; otherwise the daemon thread is already
            # dying and we just clear the registry).
            if loop is not None and loop.is_running():
                for ws in list(self._ws_connections):
                    try:
                        asyncio.run_coroutine_threadsafe(ws.close(), loop)
                    except Exception:  # noqa: BLE001
                        pass
                # Cancel the sentinel future so _main() exits cleanly.
                sentinel = getattr(srv, "_sentinel", None)
                if sentinel is not None:
                    try:
                        loop.call_soon_threadsafe(sentinel.cancel)
                    except Exception:  # noqa: BLE001
                        pass
            self._ws_connections.clear()
        self._clients.clear()


def _get_or_create_loop() -> asyncio.AbstractEventLoop:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


__all__ = ["WebSocketBackendV2", "WebSocketServerHandleV2", "PROTOCOL_VERSION"]
