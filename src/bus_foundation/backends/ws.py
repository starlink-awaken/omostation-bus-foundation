"""WebSocketBackend — full-duplex pub/sub for browser / JS clients.

Phase B R73: NEW backend (was 'planned' in README).

R81 rewrite: previously a "transport stub" with asyncio.Queue per
in-process client. R81 adds an optional real WebSocket transport
(`serve_forever`) that binds a TCP port and accepts browser / JS
connections. The default in-process fanout path is preserved so
existing in-cluster callers keep working without changes.

Two operation modes share the same publish() / pattern_match path:

1. **In-process fanout (default)**: `subscribe()` / `connect()` /
   `disconnect()` work against asyncio.Queues. Zero external deps.

2. **Real WebSocket server (opt-in)**: `serve_forever(host, port)`
   spins up a `websockets` asyncio server. Connected clients
   negotiate subscriptions by sending `{"action": "subscribe",
   "pattern": "..."}` over the socket; matching envelopes are
   pushed as `{"type": "event", "envelope": {...}}` JSON messages.
   Requires the `websockets` extra (`pip install bus-foundation[ws]`).

Wire protocol (over the real socket):
  C -> S:  {"action": "subscribe", "pattern": "pipeline:*"}
  C -> S:  {"action": "unsubscribe", "pattern": "pipeline:*"}
  C -> S:  {"action": "ping"}
  S -> C:  {"type": "event", "envelope": {<BusEnvelope dict>}}
  S -> C:  {"type": "ack", "action": "subscribe", "pattern": "..."}
  S -> C:  {"type": "ack", "action": "unsubscribe", "pattern": "..."}
  S -> C:  {"type": "pong"}
  S -> C:  {"type": "error", "message": "..."}
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import uuid
from collections.abc import Callable
from typing import Any

from bus_foundation.backends.pattern_match import match_pattern
from bus_foundation.envelope import BusEnvelope

logger = logging.getLogger(__name__)


# JSON wire-protocol message types. Module-level so the constants are
# importable from tests and from the future browser-side SDK.
MSG_EVENT = "event"
MSG_ACK = "ack"
MSG_PONG = "pong"
MSG_ERROR = "error"


def _encode_event(env: BusEnvelope) -> dict[str, Any]:
    """Serialize a BusEnvelope into the wire-protocol envelope payload."""
    return {"type": MSG_EVENT, "envelope": env.to_dict()}


class WebSocketServerHandle:
    """Handle returned by WebSocketBackend.serve_forever().

    Wraps the daemon-threaded server so the caller can stop it
    cleanly and inspect the bound (host, port).
    """

    def __init__(self, backend: WebSocketBackend, host: str, port: int) -> None:
        self._backend = backend
        self._host = host
        self._port = port
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        try:
            import websockets  # type: ignore[import-not-found]  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "WebSocket real transport requires the 'websockets' library. "
                "Install with: pip install bus-foundation[ws]"
            ) from e

        def _run() -> None:
            import websockets  # type: ignore[import-not-found]

            async def _handler(ws: Any) -> None:
                ws._bus_patterns = set()  # type: ignore[attr-defined]
                ws.id = f"remote-{uuid.uuid4().hex[:8]}"  # type: ignore[attr-defined]
                self._backend._ws_connections.add(ws)
                try:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except (TypeError, ValueError):
                            await ws.send(
                                json.dumps(
                                    {
                                        "type": MSG_ERROR,
                                        "message": "invalid json",
                                    }
                                )
                            )
                            continue
                        await self._backend._ws_dispatch(ws, msg)
                finally:
                    self._backend._ws_connections.discard(ws)

            async def _main() -> None:
                server = await websockets.serve(_handler, self._host, self._port)
                self._backend._server = server
                self._ready.set()
                try:
                    await asyncio.Future()  # run forever
                except asyncio.CancelledError:
                    pass
                finally:
                    server.close()
                    await server.wait_closed()

            asyncio.run(_main())

        t = threading.Thread(target=_run, daemon=True, name="ws-bus-server")
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


class WebSocketBackend:
    """In-process fanout + optional real WebSocket transport.

    Mode 1 (default): in-process asyncio.Queue per subscriber. Calling
    `subscribe()` returns a sub_id; the caller is expected to drain
    envelopes via the returned queue or attach a callback that runs
    in the asyncio loop.

    Mode 2 (opt-in): `serve_forever(host, port)` starts a real WS
    server. The server hands incoming JSON messages to the same
    publish / match path so server-side subscribe + in-process
    subscribe share semantics.
    """

    name = "ws"

    def __init__(self) -> None:
        # mode 1: client_id -> (pattern, queue, drain_task_or_None)
        self._clients: dict[str, tuple[str, asyncio.Queue[BusEnvelope], asyncio.Task | None]] = {}
        # mode 2: per-WS-connection state.
        self._ws_connections: set[Any] = set()
        self._server: Any = None
        self._server_task: asyncio.Task | None = None

    # ── BusBackend Protocol ──────────────────────────────────────

    def is_available(self) -> bool:
        return True

    def publish(self, envelope: BusEnvelope) -> str:
        """Dispatch envelope to in-process subscribers AND live WS clients.

        Both paths use the same pattern match so mode 1 and mode 2
        see the same event stream.
        """
        # Mode 1: in-process fanout.
        for client_id, (pattern, queue, _task) in self._clients.items():
            if self._match(pattern, envelope.type):
                try:
                    queue.put_nowait(envelope)
                except asyncio.QueueFull:
                    logger.warning(
                        "ws_queue_full client_id=%s event_id=%s",
                        client_id,
                        envelope.id,
                    )

        # Mode 2: WS server fanout. We don't await; if a client's
        # outbound queue is full we log and drop (fire-and-forget).
        for ws in list(self._ws_connections):
            patterns: set[str] = getattr(ws, "_bus_patterns", set())
            if any(self._match(p, envelope.type) for p in patterns):
                self._dispatch_ws(ws, _encode_event(envelope))

        return envelope.id

    def subscribe(self, pattern: str, callback: Callable) -> str:
        """Mode 1 subscribe: connect + spawn drain task."""
        client_id, queue = self.connect(pattern)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.warning("ws_subscribe_no_loop client_id=%s", client_id)
            return client_id
        task = loop.create_task(self._drain(client_id, queue, callback))
        if client_id in self._clients:
            pat, q, _ = self._clients[client_id]
            self._clients[client_id] = (pat, q, task)
        return client_id

    def unsubscribe(self, sub_id: str) -> bool:
        return self.disconnect(sub_id)

    # ── Mode 1: in-process ─────────────────────────────────────

    def connect(self, pattern: str) -> tuple[str, asyncio.Queue[BusEnvelope]]:
        """Add an in-process client. Returns (client_id, queue)."""
        client_id = f"ws-{uuid.uuid4().hex[:8]}"
        queue: asyncio.Queue[BusEnvelope] = asyncio.Queue(maxsize=1000)
        self._clients[client_id] = (pattern, queue, None)
        return client_id, queue

    def disconnect(self, client_id: str) -> bool:
        """Cancel drain task and remove client."""
        entry = self._clients.pop(client_id, None)
        if entry is None:
            return False
        _, _, task = entry
        if task is not None and not task.done():
            task.cancel()
        return True

    async def _drain(
        self,
        client_id: str,
        queue: asyncio.Queue,
        callback: Callable,
    ) -> None:
        try:
            while True:
                env = await queue.get()
                try:
                    callback(env)
                except Exception as e:  # noqa: BLE001
                    logger.error("ws_callback_error client_id=%s err=%s", client_id, e)
        except asyncio.CancelledError:
            logger.debug("ws_drain_cancelled client_id=%s", client_id)
            raise

    # ── Mode 2: real WebSocket transport ─────────────────────────

    def serve_forever(
        self,
        host: str = "127.0.0.1",
        port: int = 8765,
    ) -> WebSocketServerHandle:
        """Start a real WebSocket server. Returns a handle to inspect / stop.

        The server implements the JSON wire protocol documented at
        the module top. Requires the `websockets` extra:
            pip install bus-foundation[ws]
        """
        handle = WebSocketServerHandle(self, host, port)
        handle.start()
        return handle

    async def _ws_dispatch(self, ws: Any, msg: dict[str, Any]) -> None:
        """Handle a single JSON message from a WS client."""
        action = msg.get("action")
        if action == "subscribe":
            pattern = msg.get("pattern", "*")
            ws._bus_patterns.add(pattern)  # type: ignore[attr-defined]
            await ws.send(json.dumps({"type": MSG_ACK, "action": "subscribe", "pattern": pattern}))
        elif action == "unsubscribe":
            pattern = msg.get("pattern", "*")
            ws._bus_patterns.discard(pattern)  # type: ignore[attr-defined]
            await ws.send(json.dumps({"type": MSG_ACK, "action": "unsubscribe", "pattern": pattern}))
        elif action == "ping":
            await ws.send(json.dumps({"type": MSG_PONG}))
        else:
            await ws.send(json.dumps({"type": MSG_ERROR, "message": f"unknown action: {action!r}"}))

    def _dispatch_ws(self, ws: Any, payload: dict[str, Any]) -> None:
        """Send a JSON event to a WS client, cross-loop safe."""
        message = json.dumps(payload)
        server = self._server
        loop = None
        if server is not None:
            loop = getattr(server, "loop", None) or getattr(server, "_loop", None)
        if loop is not None:
            try:
                loop.call_soon_threadsafe(asyncio.ensure_future, ws.send(message))
                return
            except RuntimeError:
                pass
        # Fallback: best-effort direct send (only safe in same loop).
        try:
            coro = ws.send(message)
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(coro)
                else:
                    asyncio.ensure_future(coro)
            except RuntimeError:
                pass
        except Exception as e:  # noqa: BLE001
            logger.error(
                "ws_publish_dispatch_error client=%s err=%s",
                getattr(ws, "id", "?"),
                e,
            )

    def stop_server(self) -> None:
        """Stop the real WebSocket server if running."""
        server = self._server
        self._server = None
        if server is not None:
            try:
                server.close()
            except Exception:  # noqa: BLE001
                pass
        task = self._server_task
        self._server_task = None
        if task is not None and not task.done():
            task.cancel()
        for ws in list(self._ws_connections):
            try:
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = None
                if loop is not None and loop.is_running():
                    close_coro = ws.close()
                    loop.create_task(close_coro)
            except Exception:  # noqa: BLE001
                pass
        self._ws_connections.clear()

    @staticmethod
    def _match(pattern: str, event_type: str) -> bool:
        return match_pattern(pattern, event_type)
