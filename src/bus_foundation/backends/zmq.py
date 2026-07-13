"""ZeroMQBackend — cross-process pub/sub without any external server.

Uses ZeroMQ PUB/SUB sockets in a peer-to-peer mesh:
- Each backend binds a PUB socket (outgoing) on a configurable port.
- Each backend connects a SUB socket (incoming) to peer PUB addresses.
- No central server needed; works as long as peers can reach each other's ports.

Usage:
    pip install bus-foundation[zmq]

    # Process A (binds a PUB socket on the default port, e.g. ``ZeroMQBackend(port=PORT_A)``)
    backend_a = ZeroMQBackend(port=PORT_A)  # noqa: F821
    backend_a.publish(envelope)  # fans out to all connected SUB peers

    # Process B (connects to A's PUB)
    backend_b = ZeroMQBackend(port=PORT_B, peers=[f"tcp://localhost:{PORT_A}"])  # noqa: F821,F821
    backend_b.subscribe("*", handler)  # receives A's messages

Requirements:
    pyzmq>=25.0 (installed via [zmq] extra, no external server needed)

Round 0 / P7x: ``import zmq`` is lazy so ``import bus_foundation`` still
works on environments without pyzmq installed.
"""

from __future__ import annotations

import logging
import threading
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from bus_foundation.backends.pattern_match import match_pattern
from bus_foundation.envelope import BusEnvelope

if TYPE_CHECKING:
    import zmq as _zmq_typing  # noqa: F401  (for type checkers only)

logger = logging.getLogger(__name__)

DEFAULT_PORT = 8766
DEFAULT_PEERS: list[str] = []
POLL_TIMEOUT_MS = 500


def _require_zmq() -> Any:
    """Lazy import of pyzmq; raises a clear error if not installed."""
    try:
        import zmq
        return zmq
    except ImportError as e:
        raise ImportError(
            "ZeroMQBackend requires the 'zmq' optional dependency. "
            "Install with: pip install bus-foundation[zmq]"
        ) from e


class ZeroMQBackend:
    """Cross-process pub/sub via ZeroMQ (no external server).

    Thread safety:
      - ``publish()`` is thread-safe (ZMQ PUB socket is thread-safe).
      - The SUB listener runs in a single daemon thread.
      - ``subscribe()`` / ``unsubscribe()`` are guarded by a lock.
    """

    name = "zmq"

    def __init__(
        self,
        port: int = DEFAULT_PORT,
        peers: list[str] | None = None,
        context: Any = None,
    ):
        zmq_mod = _require_zmq()
        # Initialize _stop_event FIRST so __del__/close() can run safely
        # even if a later step in __init__ raises.
        self._stop_event = threading.Event()
        self._listener_thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._subscribers: dict[str, tuple[str, Callable]] = {}
        self._port = port
        self._peers = peers or list(DEFAULT_PEERS)

        try:
            self._ctx = context or zmq_mod.Context.instance()

            # Outgoing: PUB socket (binds only when port > 0 for inproc tests)
            self._pub_socket = self._ctx.socket(zmq_mod.PUB)
            if self._port > 0:
                self._pub_socket.bind(f"tcp://*:{self._port}")
            self._pub_socket.linger = 0

            # Incoming: SUB socket (connects to peers)
            self._sub_socket = self._ctx.socket(zmq_mod.SUB)
            self._sub_socket.setsockopt_string(zmq_mod.SUBSCRIBE, "")
            self._sub_socket.rcvtimeo = POLL_TIMEOUT_MS
            self._sub_socket.linger = 0
            for peer in self._peers:
                self._sub_socket.connect(peer)

            # Listener thread
            self._listener_thread = threading.Thread(
                target=self._listen_loop,
                daemon=True,
                name="bus-zmq-listener",
            )
            self._listener_thread.start()
        except Exception:
            # Clean up partial init so __del__ doesn't AttributeError.
            self.close()
            raise

    # ── BusBackend Protocol ──────────────────────────────────────

    def is_available(self) -> bool:
        try:
            return self._pub_socket.closed is False
        except Exception:  # noqa: BLE001
            return False

    def publish(self, envelope: BusEnvelope) -> str:
        """Publish envelope to all connected peers via PUB socket."""
        payload = envelope.to_json()
        self._pub_socket.send_string(payload)
        return envelope.id

    def subscribe(self, pattern: str, callback: Callable) -> str:
        sub_id = f"zmq-{uuid.uuid4().hex[:8]}"
        with self._lock:
            self._subscribers[sub_id] = (pattern, callback)
        return sub_id

    def unsubscribe(self, sub_id: str) -> bool:
        with self._lock:
            if sub_id not in self._subscribers:
                return False
            del self._subscribers[sub_id]
            return True

    # ── Listener ─────────────────────────────────────────────────

    def _listen_loop(self) -> None:
        """Daemon thread: poll SUB socket, dispatch to local subscribers."""
        zmq_mod = _require_zmq()
        while not self._stop_event.is_set():
            try:
                message = self._sub_socket.recv_string()
            except zmq_mod.Again:  # type: ignore[attr-defined]
                continue  # timeout, check stop_event
            except Exception as e:  # noqa: BLE001
                if self._stop_event.is_set():
                    break
                logger.error("zmq_recv_error: %s", e)
                continue

            try:
                env = BusEnvelope.from_json(message)
            except Exception as e:  # noqa: BLE001
                logger.error("zmq_deserialize_error: %s", e)
                continue

            # Snapshot subscribers under lock, dispatch outside
            with self._lock:
                snapshot = list(self._subscribers.items())
            for sub_id, (pattern, callback) in snapshot:
                if match_pattern(pattern, env.type):
                    try:
                        callback(env)
                    except Exception as e:  # noqa: BLE001
                        logger.error("zmq_callback_error sub_id=%s err=%s", sub_id, e)

    def bind_pub(self, address: str) -> None:
        """Bind PUB socket to an explicit address (for inproc tests)."""
        self._pub_socket.bind(address)

    def close(self) -> None:
        """Shutdown listener thread and close ZMQ sockets. Idempotent."""
        if not hasattr(self, "_stop_event"):
            return
        self._stop_event.set()
        thread = getattr(self, "_listener_thread", None)
        if thread is not None and thread.is_alive():
            thread.join(timeout=1.0)
        # Close sockets; tolerate AttributeError if init failed early
        for attr in ("_pub_socket", "_sub_socket"):
            sock = getattr(self, attr, None)
            if sock is not None:
                try:
                    sock.close()
                except Exception:  # noqa: BLE001
                    pass

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:  # noqa: BLE001
            pass  # never let __del__ raise


__all__ = ["ZeroMQBackend", "DEFAULT_PORT"]
