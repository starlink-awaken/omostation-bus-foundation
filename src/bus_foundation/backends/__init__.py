"""Bus backends — pluggable transport implementations.

Phase B (R66) status: 5 in-process backends (no optional deps).
  - eventbus   : in-process pub/sub (default)
  - croniter   : scheduled jobs (cron expressions)
  - asyncio    : in-process pub/sub via asyncio.Queue
  - messagebus : agent-to-agent request/response
  - sse        : in-process fan-out (HTTP layer wires its own SSE)

Optional cross-process backends (require extras):
  - redis      : cross-process pub/sub via Redis (pip install bus-foundation[redis])
  - ws         : real WS transport              (pip install bus-foundation[ws])
  - zmq        : cross-process pub/sub via ZMQ  (pip install bus-foundation[zmq])

Premium backends that wrap agora.core.* (e.g. the persistent EventBusBackend
that uses agora-events.json) STAY in agora.bus.backends. Consumers can
opt-in by importing from agora.bus.backends instead.
"""

from __future__ import annotations

from bus_foundation.backends.asyncio import AsyncioBackend
from bus_foundation.backends.base import BusBackend
from bus_foundation.backends.croniter import CroniterBackend
from bus_foundation.backends.eventbus import EventBusBackend
from bus_foundation.backends.messagebus import MessageBusBackend
from bus_foundation.backends.sse import SSEBackend

# Optional cross-process backends are loaded lazily via __getattr__ so
# `import bus_foundation` succeeds even when the corresponding
# optional dependency (pyzmq / redis / websockets) is not installed.
_LAZY_BACKENDS: dict[str, tuple[str, str]] = {
    "RedisBackend": ("bus_foundation.backends.redis", "RedisBackend"),
    "WebSocketBackend": ("bus_foundation.backends.ws", "WebSocketBackend"),
    "WebSocketBackendV2": ("bus_foundation.backends.ws_v2", "WebSocketBackendV2"),
    "ZeroMQBackend": ("bus_foundation.backends.zmq", "ZeroMQBackend"),
}


def __getattr__(name: str):  # PEP 562 module-level __getattr__
    """Lazily import optional backends on first access.

    If the corresponding optional dep is not installed, raise a clear
    ImportError pointing the user to the right `pip install` extra.
    """
    if name in _LAZY_BACKENDS:
        module_name, attr = _LAZY_BACKENDS[name]
        import importlib

        try:
            mod = importlib.import_module(module_name)
        except ImportError as e:
            extra = {
                "RedisBackend": "redis",
                "WebSocketBackend": "ws",
                "WebSocketBackendV2": "ws",
                "ZeroMQBackend": "zmq",
            }[name]
            raise ImportError(
                f"{name} requires the '{extra}' optional dependency. Install with: pip install bus-foundation[{extra}]"
            ) from e
        return getattr(mod, attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "AsyncioBackend",
    "BusBackend",
    "CroniterBackend",
    "EventBusBackend",
    "MessageBusBackend",
    "SSEBackend",
    "RedisBackend",
    "WebSocketBackend",
    "WebSocketBackendV2",
    "ZeroMQBackend",
]
