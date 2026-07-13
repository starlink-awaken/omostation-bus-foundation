# ruff: noqa: I001
"""bus_foundation — unified bus interface (Phase B, R66 split from agora/bus).

Version: 0.3.0 (R89-R97 modernization) + R93-R95 extensions.

Public API:
    publish(envelope)  — publish an event
    subscribe(pattern, fn)  — register a subscriber
    schedule(expr, fn)  — schedule a recurring task
    enable_metrics(port=N, server=True)  — R89: enable /metrics endpoint
    metrics.snapshot()  — R89: read all counters/gauges/histograms

Architecture: facade → router → backend (9 + Redis + WS v2) → DLQ on failure.
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Callable
from pathlib import Path

__version__ = "0.3.0"


from bus_foundation.backends.base import BusBackend
from bus_foundation.backends.croniter import CroniterBackend
from bus_foundation.backends.eventbus import EventBusBackend
from bus_foundation.dlq import DLQ
from bus_foundation.envelope import BusEnvelope, EventType
from bus_foundation.metrics import Registry as _MetricsRegistry

# R89: metrics surface (default OFF, zero overhead)
from bus_foundation.metrics import snapshot as metrics_snapshot
from bus_foundation.metrics_instrumentation import (  # noqa: F401  (side-effect: registers metrics)
    record_backend_health,
    record_dlq_depth,
    record_subscribe,
)
from bus_foundation.router import Router

_metrics_server = None


def enable_metrics(
    port: int | None = None,
    host: str = "0.0.0.0",
    server: bool = True,
) -> None:
    """R89: enable metric collection and (optionally) HTTP /metrics server.

    Args:
        port: TCP port for /metrics. Falls back to BUS_METRICS_PORT env var.
            If neither is set, raises ValueError (the port is a workspace SSOT,
            not a default; see protocols/port-registry.yaml → 8745).
        host: Bind address.
        server: If True, also start a daemon HTTP server exposing /metrics.
    """
    global _metrics_server
    _MetricsRegistry.enable()
    if not server:
        return
    if port is None:
        env_port = os.environ.get("BUS_METRICS_PORT")
        if not env_port:
            raise ValueError(
                "bus-foundation /metrics port not configured. "
                "Pass `port=` to enable_metrics() or set the BUS_METRICS_PORT "
                "env var. See protocols/port-registry.yaml (bus-foundation-metrics)."
            )
        port = int(env_port)
    if _metrics_server is None:
        from bus_foundation.metrics_server import MetricsHTTPServer

        _metrics_server = MetricsHTTPServer(host=host, port=port)
        _metrics_server.start()


def disable_metrics() -> None:
    """R89: stop collecting metrics + shutdown /metrics server (if any)."""
    global _metrics_server
    _MetricsRegistry.disable()
    if _metrics_server is not None:
        _metrics_server.stop()
        _metrics_server = None


# R90: retry + circuit breaker surface
from bus_foundation.retry import (  # noqa: E402
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    RetryPolicy,
    RetryRegistry,
    with_retry,
)


def set_retry_policy(backend: str, policy: RetryPolicy) -> None:
    """R90: enable retry on a backend. Default is disabled.

    Per RETRY-OWNERSHIP.md, backend adapters do NOT retry. This opt-in
    application-layer retry wraps the backend's publish() call.

    Suggested defaults for new backends:
        bus_foundation.set_retry_policy(
            "my_backend",
            RetryPolicy(max_attempts=3, initial_backoff=0.1, max_backoff=5.0),
        )

    For fire-and-forget (data_plane) or scheduled (croniter) semantics,
    do NOT call this — those backends will skip retry automatically.
    """
    RetryRegistry.set_policy(backend, policy)


def retry_stats() -> dict:
    """R90: snapshot all per-backend retry policies + circuit breaker state."""
    return RetryRegistry.all()


# R92: schema registry surface
from bus_foundation.schema import (  # noqa: E402
    CompatibilityMode,
    SchemaError,
    SchemaRegistry,
    validate as _validate_schema,
)


def validate_envelope(envelope: BusEnvelope, target_version: str = "v2") -> None:
    """R92: validate an envelope is compatible with target schema version.

    Raises SchemaError if incompatible. Default target is v2.

    Background: the `schema_version` field on the envelope has been
    declared but unused since R82. R92 makes it a first-class concept
    so consumers can declare what version they expect and reject
    incompatible envelopes early.
    """
    _validate_schema(envelope, target_version)


class FallbackLogger:
    """Hard-wired fallback logger to ensure dying signals are delivered."""

    def __init__(self, fallback_file="~/.runtime/bus_fallback.log"):
        self.fallback_file = Path(fallback_file).expanduser()
        self.fallback_file.parent.mkdir(parents=True, exist_ok=True)

    def critical(self, message: str, envelope: BusEnvelope | None = None) -> None:
        try:
            with open(self.fallback_file, "a") as f:
                f.write(f"[FALLBACK CRITICAL] {message}\\n")
                if envelope:
                    f.write(f"Envelope Dump: {envelope.to_json()}\\n")
            print(f"[FALLBACK CRITICAL] {message}", file=sys.stderr)
        except Exception as e:  # noqa: BLE001  # defensive fallback
            print(f"FATAL: Fallback logger also failed: {e}", file=sys.stderr)


_fallback = FallbackLogger()

logger = logging.getLogger(__name__)

# Multi-backend registry — extend here when adding new backends.
_backends: dict[str, BusBackend] = {
    "eventbus": EventBusBackend(),
    "croniter": CroniterBackend(),
}
# Type-narrowed alias so pyright/mypy see CroniterBackend.start().
_croniter: CroniterBackend = _backends["croniter"]  # type: ignore[assignment]

_default_backend = _backends["eventbus"]
_default_dlq = DLQ()
_router = Router(backend=_default_backend, dlq=_default_dlq)

# R94: trace context surface
from bus_foundation.observability import (  # noqa: E402
    get_current_trace_id,
    trace,
)


# Start croniter scheduler thread (no-op if not used)
_croniter.start()

__all__ = [
    "__version__",
    "BusEnvelope",
    "EventType",
    "publish",
    "subscribe",
    "schedule",
    "enable_metrics",
    "disable_metrics",
    "metrics_snapshot",
    "set_retry_policy",
    "retry_stats",
    "RetryPolicy",
    "CircuitBreaker",
    "CircuitState",
    "CircuitOpenError",
    "validate_envelope",
    "CompatibilityMode",
    "SchemaError",
    "SchemaRegistry",
    "trace",
    "get_current_trace_id",
]


def publish(envelope: BusEnvelope) -> str:
    """Publish via the appropriate backend (envelope.backend, default eventbus).

    .. deprecated::
        Prefer the per-plane facades for new code:

        - :func:`bus_foundation.facade.event.publish` (pub/sub, fan-out)
        - :func:`bus_foundation.facade.control.submit_task` (ack/nack/DLQ)
        - :func:`bus_foundation.facade.data.emit` (high-throughput, fire-and-forget)

        This top-level entry is kept for backward compatibility (agora.bus
        consumers + legacy adapters) but is no longer the recommended
        surface. It will be removed in 1.0.0.
    """
    backend_name = getattr(envelope, "backend", None) or "eventbus"
    # R94: attach current trace_id to envelope before publish
    from bus_foundation.observability import attach_to_envelope

    attach_to_envelope(envelope)
    # R89: instrument publish latency + status (zero-overhead when metrics disabled)
    from bus_foundation.metrics_instrumentation import instrument_publish

    with instrument_publish(backend_name) as ctx:
        try:
            backend = _backends.get(backend_name)
            if backend is None:
                event_id = _router.publish(envelope)
                ctx["status"] = "success"
                return event_id
            if not backend.is_available():
                _default_dlq.enqueue(
                    event_id=envelope.id,
                    backend=backend_name,
                    envelope_json=envelope.to_json(),
                    error="backend unavailable",
                )
                ctx["status"] = "dlq"
                record_dlq_depth(backend_name, "PENDING", _dlq_count(_default_dlq, "PENDING"))
                return envelope.id
            try:
                # R90: opt-in retry + circuit breaker (no-op if no policy set)
                event_id = with_retry(backend_name, lambda: backend.publish(envelope))
                ctx["status"] = "success"
                return event_id
            except CircuitOpenError:
                # Circuit open: skip to DLQ
                _default_dlq.enqueue(
                    event_id=envelope.id,
                    backend=backend_name,
                    envelope_json=envelope.to_json(),
                    error="circuit open",
                )
                ctx["status"] = "dlq"
                record_dlq_depth(backend_name, "PENDING", _dlq_count(_default_dlq, "PENDING"))
                return envelope.id
            except Exception as e:  # noqa: BLE001  # defensive fallback
                try:
                    _default_dlq.enqueue(
                        event_id=envelope.id,
                        backend=backend_name,
                        envelope_json=envelope.to_json(),
                        error=str(e),
                    )
                except Exception as dlq_err:  # noqa: BLE001
                    _fallback.critical(f"DLQ enqueue failed during publish error handling: {dlq_err}", envelope)
                ctx["status"] = "dlq"
                record_dlq_depth(backend_name, "PENDING", _dlq_count(_default_dlq, "PENDING"))
                return envelope.id
        except Exception:  # noqa: BLE001  # last-resort: never let metrics break publish
            ctx["status"] = "fail"
            return envelope.id


def _dlq_count(dlq: DLQ, status: str) -> int:
    """Helper: count DLQ entries with given status. Cheap O(N) scan; OK for now."""
    try:
        return sum(1 for e in dlq.list_all() if e.get("status") == status)
    except Exception:  # noqa: BLE001
        return 0


def subscribe(pattern: str) -> Callable:
    """Decorator: register a subscriber for a pattern (uses eventbus backend).

    .. deprecated::
        Use :func:`bus_foundation.facade.event.subscribe` for new code.
    """

    def decorator(fn: Callable) -> Callable:
        sub_id = _default_backend.subscribe(pattern, fn)
        logger.warning("bus_subscribed: pattern=%s sub_id=%s fn=%s", pattern, sub_id, fn.__name__)
        return fn

    return decorator


def schedule(expr: str) -> Callable:
    """Decorator: schedule a recurring task via the croniter backend.

    .. deprecated::
        Use :func:`bus_foundation.facade.control.schedule_callback` for new code.
    """
    if not isinstance(expr, str):
        raise TypeError(f"schedule expression must be str, got {type(expr)}")

    def decorator(fn: Callable) -> Callable:
        sub_id = _croniter.subscribe(expr, fn)
        logger.warning("bus_scheduled: expr=%s sub_id=%s fn=%s", expr, sub_id, fn.__name__)
        return fn

    return decorator
