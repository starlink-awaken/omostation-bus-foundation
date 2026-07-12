"""R89: Bus-foundation metric instrumentation.

Defines the standard metric set (registered into Registry on first import)
and provides `instrument_publish` / `instrument_subscribe` helpers for
backend adapters. Each backend calls these from its publish() /
subscribe() path. Default zero-overhead when Registry.is_enabled() is
False.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager

from .metrics import Counter, Gauge, Histogram, Registry

# Standard metric set (names from protocols/metrics-registry.yaml)
PUBLISH_TOTAL: Counter = Registry.register(Counter("bus_publish_total", labels=("backend", "status")))
PUBLISH_LATENCY: Histogram = Registry.register(Histogram("bus_publish_latency_seconds", labels=("backend",)))
SUBSCRIBE_ACTIVE: Gauge = Registry.register(Gauge("bus_subscribe_active", labels=("backend", "pattern")))
DLQ_DEPTH: Gauge = Registry.register(Gauge("bus_dlq_depth", labels=("backend", "status")))
BACKEND_HEALTHY: Gauge = Registry.register(Gauge("bus_backend_healthy", labels=("backend",)))


@contextmanager
def instrument_publish(backend: str) -> Iterator[dict[str, str]]:
    """Context manager: record publish latency and status.

    Usage:
        with instrument_publish("eventbus") as ctx:
            backend.publish(envelope)
            ctx["status"] = "success"
    """
    state: dict[str, str] = {"status": "fail"}
    start = time.perf_counter()
    try:
        yield state
    finally:
        elapsed = time.perf_counter() - start
        PUBLISH_LATENCY.observe(elapsed, backend=backend)
        PUBLISH_TOTAL.inc(backend=backend, status=state["status"])


def record_subscribe(backend: str, pattern: str, active: int) -> None:
    """Update active subscription gauge."""
    SUBSCRIBE_ACTIVE.set(float(active), backend=backend, pattern=pattern)


def record_dlq_depth(backend: str, status: str, depth: int) -> None:
    """Update DLQ depth gauge."""
    DLQ_DEPTH.set(float(depth), backend=backend, status=status)


def record_backend_health(backend: str, healthy: bool) -> None:
    """Update backend health gauge."""
    BACKEND_HEALTHY.set(1.0 if healthy else 0.0, backend=backend)


__all__ = [
    "instrument_publish",
    "record_subscribe",
    "record_dlq_depth",
    "record_backend_health",
    "PUBLISH_TOTAL",
    "PUBLISH_LATENCY",
    "SUBSCRIBE_ACTIVE",
    "DLQ_DEPTH",
    "BACKEND_HEALTHY",
]
