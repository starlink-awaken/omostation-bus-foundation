"""R89: Metrics abstraction for bus-foundation.

Public surface (lazy / zero-overhead by default):
    from bus_foundation.metrics import Counter, Histogram, Gauge, Registry, snapshot

Enable the registry to start collecting values:
    bus_foundation.enable_metrics(port=8745)   # also spins up /metrics HTTP server

Design notes:
- All classes are pure-Python no-op by default until `Registry.enable()` is
  called. This keeps default-mode overhead at < 1us per call (verified by
  benchmarks/test_metrics_overhead.py).
- No external dependencies. prometheus_client is optional and only loaded
  when `enable_metrics(server=True)` is invoked.
- Thread-safe via a single internal Lock. Lock contention is irrelevant at
  expected QPS (< 10K publish/s).
"""

from __future__ import annotations

import threading
from typing import Any, TypeVar

M = TypeVar("M", bound="_Metric")


class _Metric:
    """Base class for Counter / Histogram / Gauge."""

    def __init__(self, name: str, labels: tuple[str, ...] = ()):
        self.name = name
        self.labels = labels
        self._lock = threading.Lock()
        self._values: dict = {}
        self._data: dict = {}

    def _label_key(self, labels: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
        return tuple(sorted(labels.items()))


class Counter(_Metric):
    """Monotonically increasing counter."""

    def inc(self, amount: float = 1.0, **labels: Any) -> None:
        if not Registry.is_enabled():
            return
        key = self._label_key(labels)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + amount

    def value(self, **labels: Any) -> float:
        return self._values.get(self._label_key(labels), 0.0)  # type: ignore[no-any-return]  # type: ignore[no-any-return]

    def snapshot(self) -> dict[tuple[tuple[str, Any], ...], float]:
        with self._lock:
            return dict(self._values)


class Histogram(_Metric):
    """Distribution of values (Prometheus-style)."""

    DEFAULT_BUCKETS: tuple[float, ...] = (
        0.0001,
        0.0005,
        0.001,
        0.005,
        0.01,
        0.05,
        0.1,
        0.5,
        1.0,
    )

    def __init__(
        self,
        name: str,
        labels: tuple[str, ...] = (),
        buckets: tuple[float, ...] = DEFAULT_BUCKETS,
    ):
        super().__init__(name, labels)
        self.buckets = tuple(sorted(buckets))

    def observe(self, value: float, **labels: Any) -> None:
        if not Registry.is_enabled():
            return
        key = self._label_key(labels)
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                bucket_counts = [0] * len(self.buckets)
                entry = (bucket_counts, 0.0, 0)
            bucket_counts, total, count = entry
            for i, b in enumerate(self.buckets):
                if value <= b:
                    bucket_counts[i] += 1
            self._data[key] = (bucket_counts, total + value, count + 1)

    def snapshot(self) -> dict[tuple[tuple[str, Any], ...], dict[str, Any]]:
        out: dict[tuple[tuple[str, Any], ...], dict[str, float]] = {}
        with self._lock:
            for key, (bucket_counts, total, count) in self._data.items():
                out[key] = {
                    "buckets": dict(zip(self.buckets, bucket_counts, strict=True)),
                    "sum": total,
                    "count": count,
                }
        return out


class Gauge(_Metric):
    """Point-in-time value (can go up or down)."""

    def set(self, value: float, **labels: Any) -> None:
        if not Registry.is_enabled():
            return
        key = self._label_key(labels)
        with self._lock:
            self._values[key] = value

    def inc(self, amount: float = 1.0, **labels: Any) -> None:
        if not Registry.is_enabled():
            return
        key = self._label_key(labels)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + amount

    def dec(self, amount: float = 1.0, **labels: Any) -> None:
        self.inc(-amount, **labels)

    def value(self, **labels: Any) -> float:
        return self._values.get(self._label_key(labels), 0.0)

    def snapshot(self) -> dict[tuple[tuple[str, Any], ...], float]:
        with self._lock:
            return dict(self._values)


class Registry:
    """Global metric registry. Singleton."""

    _enabled: bool = False
    _lock = threading.Lock()
    _metrics: dict[str, _Metric] = {}

    @classmethod
    def is_enabled(cls) -> bool:
        return cls._enabled

    @classmethod
    def enable(cls) -> None:
        with cls._lock:
            cls._enabled = True

    @classmethod
    def disable(cls) -> None:
        with cls._lock:
            cls._enabled = False

    @classmethod
    def register(cls, metric: M) -> M:  # type: ignore[return-value]
        with cls._lock:
            if metric.name in cls._metrics:
                return cls._metrics[metric.name]
            cls._metrics[metric.name] = metric
            return metric

    @classmethod
    def get(cls, name: str) -> _Metric | None:
        return cls._metrics.get(name)

    @classmethod
    def all(cls) -> dict[str, _Metric]:
        with cls._lock:
            return dict(cls._metrics)

    @classmethod
    def reset(cls) -> None:
        """Test helper: clear all metrics. Do not use in production."""
        with cls._lock:
            for m in cls._metrics.values():
                m._values = {}
                m._data = {}


def snapshot() -> dict[str, Any]:
    """Return all metrics as a JSON-serializable dict."""
    out: dict[str, Any] = {}
    for name, m in Registry.all().items():
        if isinstance(m, (Counter, Gauge, Histogram)):
            out[name] = {str(k): v for k, v in m.snapshot().items()}
    return out


__all__ = ["Counter", "Histogram", "Gauge", "Registry", "snapshot"]
