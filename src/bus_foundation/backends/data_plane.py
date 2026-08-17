"""DataPlaneBackend — High-throughput, loss-tolerant ring buffer for telemetry.

Designed for fire-and-forget telemetry, logs, and observability data.

R80 fix: subscribe(pattern, callback) now honors the `pattern` argument
and only flushes batches where at least one envelope matches the
pattern. Previously every subscriber received every envelope, which
defeats the purpose of subscribing to `telemetry:cpu` only. Per-sub
matchers are stored so unsubscribe() can do precise removal.
"""

import collections
import logging
import threading
import time
from collections.abc import Callable

from bus_foundation.backends.pattern_match import match_pattern
from bus_foundation.envelope import OmniEnvelope

logger = logging.getLogger(__name__)


class DataPlaneBackend:
    """Ring buffer backed data plane with batch flush capability."""

    name = "data_plane"

    def __init__(self, capacity: int = 10000, flush_interval: float = 1.0):
        self.capacity = capacity
        self.flush_interval = flush_interval

        self._buffer: collections.deque = collections.deque(maxlen=capacity)
        self._lock = threading.Lock()
        # R80: per-subscriber (pattern, callback) so flush() can filter.
        self._subscribers: dict[str, tuple[str, Callable[[list[OmniEnvelope]], None]]] = {}

        # R84: telemetry counters (thread-safe, best-effort).
        self._publish_count = 0
        self._drop_count = 0
        self._flush_count = 0
        self._last_publish_time = 0.0
        self._last_flush_time = time.time()

        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()

    def is_available(self) -> bool:
        return True

    def publish(self, envelope: OmniEnvelope) -> str:
        """Push an envelope into the ring buffer. Fire and forget.

        R84: tracks drop count when the ring buffer is full.
        """
        now = time.time()
        with self._lock:
            self._publish_count += 1
            self._last_publish_time = now
            if len(self._buffer) == self.capacity:
                self._drop_count += 1
            self._buffer.append(envelope)
        return envelope.id

    def metrics(self) -> dict:
        """Return operational telemetry for this DataPlane instance.

        Fields:
          publish_count   — total envelopes published (monotonic)
          drop_count      — envelopes dropped due to ring buffer full
          drop_rate_pct   — percentage of publishes that were dropped
          flush_count     — total flush cycles completed
          drain_lag_ms    — milliseconds since the last flush
          buffer_usage    — current ring buffer fill ratio 0.0-1.0
        """
        with self._lock:
            buffer_usage = len(self._buffer) / self.capacity if self.capacity else 0.0
            flush_count = self._flush_count
            drop_count = self._drop_count
            publish_count = self._publish_count
        drain_lag_ms = (time.time() - self._last_flush_time) * 1000
        drop_rate_pct = (drop_count / publish_count * 100) if publish_count else 0.0
        return {
            "publish_count": publish_count,
            "drop_count": drop_count,
            "drop_rate_pct": round(drop_rate_pct, 4),
            "flush_count": flush_count,
            "drain_lag_ms": round(drain_lag_ms, 2),
            "buffer_usage": round(buffer_usage, 4),
        }

    def subscribe(self, pattern: str, callback: Callable) -> str:
        """Subscribe to a pattern; only matching envelopes are flushed.

        `pattern` follows the standard bus syntax: `*` (all), `foo:*`
        (prefix), `foo:bar` (exact). An empty pattern is treated as
        `*` so legacy callers that pass "" still receive everything.
        """
        import uuid

        sub_id = f"data-sub-{uuid.uuid4().hex[:8]}"
        # Normalize empty pattern to catch-all so silent-failure mode
        # (empty pattern => no envelopes) doesn't surprise callers.
        normalized = pattern or "*"
        with self._lock:
            self._subscribers[sub_id] = (normalized, callback)
        return sub_id

    def unsubscribe(self, sub_id: str) -> bool:
        with self._lock:
            return self._subscribers.pop(sub_id, None) is not None

    def _flush_loop(self):
        while True:
            time.sleep(self.flush_interval)
            self._flush()

    def _flush(self):
        with self._lock:
            if not self._buffer:
                return
            # Take a snapshot; clear so producers can keep appending.
            batch = list(self._buffer)
            self._buffer.clear()
            self._flush_count += 1
            self._last_flush_time = time.time()
            subscribers = list(self._subscribers.items())

        # Dispatch per-subscriber with pattern-filtered batch.
        for sub_id, (pattern, callback) in subscribers:
            filtered = [env for env in batch if match_pattern(pattern, env.type)]
            if not filtered:
                continue
            try:
                callback(filtered)
            except Exception as e:
                logger.error(
                    "DataPlane subscriber %s failed during batch flush: %s",
                    sub_id,
                    e,
                )

    def close(self):
        self._flush()
