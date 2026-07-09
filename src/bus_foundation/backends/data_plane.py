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

        self._buffer = collections.deque(maxlen=capacity)
        self._lock = threading.Lock()
        # R80: per-subscriber (pattern, callback) so flush() can filter
        # the batch by pattern. Previously the pattern was ignored.
        self._subscribers: dict[str, tuple[str, Callable[[list[OmniEnvelope]], None]]] = {}

        self._last_flush = time.time()
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()

    def is_available(self) -> bool:
        return True

    def publish(self, envelope: OmniEnvelope) -> str:
        """Push an envelope into the ring buffer. Fire and forget."""
        with self._lock:
            self._buffer.append(envelope)
        return envelope.id

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
            # Take a snapshot; clear so producers can keep appending
            # while we dispatch. The deque is the source of truth.
            batch = list(self._buffer)
            self._buffer.clear()
            # Snapshot subscribers under the lock.
            subscribers = list(self._subscribers.items())

        # Dispatch per-subscriber with pattern-filtered batch.
        for sub_id, (pattern, callback) in subscribers:
            filtered = [env for env in batch if match_pattern(pattern, env.type)]
            if not filtered:
                continue
            try:
                callback(filtered)
            except Exception as e:  # noqa: BLE001
                logger.error(
                    "DataPlane subscriber %s failed during batch flush: %s",
                    sub_id,
                    e,
                )

    def close(self):
        self._flush()

