"""DataPlaneBackend — High-throughput, loss-tolerant ring buffer for telemetry.

Designed for fire-and-forget telemetry, logs, and observability data.
"""

import collections
import logging
import threading
import time
from collections.abc import Callable

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
        self._subscribers: dict[str, Callable[[list[OmniEnvelope]], None]] = {}

        self._last_flush = time.time()
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()

    def is_available(self) -> bool:
        return True

    def publish(self, envelope: OmniEnvelope) -> str:
        """Push an envelope into the ring buffer. Fire and forget."""
        with self._lock:
            if len(self._buffer) == self.capacity:
                # Ring buffer is full, oldest will be dropped automatically by deque
                # In a real system, we might increment a drop counter here
                pass
            self._buffer.append(envelope)
        return envelope.id

    def subscribe(self, pattern: str, callback: Callable) -> str:
        """Data plane subscriptions usually consume batches.
        For simplicity, we treat `pattern` as a sub_id or filter.
        """
        import uuid
        sub_id = f"data-sub-{uuid.uuid4().hex[:8]}"
        with self._lock:
            self._subscribers[sub_id] = callback
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
            # Extract current batch
            batch = list(self._buffer)
            self._buffer.clear()

        # Dispatch batch to all subscribers
        # Usually data plane has 1 or 2 aggregate consumers (e.g., langfuse outbox)
        for sub_id, callback in list(self._subscribers.items()):
            try:
                # Consumers of data plane should accept a list of envelopes
                callback(batch)
            except Exception as e:
                logger.error("DataPlane subscriber %s failed during batch flush: %s", sub_id, e)

    def close(self):
        self._flush()
