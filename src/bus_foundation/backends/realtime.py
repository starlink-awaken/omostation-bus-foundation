"""RealtimeBackend — task/version event stream with optimistic concurrency.

Phase B R73: NEW backend (was 'planned' in README).

Use case: tasks/objects that need to be watched for changes by id.
Reuses the `version INTEGER` optimistic-concurrency pattern from
`agora.realtime.TaskSync` (which this replaces; the agora version
remains for backward compat).

Each subscribed task_id has a current version. publish() upserts
a new version; subscribers get notified of the new version.

R79 fixes (this rewrite):
- Callback signature is now `cb(envelope)` only, matching the
  BusBackend Protocol. The version is exposed via `envelope.retry_count`
  repurposed as `version` (kept 0..N monotonic) so subscribers that
  want the version can read it from the envelope.
- `unsubscribe` tracks each sub_id to its exact callback instance so
  removal is precise, not "pop the most recent" (R73 fix was lossy).
"""

from __future__ import annotations

import logging
import threading
import uuid
from collections import defaultdict
from collections.abc import Callable

from bus_foundation.envelope import BusEnvelope

logger = logging.getLogger(__name__)


class RealtimeBackend:
    """Versioned per-task event stream.

    Subscribers register a callback keyed on task_id. publish() takes
    an envelope, treats envelope.type as task_id, bumps version
    monotonically, and dispatches the envelope (with version stamped
    onto envelope.retry_count) to every subscriber.
    """

    name = "realtime"

    def __init__(self) -> None:
        self._versions: dict[str, int] = defaultdict(int)
        # R79: store callbacks in a dict so unsubscribe can target the
        # exact instance, not "the most recent push".
        self._subscribers: dict[str, dict[str, Callable]] = defaultdict(dict)
        # R79: sub_id -> (task_id, callback) for exact removal.
        self._sub_index: dict[str, tuple[str, Callable]] = {}
        self._lock = threading.Lock()

    def is_available(self) -> bool:
        return True

    def publish(self, envelope: BusEnvelope) -> str:
        """Publish: treat envelope.type as task_id, payload as event.

        Stamps the monotonically increasing version onto
        envelope.retry_count so single-arg subscribers can read it.
        """
        task_id = envelope.type
        with self._lock:
            self._versions[task_id] += 1
            version = self._versions[task_id]
            subs = list(self._subscribers.get(task_id, {}).items())
        for _sub_id, cb in subs:
            try:
                # R79: surface the version on the envelope itself so the
                # callback can stay single-arg and Protocol-compliant.
                envelope.retry_count = version
                cb(envelope)
            except Exception as e:  # noqa: BLE001  # defensive fallback
                logger.error("realtime_callback_error task_id=%s err=%s", task_id, e)
        return envelope.id

    def subscribe(self, pattern: str, callback: Callable) -> str:
        """Subscribe by task_id (parameter is named `pattern` for Protocol
        compat, but in this backend it is a literal task_id — no wildcards).

        R79: callbacks are stored in a dict keyed by sub_id, so
        unsubscribe removes the exact instance (no more "pop the most
        recent" loss when multiple subscribers watch the same task_id).
        """
        sub_id = f"rt-{uuid.uuid4().hex[:8]}"
        with self._lock:
            self._subscribers[pattern][sub_id] = callback
            self._sub_index[sub_id] = (pattern, callback)
        return sub_id

    def unsubscribe(self, sub_id: str) -> bool:
        """R79: precise removal via the sub_index reverse map."""
        with self._lock:
            entry = self._sub_index.pop(sub_id, None)
            if entry is None:
                return False
            task_id, _cb = entry
            task_subs = self._subscribers.get(task_id)
            if task_subs is not None and sub_id in task_subs:
                del task_subs[sub_id]
                if not task_subs:
                    del self._subscribers[task_id]
            return True

    def get_version(self, task_id: str) -> int:
        with self._lock:
            return self._versions[task_id]
