"""EventBusBackend — in-process pubsub (bus-foundation flavor).

Phase B (R66): bus-foundation has NO agora dependency. The premium backend
that wraps `agora.core.event_bus` STAYS in agora (it is an agora-specific
premium backend that consumers can opt into). The bus-foundation version
here is a simple in-process dict-of-lists pubsub for projects that don't
need agora's persistent-event-log semantics.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable

from bus_foundation.backends.base import dispatch_to_subscribers
from bus_foundation.backends.pattern_match import match_pattern
from bus_foundation.envelope import BusEnvelope

logger = logging.getLogger(__name__)


class EventBusBackend:
    """Simple in-process pub/sub for bus-foundation (zero external deps).

    Implementation: dict[sub_id] = (pattern, callback). Publish() iterates
    and dispatches synchronously. Patterns: '*' (all), 'foo:*' (prefix),
    'foo:bar' (exact).

    Thread safety: dispatch_to_subscribers handles the subscriber list
    copy internally; callbacks must not call subscribe/unsubscribe of the
    same backend from within a callback.
    """

    name = "eventbus"

    def __init__(self) -> None:
        self._subscribers: dict[str, tuple[str, Callable]] = {}

    def is_available(self) -> bool:
        """In-process backend is always available."""
        return True

    def publish(self, envelope: BusEnvelope) -> str:
        dispatch_to_subscribers(self._subscribers, envelope, ctx="eventbus")
        return envelope.id

    def subscribe(self, pattern: str, callback: Callable) -> str:
        sub_id = f"eventbus-{uuid.uuid4().hex[:8]}"
        self._subscribers[sub_id] = (pattern, callback)
        return sub_id

    def unsubscribe(self, sub_id: str) -> bool:
        return self._subscribers.pop(sub_id, None) is not None

    @staticmethod
    def _match(pattern: str, event_type: str) -> bool:
        return match_pattern(pattern, event_type)
