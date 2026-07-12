"""MessageBusBackend — in-process agent-to-agent pub/sub with req/resp correlation."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable

from bus_foundation.backends.base import dispatch_to_subscribers
from bus_foundation.backends.pattern_match import match_pattern
from bus_foundation.envelope import BusEnvelope

logger = logging.getLogger(__name__)


class MessageBusBackend:
    """In-process agent-to-agent pub/sub with request/response correlation."""

    name = "messagebus"

    def __init__(self):
        self._subscribers: dict[str, tuple[str, Callable]] = {}

    def is_available(self) -> bool:
        return True

    def publish(self, envelope: BusEnvelope) -> str:
        dispatch_to_subscribers(self._subscribers, envelope, ctx="messagebus")
        return envelope.id

    def subscribe(self, pattern: str, callback: Callable) -> str:
        sub_id = f"msgbus-{uuid.uuid4().hex[:8]}"
        self._subscribers[sub_id] = (pattern, callback)
        return sub_id

    def unsubscribe(self, sub_id: str) -> bool:
        return self._subscribers.pop(sub_id, None) is not None

    @staticmethod
    def _match(pattern: str, event_type: str) -> bool:
        return match_pattern(pattern, event_type)
