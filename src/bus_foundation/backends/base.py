"""BusBackend Protocol — contract all backends must satisfy.

Also exports shared dispatch helpers used across backends to avoid
duplicating the subscriber-iteration + pattern-match + try/except
pattern six times.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

from bus_foundation.backends.pattern_match import match_pattern
from bus_foundation.envelope import BusEnvelope

logger = logging.getLogger(__name__)


def safe_call(
    callback: Callable[[BusEnvelope], Any],
    envelope: BusEnvelope,
    *,
    ctx: str = "",
    sub_id: str = "",
) -> None:
    """Call a subscriber callback, logging (not raising) on error.

    This eliminates the duplicated ``except Exception: logger.error``
    block found in every backend.
    """
    try:
        callback(envelope)
    except Exception as e:  # noqa: BLE001
        ctx_part = f" {ctx}" if ctx else ""
        sub_part = f" sub_id={sub_id}" if sub_id else ""
        logger.error("callback_error%s%s err=%s", ctx_part, sub_part, e)


def dispatch_to_subscribers(
    subscribers: dict[str, tuple[str, Callable[[BusEnvelope], Any]]],
    envelope: BusEnvelope,
    *,
    ctx: str = "",
) -> None:
    """Fan-out an envelope to all matching subscribers.

    Shared helper that eliminates the duplicated subscriber-iteration
    + pattern-match + try/except block across backends.

    Args:
        subscribers: dict of sub_id -> (pattern, callback)
        envelope: the event to dispatch
        ctx: optional context string for log messages
    """
    for sub_id, (pattern, callback) in list(subscribers.items()):
        if match_pattern(pattern, envelope.type):
            safe_call(callback, envelope, ctx=ctx or "dispatch", sub_id=sub_id)


@runtime_checkable
class BusBackend(Protocol):
    """Pluggable bus transport."""

    name: str

    def is_available(self) -> bool:
        """Return True if backend is reachable and writable."""
        ...

    def publish(self, envelope: BusEnvelope) -> str:
        """Publish event. Returns event_id.

        MUST NOT retry on failure (RETRY-OWNERSHIP.md).
        MUST raise on failure (router catches + writes to DLQ).
        """
        ...

    def subscribe(self, pattern: str, callback: Callable[[BusEnvelope], Any]) -> str:
        """Subscribe to events matching pattern. Returns subscription_id."""
        ...

    def unsubscribe(self, sub_id: str) -> bool:
        """Remove subscription. Returns True if removed."""
        ...
