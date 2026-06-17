"""
Event Plane Facade (bus.event)
Designed for Pub/Sub, domain events, and state change notifications.
Fan-out semantics.
"""
from typing import Any

from .. import publish as _bus_publish
from .. import subscribe as _bus_subscribe
from ..envelope import BusEnvelope


def publish(topic: str, payload: dict[str, Any], source_uri: str = "bos://capability/bus/event", trace_id: str = None) -> None:
    """
    Publish a domain event to all subscribers.
    """
    envelope = BusEnvelope(
        type=topic,  # Legacy mapping
        source=source_uri,
        payload=payload,
        trace_id=trace_id,
        backend="eventbus"
    )
    _bus_publish(envelope)

def subscribe(topic_pattern: str, version_range: str = ">=1.0"):
    """
    Decorator to register an event subscriber.
    """
    return _bus_subscribe(topic_pattern)
