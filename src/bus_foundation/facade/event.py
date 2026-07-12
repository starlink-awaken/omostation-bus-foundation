"""
Event Plane Facade (bus.event)
Designed for Pub/Sub, domain events, and state change notifications.
Fan-out semantics.
"""

from typing import Any

from .. import publish as _bus_publish
from .. import subscribe as _bus_subscribe
from ..envelope import OmniEnvelope, OmniPlane


def publish(
    topic: str, payload: dict[str, Any], source_uri: str = "bos://capability/bus/event", trace_id: str = ""
) -> None:
    """
    Publish a domain event to all subscribers.
    """
    envelope = OmniEnvelope(
        plane=OmniPlane.EVENT,
        topic=topic,
        source_uri=source_uri,
        payload=payload,
        trace_id=trace_id,
    )
    # The global bus publish will use eventbus/messagebus underlying layer
    _bus_publish(envelope)


def subscribe(topic_pattern: str, version_range: str = ">=1.0") -> Callable:
    """
    Decorator to register an event subscriber.
    """
    return _bus_subscribe(topic_pattern)


def outbox_emit(
    db_session: Any,
    topic: str,
    payload: dict[str, Any],
    source_uri: str = "bos://capability/bus/event",
    trace_id: str = "",
) -> None:
    """
    Transactional Outbox Pattern.
    Writes the event directly into the provided database session instead of publishing immediately.
    A separate background worker should poll the outbox table and call `publish()`.
    """
    import json
    import time
    import uuid

    envelope_data = {
        "id": uuid.uuid4().hex,
        "plane": OmniPlane.EVENT.value,
        "topic": topic,
        "source_uri": source_uri,
        "payload": payload,
        "trace_id": trace_id,
        "timestamp": time.time(),
        "version": "1.0",
    }

    # Assuming db_session is an SQLAlchemy session
    # We execute a raw SQL insert to an outbox table
    # This ensures the event is committed exactly when the business logic is committed
    db_session.execute(
        "INSERT INTO _bus_outbox (id, envelope_json, created_at, status) VALUES (:id, :json, :now, 'PENDING')",
        {"id": envelope_data["id"], "json": json.dumps(envelope_data), "now": envelope_data["timestamp"]},
    )
