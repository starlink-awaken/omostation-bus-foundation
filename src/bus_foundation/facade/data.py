"""
Data Plane Facade (bus.data)
Designed for high-throughput, loss-tolerant telemetry and logs.
Fire-and-forget semantics.
"""
from typing import Any

from .. import publish
from ..envelope import BusEnvelope


def emit(topic: str, payload: dict[str, Any], source_uri: str = "bos://capability/bus/data", trace_id: str = None) -> None:
    """
    Emit a data event. Non-blocking, fire-and-forget.
    """
    envelope = BusEnvelope(
        type=topic,  # Legacy mapping: topic -> type
        source=source_uri,
        payload=payload,
        trace_id=trace_id,
        backend="eventbus" # For now, route to simple eventbus
    )
    publish(envelope)
