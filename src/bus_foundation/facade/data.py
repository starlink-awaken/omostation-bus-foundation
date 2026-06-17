"""
Data Plane Facade (bus.data)
Designed for high-throughput, loss-tolerant telemetry and logs.
Fire-and-forget semantics.
"""
from typing import Any

from ..backends.data_plane import DataPlaneBackend
from ..envelope import OmniEnvelope, OmniPlane

_data_backend = DataPlaneBackend()

def emit(topic: str, payload: dict[str, Any], source_uri: str = "bos://capability/bus/data", trace_id: str = "") -> None:
    """
    Emit a data event. Non-blocking, fire-and-forget, routed via Ring Buffer.
    """
    envelope = OmniEnvelope(
        plane=OmniPlane.DATA,
        topic=topic,
        source_uri=source_uri,
        payload=payload,
        trace_id=trace_id,
    )
    _data_backend.publish(envelope)

def subscribe(pattern: str, callback: Any) -> str:
    """
    Subscribe to the data plane flush batches.
    """
    return _data_backend.subscribe(pattern, callback)
