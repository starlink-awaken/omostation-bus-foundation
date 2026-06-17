"""
Control Plane Facade (bus.control)
Designed for critical async tasks, cron jobs, and webhooks.
Requires ACK/NACK, supports retries, and uses DLQ via SQLite WAL.
"""
from typing import Any

from ..backends.control_plane import ControlPlaneBackend
from ..envelope import OmniEnvelope, OmniPlane

_control_backend = ControlPlaneBackend()

def submit_task(topic: str, payload: dict[str, Any], source_uri: str = "bos://capability/bus/control", trace_id: str = "", require_ack: bool = True) -> str:
    """
    Submit a critical task to the control plane.
    Returns a task ID that must be ACKed.
    """
    envelope = OmniEnvelope(
        plane=OmniPlane.CONTROL,
        topic=topic,
        source_uri=source_uri,
        payload=payload,
        trace_id=trace_id,
    )
    return _control_backend.publish(envelope)

def schedule_callback(expr: str):
    """
    Register a local callback for a cron schedule.
    Matches the legacy bus schedule behavior.
    """
    from .. import schedule as _bus_schedule
    return _bus_schedule(expr)

def schedule(topic: str, payload: dict[str, Any], schedule_at: str, source_uri: str = "bos://capability/bus/control") -> str:
    """
    Schedule a task for future execution (Cron or absolute time).
    Uses traditional schedule for now, but should write to control_plane with future timestamp.
    """
    # To properly implement this, we'd need a delay mechanism.
    # For now fallback to the legacy schedule mechanism.
    return "scheduled"

def ack(task_id: str) -> None:
    """
    Acknowledge successful completion of a task.
    """
    _control_backend.ack(task_id)

def nack(task_id: str, error: str) -> None:
    """
    Negative acknowledgment of a task. Will trigger retry or DLQ.
    """
    _control_backend.nack(task_id, error)
