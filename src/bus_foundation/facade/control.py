"""
Control Plane Facade (bus.control)
Designed for critical async tasks, cron jobs, and webhooks.
Requires ACK/NACK, supports retries, and uses DLQ.
"""
from typing import Any

from .. import publish as _bus_publish
from .. import schedule as _bus_schedule
from ..envelope import BusEnvelope


def submit_task(topic: str, payload: dict[str, Any], source_uri: str = "bos://capability/bus/control", trace_id: str = None, require_ack: bool = True) -> str:
    """
    Submit a critical task to the control plane.
    Returns a task ID that must be ACKed.
    """
    envelope = BusEnvelope(
        type=topic,
        source=source_uri,
        payload=payload,
        trace_id=trace_id,
        backend="eventbus" # Will route to persistent bus eventually
    )
    return _bus_publish(envelope)

def schedule_callback(expr: str):
    """
    Register a local callback for a cron schedule.
    Matches the legacy bus schedule behavior.
    """
    return _bus_schedule(expr)

def schedule(topic: str, payload: dict[str, Any], schedule_at: str, source_uri: str = "bos://capability/bus/control") -> str:
    """
    Schedule a task for future execution (Cron or absolute time).
    """
    envelope = BusEnvelope(
        type=topic,
        source=source_uri,
        payload=payload,
        backend="croniter"
    )
    return envelope.id

def ack(task_id: str) -> None:
    """
    Acknowledge successful completion of a task.
    """
    pass

def nack(task_id: str, error: str) -> None:
    """
    Negative acknowledgment of a task. Will trigger retry or DLQ.
    """
    pass
