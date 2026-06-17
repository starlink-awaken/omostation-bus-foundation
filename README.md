# bus-foundation

> Standalone Python package providing the unified Omni-Bus interface for omostation.
> Split from `agora/bus/` in Phase B (R66) and upgraded to Omni-Bus v2 (R78).

## Why this package exists

Inside `agora`, the bus lived at `agora/bus/`. As projects adopted it,
the dependency on agora for a generic bus primitive became a tax. Phase B moved
the bus into its own package so projects can `pip install bus-foundation`.
In Phase C (R78), it has been upgraded to a true Omni-Bus supporting unified
planes (Data, Event, Control) to replace the older, monolithic pubsub.

## Architecture

The `OmniEnvelope` schema normalizes all inter-process messages.
Messages are routed into three distinct planes:

1. **Data Plane (`bus.data`)**: Asynchronous, non-blocking Ring Buffer. Flushes in batches (e.g. Telemetry, Logs).
2. **Event Plane (`bus.event`)**: Domain Pub/Sub fan-out (e.g. State changes, Hooks).
3. **Control Plane (`bus.control`)**: Durable SQLite WAL, ACK/NACK semantics, retries, and Dead Letter Queue (DLQ) support (e.g. Cron tasks, async workflows).

## Install

```bash
uv add bus-foundation
# or
pip install bus-foundation
```

## Public API

```python
from bus_foundation.facade import data, event, control
from bus_foundation.envelope import OmniEnvelope, OmniPlane

# 1. Data Plane: High frequency, drop-tail safe
data.emit("system:telemetry", payload={"cpu": 80})

# 2. Event Plane: Domain events
event.publish("pipeline:completed", payload={"task_id": "t-123"})

@event.subscribe("pipeline:*")
def on_pipeline_event(envelope: OmniEnvelope) -> None:
    print(f"received {envelope.topic}")

# 3. Control Plane: Durable execution
control.submit_task("cron:sync_job", payload={"scope": "global"})

@control.task_handler("cron:sync_job")
def handle_sync_job(envelope: OmniEnvelope) -> None:
    # Process the job...
    control.ack(envelope.id)
```

## What's in 0.2.0 (Omni-Bus)

- `OmniEnvelope` wire format (plane, topic, source_uri, trace_id, payload)
- 3 Facades: `bus.data`, `bus.event`, `bus.control`
- Backends: `DataPlaneBackend` (RingBuffer), `ControlPlaneBackend` (SQLite WAL), `EventBusBackend`
- `DLQ` (Dead Letter Queue) for Control Plane failures
- 59 tests with full 0.1.x backwards compatibility for `BusEnvelope`.

## License

Apache-2.0
