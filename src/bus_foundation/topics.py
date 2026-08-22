"""Topic constant registry — SSOT for cross-project bus topic names.

Round 0 (P7x-bus-foundation-rollout): centralized topic catalog so adapter
authors can `from bus_foundation.topics import PIPELINE_COMPLETED` instead
of inventing free-form strings. This eliminates the "string typo" class of
bugs where one project publishes `pipeline:completed` and another
subscribes to `pipieline:completed`.

Convention (v1):
  - lowercase, colon-separated hierarchical
  - first segment = producer domain (pipeline, debt, swarm, mesh,
    kairon, metaos, omo, runtime, system, control, bus)
  - second segment = event category (e.g. status change, completion, error)
  - remaining segments = specific event

Producers should prefer the named constants; the bus router does pattern
matching on the *string* form so subscription patterns like "pipeline.*"
still work as before.
"""

from __future__ import annotations

# ── pipeline / agora events ────────────────────────────────────────
PIPELINE_COMPLETED = "pipeline:completed"
PIPELINE_STARTED = "pipeline:started"
PIPELINE_STEP_OK = "pipeline:step:ok"
PIPELINE_FAILED = "pipeline:failed"

# ── governance (omo) ──────────────────────────────────────────────
DEBT_CREATED = "debt:created"
DEBT_REVIEWED = "debt:reviewed"
NODE_COMPLETED = "node:completed"
REGISTRY_SERVICE_REGISTERED = "registry:service.registered"

# ── control plane (system) ────────────────────────────────────────
SYSTEM_CRON_TICK = "system:cron:tick"
SYSTEM_BUS_DLQ_PURGED = "system:bus:dlq:purged"
SYSTEM_BUS_HEALTH = "system:bus:health"

# ── kairon pipeline ───────────────────────────────────────────────
KAIRON_SOURCE_INGESTED = "kairon:source:ingested"
KAIRON_EXTRACTION_COMPLETED = "kairon:extraction:completed"
KAIRON_QUALITY_GATE_RESULT = "kairon:quality_gate:result"
KAIRON_DOWNSTREAM_DISPATCHED = "kairon:downstream:dispatched"

# ── metaos workflow ───────────────────────────────────────────────
METAOS_NODE_PENDING = "metaos:node:pending"
METAOS_NODE_RUNNING = "metaos:node:running"
METAOS_NODE_COMPLETED = "metaos:node:completed"
METAOS_NODE_FAILED = "metaos:node:failed"
METAOS_NODE_TIMED_OUT = "metaos:node:timed_out"
METAOS_NODE_AWAITING_APPROVAL = "metaos:node:awaiting_approval"
METAOS_APPROVAL_GRANTED = "metaos:approval:granted"
METAOS_APPROVAL_DENIED = "metaos:approval:denied"

# ── aetherforge (swarm / mesh) ────────────────────────────────────
SWARM_WORKER_HATCHED = "swarm:worker:hatched"
SWARM_WORKER_TERMINATED = "swarm:worker:terminated"
SWARM_HATCHER_INITIALIZED = "swarm:hatcher:initialized"
SWARM_HATCHER_SHUTDOWN = "swarm:hatcher:shutdown"
SWARM_HATCHER_DRAIN_ALL = "swarm:hatcher:drain_all"
SWARM_HATCHER_WORKER_CANCELLED = "swarm:hatcher:worker:cancelled"
SWARM_HATCHER_WORKER_RETRY = "swarm:hatcher:worker:retry_scheduled"
SWARM_HATCHER_WORKER_DEAD_LETTER = "swarm:hatcher:worker:dead_letter"
SWARM_AGENT_SEND = "swarm:agent:send"
SWARM_AGENT_RECEIVE = "swarm:agent:receive"
SWARM_AGENT_ACK = "swarm:agent:ack"
MESH_NODE_STATUS_CHANGED = "mesh:node:status_changed"
MESH_ROUTE = "mesh:route"
SWARM_STEP = "swarm:step"
LLM_CALL = "llm:call"

# ── workflow mesh (resident event ingest) ──────────────────────────
MESH_WORKFLOW_REQUESTED = "mesh:workflow:requested"
MESH_WORKFLOW_STARTED = "mesh:workflow:started"
MESH_WORKFLOW_CLOSED = "mesh:workflow:closed"
MESH_WORKFLOW_FAILED = "mesh:workflow:failed"
MESH_STEP_FAILED = "mesh:step:failed"
MESH_EVENT_RAW = "mesh:event:raw"

# ── cockpit ───────────────────────────────────────────────────────
COCKPIT_OMOS_BROADCAST = "cockpit:omos:broadcast"
COCKPIT_SYSTEM_MAP = "cockpit:system_map"

# ── runtime cron ──────────────────────────────────────────────────
RUNTIME_CRON_FIRED = "runtime:cron:fired"

# ── omo (governance engine) ──────────────────────────────────────
OMO_AUDIT_COMPLETED = "omo:audit:completed"
OMO_AUDIT_FAILED = "omo:audit:failed"
OMO_HEALING_TRIGGERED = "omo:healing:triggered"
OMO_MESH_NODE_WRITTEN = "omo:mesh:node:written"
PATTERN_OMO_ALL = "omo:*"

# ── wildcard patterns (for facade.event.subscribe(...)) ──────────
# Note: the bus pattern matcher uses `*` as a STRING PREFIX, not glob.
# So `pipeline:*` matches "pipeline:completed" / "pipeline:started" etc.
PATTERN_PIPELINE_ALL = "pipeline:*"
PATTERN_DEBT_ALL = "debt:*"
PATTERN_SWARM_ALL = "swarm:*"
PATTERN_METAOS_ALL = "metaos:*"
PATTERN_KAIRON_ALL = "kairon:*"
PATTERN_GOVERNANCE_ALL = "governance:*"
PATTERN_SYSTEM_ALL = "system:*"
PATTERN_OMO_ALL = "omo:*"
PATTERN_ALL = "*"


__all__ = [
    # pipeline
    "PIPELINE_COMPLETED",
    "PIPELINE_STARTED",
    "PIPELINE_STEP_OK",
    "PIPELINE_FAILED",
    # governance
    "DEBT_CREATED",
    "DEBT_REVIEWED",
    "NODE_COMPLETED",
    "REGISTRY_SERVICE_REGISTERED",
    # system
    "SYSTEM_CRON_TICK",
    "SYSTEM_BUS_DLQ_PURGED",
    "SYSTEM_BUS_HEALTH",
    # kairon
    "KAIRON_SOURCE_INGESTED",
    "KAIRON_EXTRACTION_COMPLETED",
    "KAIRON_QUALITY_GATE_RESULT",
    "KAIRON_DOWNSTREAM_DISPATCHED",
    # metaos
    "METAOS_NODE_PENDING",
    "METAOS_NODE_RUNNING",
    "METAOS_NODE_COMPLETED",
    "METAOS_NODE_FAILED",
    "METAOS_NODE_TIMED_OUT",
    "METAOS_NODE_AWAITING_APPROVAL",
    "METAOS_APPROVAL_GRANTED",
    "METAOS_APPROVAL_DENIED",
    # aetherforge
    "SWARM_WORKER_HATCHED",
    "SWARM_WORKER_TERMINATED",
    "SWARM_HATCHER_INITIALIZED",
    "SWARM_HATCHER_SHUTDOWN",
    "SWARM_HATCHER_DRAIN_ALL",
    "SWARM_HATCHER_WORKER_CANCELLED",
    "SWARM_HATCHER_WORKER_RETRY",
    "SWARM_HATCHER_WORKER_DEAD_LETTER",
    "SWARM_AGENT_SEND",
    "SWARM_AGENT_RECEIVE",
    "SWARM_AGENT_ACK",
    "MESH_NODE_STATUS_CHANGED",
    "MESH_ROUTE",
    "SWARM_STEP",
    "LLM_CALL",
    # workflow mesh (resident event ingest)
    "MESH_WORKFLOW_REQUESTED",
    "MESH_WORKFLOW_STARTED",
    "MESH_WORKFLOW_CLOSED",
    "MESH_WORKFLOW_FAILED",
    "MESH_STEP_FAILED",
    "MESH_EVENT_RAW",
    # cockpit
    "COCKPIT_OMOS_BROADCAST",
    "COCKPIT_SYSTEM_MAP",
    # runtime
    "RUNTIME_CRON_FIRED",
    # omo
    "OMO_AUDIT_COMPLETED",
    "OMO_AUDIT_FAILED",
    "OMO_HEALING_TRIGGERED",
    "OMO_MESH_NODE_WRITTEN",
    # patterns
    "PATTERN_PIPELINE_ALL",
    "PATTERN_DEBT_ALL",
    "PATTERN_SWARM_ALL",
    "PATTERN_METAOS_ALL",
    "PATTERN_KAIRON_ALL",
    "PATTERN_GOVERNANCE_ALL",
    "PATTERN_SYSTEM_ALL",
    "PATTERN_OMO_ALL",
    "PATTERN_ALL",
]
