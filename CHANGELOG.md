# Changelog — bus-foundation

All notable changes to this project are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/).

## [0.3.0] — 2026-07-09 (R89-R97 modernization)

### Added

- **R89 Metrics & Health Endpoints** — 5 standard metrics
  (`bus_publish_total`, `bus_publish_latency_seconds`,
  `bus_subscribe_active`, `bus_dlq_depth`, `bus_backend_healthy`)
  + Prometheus-compatible `/metrics` HTTP server. Default OFF,
  zero overhead. `protocols/metrics-registry.yaml` SSOT.
- **R90 Retry Middleware & Circuit Breaker** — per-backend
  opt-in retry with exponential backoff + jitter, 3-state
  circuit breaker. `set_retry_policy(backend, policy)` API.
- **R91 DLQ Admin CLI** — `bin/bus-dlq` with `list / stats / ack /
  retry / purge / watch` sub-commands. Plus payload redaction
  (password / secret / token / Luhn-CC / email / IPv4) and
  `bus_dlq_redacted_total` metric.
- **R92 Schema Registry** — `bus_foundation.validate_envelope(env,
  target_version="v2")` with `strict / additive / breaking`
  compatibility modes. The `schema_version` field (declared in
  R82) is now a first-class concept.
- **R94 Trace Context Propagation** — pure-stdlib W3C-style
  trace_id + span_id propagation. Auto-upgrades to opentelemetry-api
  when `[otel]` extra is installed. `bus.publish()` automatically
  attaches the caller's trace_id to the envelope.
- **R96 5-Segment Cron + Timezone** — croniter backend now
  accepts standard cron expressions (`* * * * *`,
  `0 9 * * 1`, `*/5 * * * *`) with timezone support via
  `zoneinfo`. R79 `every Nm/Nh` syntax preserved.
- **R97 Property-Based + Chaos Testing** — `hypothesis>=6.0`
  test framework integrated. 11 new tests covering envelope
  round-trip, pattern match edge cases, DLQ redaction
  invariants, and chaos scenarios (random failures,
  circuit breaker transitions, mixed workload recovery).

### Internal

- `_jobs` tuple schema changed from 3-tuple to 4-tuple in
  croniter to support both interval and cron-style schedules.
- `bus.publish()` now wrapped in `instrument_publish()` for
  transparent metrics capture (no-op when disabled).
- DLQ enqueue transparently applies redaction before persisting.
- New modules: `bus_foundation.metrics`, `.metrics_instrumentation`,
  `.metrics_server`, `.retry`, `.dlq_admin`, `.dlq_redaction`,
  `.schema`, `.observability`.

### Test Results

- 218/223 total tests PASS (5 skipped)
- 87 new test cases across 7 rounds
- Zero backward-compatibility breakage: all R73-R88 functionality
  preserved unchanged

### Public API Additions

```python
# R89
bus_foundation.enable_metrics(port=8745)
bus_foundation.disable_metrics()
bus_foundation.metrics_snapshot()

# R90
bus_foundation.set_retry_policy(backend, policy)
bus_foundation.retry_stats()
bus_foundation.RetryPolicy / CircuitBreaker / CircuitState

# R91 — see bin/bus-dlq

# R92
bus_foundation.validate_envelope(env, target_version="v2")

# R94
bus_foundation.trace("op_name", backend=...)
bus_foundation.get_current_trace_id()
```

### Out of Scope (deferred to 0.4.0+)

- R93 Distributed backends (Redis / NATS)
- R95 WebSocket protocol v2
- R98 Performance SLA + benchmark trend tracking

## [0.2.0] — 2026-07-09 (R82 pydantic envelope)

## [0.1.0] — 2027-04-12 (R66 initial release)

### Added (R66-R67: Phase B setup)

- Standalone repo split from `agora/bus/` (per ADR-0008)
- `BusEnvelope` (Pydantic-lite wire format)
- `match_pattern` shared helper (R74 dedup; extracted early)
- 5 backends (initial set, all from `agora/bus/`):
  - `EventBusBackend` — in-process pub/sub
  - `AsyncioBackend` — in-process pub/sub via `asyncio.Queue`
  - `CroniterBackend` — cron-style scheduling
  - `MessageBusBackend` — agent-to-agent pub/sub with req/resp correlation
  - `SSEBackend` — in-process fan-out for HTTP/SSE layer
- 28 tests (initial release baseline)
- Cross-repo smoke test for Phase B GO gate
- `OWNERS.md`, `GOVERNANCE.md`, monthly 5-hard-conditions check script
- Public API frozen for 6 months from 2026-06-12

### Changed (R73-R75: feature + simplify + style)

- **R73** — Added 3 new backends:
  - `WebSocketBackend` — full-duplex fanout for browser clients
  - `RealtimeBackend` — versioned per-task event stream
  - `PersistentBusBackend` — SQLite-backed durable pub/sub
- **R73** — R73 code review + 3 fixes (HIGH unsubscribe, MEDIUM drain leak, MEDIUM `__init__.py` exports)
- **R74** — 4 of 5 LOW review fixes applied:
  - `time.monotonic()` for TTL
  - `match_pattern` helper extracted; 1 backend pilot
  - 2 docstring clarifications
- **R74 simplify** — 5 backends delegate to `match_pattern` (dedup 6 copies, -15 net LOC)
- **R75** — `ruff --fix` collections.abc (UP035) + import sort (I001); 17+2 auto-fixes
- **R75** — `GOVERNANCE.md` + `CLAUDE.md` updated to reflect R73/R74/R75 state
- **R75** — Phase C decision (R72 Path C: Defer) documented in `GOVERNANCE.md`

### Verified invariants (R75)

- **Single file < 500 LOC**: max 130 (persistent bus.py)
- **Zero `from agora` imports** in `src/bus_foundation/`: 0 (cross-repo purity)
- **No retry at backend** (per RETRY-OWNERSHIP.md): 0
- **56 tests, 100% pass** (R75)
- **ruff**: 0 errors (after R75 auto-fix)


### R78 (governance propagation, no code change)

- **X4 added to charter**: `projects/AGENTS.md` and
  `.omo/_truth/governance-charter-v1.md` §1.2 now lists all 4
  cross-cutting concerns (X1 审计, X2 抗熵, X3 价值栈, X4 一致性).
  X4 was previously in `LAYER-INDEX.md:114` but missing from the
  charter table. Caught during R77 governance audit propagation.
- **L0 decision propagated to `projects/AGENTS.md`**: the "bus-foundation
  defer L0" status (R72) is now explicitly cross-referenced in the
  workspace root. Future maintainers can find the rationale via
  `projects/bus-foundation/docs/ADR-0003-no-l0-promotion.md`.
- **5 consumer AGENTS.md updated**: omo / metaos / runtime / kairon /
  llm-gateway now have a `## Bus foundation` section pointing at
  the adapter file and the import pattern. hermes-console (TS HTTP
  adapter) intentionally skipped — TS adapter imports differ.

## ## Backlog (deferred to normal feature work)

- **R75-LOW-1**: PersistentBusBackend.publish() runs `_cleanup_subs`
  on every publish. For high-subscriber counts (>100) this is O(N)
  per event. Switch to a separate reaper thread or rate-limited cleanup.
- **R75-LOW-2**: WebSocketBackend and PersistentBusBackend publish()
  are O(N) over subscribers. For >1000 subscribers, add a trie or
  sharded dispatch table.
- **R75-ADR-0003**: Document the "no L0 promotion" decision formally
  as an ADR (so the next maintainer doesn't have to read the
  18-month history to understand why).
- **R75-rerun**: Re-verify 0 ruff warnings after the auto-fix batch.

## References

- Architecture analysis: `.omo/_delivery/async-event-cron-architecture-2026-06-12.md`
- Plan: `docs/superpowers/plans/2026-06-12-bus-unification.md`
- ADR-0008 (Phase B trigger): `../agora/docs/ADR-0008-bus-foundation-strategy.md`
- ADR-0008.1 (Phase B Condition 4 proxy): `../agora/docs/ADR-0008.1-condition-4-amendment.md`
- 14-month retrospective: `.omo/_delivery/r72-final-retrospective-2027-09-12.md`
- R73 code review: `.omo/_delivery/r73-code-review.md`
- R74 evidence: `.omo/_delivery/r74-monthly-evidence-2026-06-13.md`
- Path C decision: `.omo/_delivery/r71-phase-c-recommendation-memo.md`

## [0.2.0] — 2026-07-09 (R79-R88: durability, control plane, DLQ, WS transport)

### Added
- `PersistentBusBackend` — SQLite-WAL-backed durable pub/sub with FIFO GC
- `ControlPlaneBackend` — SQLite-backed task execution with ACK/NACK/DLQ
- `DLQ` — Dead Letter Queue with SQLite storage and redaction
- `WebSocketBackend` (v1) — Real WebSocket transport via `websockets` library
- `DataPlaneBackend` — Ring-buffer-backed high-throughput data plane
- `SqliteBase` — Shared SQLite infrastructure (WAL, locking, GC)
- Router with per-backend DLQ routing
- 5 new backend adapters (ws, sse, realtime, persistent, control)
- R75 rate-limit cleanup mechanism

### Changed
- BusEnvelope migrated to Pydantic BaseModel (R82)
- Facade layer restructured into data/event/control planes
- Backend protocol formalized via `BusBackend` Protocol class
- Pattern matching deduplicated into shared `pattern_match.py`

### Fixed
- R79: subscribe() now works on ControlPlaneBackend (was stubbed)
- R75-LOW-1: rate-limit cleanup O(N) → O(N/100) per publish

## [0.3.0] — 2026-07-12 (R89-R97: metrics, retry, schema, OTel, Redis)

### Added
- `MetricsRegistry` — Pure-stdlib Counter/Histogram/Gauge (R89)
- `MetricsHTTPServer` — Pure-stdlib HTTP `/metrics` endpoint (R89)
- `RetryPolicy` + `CircuitBreaker` — Application-layer retry (R90)
- `DLQAdmin` — DLQ management CLI (`bus-dlq list|stats|purge|ack|retry`)
- `Redactor` + `RedactionConfig` — PII/credential redaction in DLQ (R91)
- `SchemaRegistry` — Envelope version compatibility (R92, 3 modes)
- `RedisBackend` — Distributed pub/sub via Redis (R93)
- `WebSocketBackendV2` — Protocol versioning, compression, heartbeat (R95)
- `observability` — Pure-stdlib W3C trace context with optional OTel upgrade (R94)
- 5-segment cron parser with timezone support (R96)
- Property-based tests (hypothesis) and chaos tests (R97)
- 14 performance benchmarks (µs → ms range)
- `docs/PERFORMANCE-SLA.md` — Official throughput/latency targets
- `docs/grafana-dashboard.json` — 8-panel metrics dashboard

### Changed
- Project version: 172 tests → 300+ tests
- Coverage: 65% → 83% (with targeted test expansions)
- All `zip()` calls use `strict=True` (R97 safety)
- `CompatibilityMode` uses `enum.StrEnum` (PEP 663)
- `Registry.register()` preserves concrete type via TypeVar
- FallbackLogger.critical uses `Optional` type annotation (PEP 484)
- Code quality: ruff 42 errors → 0, ruff format compliant

### Fixed
- ws_v2.py: missing `Any`/`threading`/`importlib` imports (3 runtime crashes)
- ws_v2.py: `PerMessageDeflate` → `ServerPerMessageDeflateFactory` (websockets 16 API)
- ws_v2.py: `stop_server` loop.stop → sentinel future cancel (RuntimeError)
- ws_v2.py: `_drain` CancelledError re-raise removed (RuntimeWarning)
- `__init__.py`: missing `trace`/`get_current_trace_id` imports
- `redis.py`: unused variable `redis` removed
- `observability.py`: unused `Status`/`StatusCode` imports removed
- `with_retry`: PEP 695 generic function syntax
