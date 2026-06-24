# AGENTS.md — bus-foundation developer guide

> R78 update. Upgraded to Omni-Bus v2.

## Quickstart

```bash
uv sync
uv run pytest -q  # 59 tests (R76)
```

## Key files (R78 truth)

| File | LOC | Purpose |
|------|-----|---------|
| `src/bus_foundation/__init__.py` | ~120 | Legacy facade (publish/subscribe/schedule) |
| `src/bus_foundation/facade/data.py` | ~20 | **R78** Omni-Bus Data Plane Facade |
| `src/bus_foundation/facade/event.py` | ~20 | **R78** Omni-Bus Event Plane Facade |
| `src/bus_foundation/facade/control.py` | ~40 | **R78** Omni-Bus Control Plane Facade |
| `src/bus_foundation/envelope.py` | ~100 | OmniEnvelope wire format (DATA, EVENT, CONTROL) |
| `src/bus_foundation/dlq.py` | ~135 | SQLite DLQ (WAL + GC) for failed control tasks |
| `src/bus_foundation/backends/data_plane.py` | ~80 | **R78** Async Ring Buffer with drop-tail logic |
| `src/bus_foundation/backends/control_plane.py`| ~100 | **R78** SQLite WAL Task Store + ACK/NACK mechanics |

## Tests (R76 = 59 cases)

| File | Tests | Coverage |
|------|-------|----------|
| `test_envelope.py` | 3 | BusEnvelope construction + validation |
| `test_dlq.py` | 5 | DLQ persistence + GC + rate-limit |
| `test_eventbus_backend.py` | 4 | in-process pubsub |
| `test_asyncio_backend.py` | 4 | asyncio.Queue pubsub |
| `test_croniter_backend.py` | (basic) | cron scheduling |
| `test_messagebus_backend.py` | (basic) | agent pub/sub |
| `test_sse_backend.py` | (basic) | in-process fan-out |
| `test_ws_backend.py` | 4 | R73 full-duplex fanout |
| `test_realtime_backend.py` | 4 | R73 versioned task events |
| `test_persistent_bus_backend.py` | 5 | R73 SQLite durable pubsub |
| `test_persistent_bus_ratelimit.py` | 3 | **R76** rate-limit cleanup |
| `test_pattern_match.py` | 4 | **R74** shared helper |
| `test_realtime_unsubscribe.py` | 3 | **R73** unsubscribe fix |
| `test_facade.py` | (basic) | publish/subscribe/schedule facade |
| `test_router_retry_ownership.py` | (basic) | Router + DLQ fallback |
| `test_cross_repo_smoke.py` | (R66) | Phase B GO gate |

## Gotchas (R78)

- **OMNI-BUS ARCHITECTURE**: All new development MUST align with the Omni-Bus plane semantics:
    - `bus.data` for fast, non-blocking telemetry and logs.
    - `bus.event` for domain fan-out.
    - `bus.control` for durable execution requiring ACK/NACK.
- **RETRY**: Control Plane handles retries automatically when tasks are `NACK`ed.
- **DLQ**: Control Plane tasks failing repeatedly are sent to DLQ `~/.runtime/bus_dlq.db`.
- **Zero agora dependency**: `grep -r "from agora" src/bus_foundation/` must return 0
- **Use `collections.abc`**: `from collections.abc import Callable`, not `from typing`

## Adding a new backend

1. Create `src/bus_foundation/backends/mybackend.py` implementing the
   `BusBackend` Protocol (4 methods: `name`, `is_available`, `publish`,
   `subscribe`, `unsubscribe`).
2. Register in `src/bus_foundation/backends/__init__.py`.
3. If dispatchable by name via `envelope.backend`, register in
   `src/bus_foundation/__init__.py` `_backends` dict.
4. Add tests in `tests/test_mybackend.py` (5+ cases: protocol,
   is_available, publish, subscribe dispatch, unsubscribe).
5. **Update `src/bus_foundation/README.md` backend selection table.**
6. Run `uv run pytest -q` and verify 0 regressions.

## Release

- **Current version**: 0.2.0 (R78 - Omni-Bus)
- **Public API frozen** since R66 (2026-06-12). No breaking changes
  to BusEnvelope for legacy systems. All new Omni-Bus fields (plane, trace_id) have safe defaults.
- **Bump version** in `pyproject.toml` for any release.
- **Breaking changes** require an ADR (see `docs/ADR-0003-no-l0-promotion.md`
  for the L0 re-opening conditions, and `docs/ADR-0002-phase-c-trigger-reality-DRAFT.md`
  for context).

## When to update this file

Update AGENTS.md when:
- Adding/removing a backend
- Adding/removing a public function in `__init__.py`
- Changing the public API contract
- Bumping the version

Do NOT update AGENTS.md for:
- Internal refactors (CHANGELOG.md is enough)
- Test additions (CHANGELOG.md is enough)
- New shared helpers (CHANGELOG.md is enough)

## Workspace-Wide Governance (2026-06-24)

This project follows the workspace-level governance conventions documented in the root `AGENTS.md`:

- **Agent Mutation Protocol**: Any autonomous agent/cron/daemon that modifies workspace state must emit `agent_mutation_intent`, avoid direct file I/O to `.omo/`/`spaces/`, and commit immediately. See `.omo/standards/agent-mutation-protocol.md` for the full protocol.
- **SSOT Guardian**: Run `python3 bin/ssot-guardian.py` from the workspace root before committing to detect task-count, current-wave, submodule-pointer, or direct-omo-io drift.
- **direct-omo-io**: Scripts must route writes to `.omo/` through `omo CLI`, `projects/omo` core, or `projects/c2g` ingress — never via raw `open()/mkdir()/write_text()`.
- **Submodule Governance**: Commit changes inside the submodule first, then bump the root-repo pointer; `git submodule status` with a `+` prefix indicates pending drift.
