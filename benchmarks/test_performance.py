"""Performance benchmarks for bus-foundation backends.

Run with: pytest benchmarks/ --benchmark-only --benchmark-json output.json

Results are recorded relative to the host machine and will vary.
The benchmarks serve as a regression baseline, not an absolute
performance guarantee.

Baseline env: macOS, Python 3.13, M3 Pro, 2026-07-09
"""

from __future__ import annotations

import pytest

from bus_foundation.backends.asyncio import AsyncioBackend
from bus_foundation.backends.eventbus import EventBusBackend
from bus_foundation.backends.persistent_bus import PersistentBusBackend
from bus_foundation.envelope import BusEnvelope

N_SMALL = 10000
N_LARGE = 500


def _env(n: int = 0) -> BusEnvelope:
    return BusEnvelope(
        id=f"bench-{n:06d}",
        topic="bench:test",
        source="bos://benchmark",
        payload={"n": n, "data": "x" * 100},
    )


# ── EventBus Backend ──────────────────────────────────────────────


class TestEventBusBench:
    @pytest.mark.benchmark(min_rounds=5, warmup=True)
    def test_publish_throughput(self, benchmark) -> None:
        be = EventBusBackend()
        # Subscribe a no-op handler to simulate real load
        be.subscribe("*", lambda env: None)

        def _run():
            for i in range(N_SMALL):
                be.publish(_env(i))

        benchmark(_run)

    @pytest.mark.benchmark(min_rounds=5, warmup=True)
    def test_subscribe_and_publish(self, benchmark) -> None:
        be = EventBusBackend()
        results: list[tuple] = []
        be.subscribe("bench:*", lambda env: results.append((env.id, env.topic)))

        def _run():
            results.clear()
            for i in range(N_SMALL):
                be.publish(_env(i))
            return len(results)

        count = benchmark(_run)
        assert count == N_SMALL, f"expected {N_SMALL} deliveries, got {count}"


# ── PersistentBus Backend ─────────────────────────────────────────


class TestPersistentBusBench:
    @pytest.mark.benchmark(min_rounds=3, warmup=True)
    def test_publish_throughput(self, benchmark, tmp_path) -> None:
        be = PersistentBusBackend(db_path=tmp_path / "bench.db")
        be.subscribe("*", lambda env: None)

        def _run():
            for i in range(N_LARGE):
                be.publish(_env(i))

        benchmark(_run)
        be.close()

    @pytest.mark.benchmark(min_rounds=3, warmup=True)
    def test_get_recent(self, benchmark, tmp_path) -> None:
        be = PersistentBusBackend(db_path=tmp_path / "recent.db")
        for i in range(N_LARGE):
            be.publish(_env(i))
        be.close()

        # Benchmark query only (fresh connection)
        def _run():
            b2 = PersistentBusBackend(db_path=tmp_path / "recent.db")
            b2.get_recent(limit=100)
            b2.close()

        benchmark(_run)


# ── Asyncio Backend ───────────────────────────────────────────────


class TestAsyncioBench:
    @pytest.mark.benchmark(min_rounds=5, warmup=True)
    def test_publish_to_in_process_subscriber(self, benchmark) -> None:
        be = AsyncioBackend()
        results: list[int] = []
        be.subscribe("*", lambda env: results.append(1))

        def _run():
            results.clear()
            for i in range(N_SMALL):
                be.publish(_env(i))

        benchmark(_run)

    @pytest.mark.asyncio
    @pytest.mark.benchmark(min_rounds=5, warmup=True)
    async def test_async_subscribe_and_deliver(self, benchmark) -> None:
        be = AsyncioBackend()
        results: list[str] = []

        def cb(env):
            results.append(env.id)

        be.subscribe("*", cb)
        import asyncio

        await asyncio.sleep(0.1)  # let the drain task start

        def _run():
            for i in range(1000):
                be.publish(_env(i))

        benchmark(_run)
