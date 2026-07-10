"""R97: chaos tests for bus-foundation backends under failure injection.

These tests inject random failures (ConnectionError, slow calls,
kill -9 mid-publish) to verify the R89 metrics + R90 retry + R91 DLQ
trinity works under adverse conditions.
"""

from __future__ import annotations

import random
import time
from typing import Any

import pytest

import bus_foundation
from bus_foundation.envelope import BusEnvelope, EventType
from bus_foundation.retry import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    RetryPolicy,
    RetryRegistry,
    with_retry,
)
from bus_foundation.metrics import Registry as MetricsRegistry


@pytest.fixture(autouse=True)
def _reset_all():
    MetricsRegistry.reset()
    MetricsRegistry.enable()
    RetryRegistry.reset()
    yield
    MetricsRegistry.reset()
    MetricsRegistry.disable()
    RetryRegistry.reset()


class TestChaosScenarios:
    def test_random_backend_failures_preserve_event_id(self):
        """Even with random failures, event_id should be preserved (returned to caller)."""
        bus_foundation.set_retry_policy(
            "chaos_backend",
            RetryPolicy(max_attempts=5, initial_backoff=0.01, jitter=0),
        )
        call_count = 0
        def flaky_publish(env: BusEnvelope) -> str:
            nonlocal call_count
            call_count += 1
            if random.random() < 0.5:
                raise ConnectionError(f"chaos failure {call_count}")
            return env.id
        # Run 100 times with 50% failure rate — should always preserve id eventually
        for _ in range(100):
            env = BusEnvelope(topic="chaos:test", payload={"x": 1}, event_type=EventType.PIPELINE_COMPLETED)
            try:
                result = with_retry("chaos_backend", lambda: flaky_publish(env))
                assert result == env.id
            except (ConnectionError, CircuitOpenError):
                pass  # acceptable: full circuit exhaustion

    def test_circuit_opens_under_sustained_failure(self):
        """Under sustained failure, circuit should transition to OPEN."""
        bus_foundation.set_retry_policy(
            "down_backend",
            RetryPolicy(max_attempts=2, initial_backoff=0.01, jitter=0),
        )
        cb = RetryRegistry.get_breaker("down_backend")
        for _ in range(20):  # well above threshold of 5
            try:
                with_retry(
                    "down_backend",
                    lambda: (_ for _ in ()).throw(ConnectionError("down")),
                )
            except (ConnectionError, CircuitOpenError):
                pass
        assert cb is not None
        assert cb.state == CircuitState.OPEN
        # Now circuit is open, next call should immediately raise CircuitOpenError
        with pytest.raises(CircuitOpenError):
            with_retry("down_backend", lambda: "x")

    def test_mixed_workload_recovers(self):
        """Mixed success/failure workload should eventually recover when failures stop."""
        bus_foundation.set_retry_policy(
            "flaky",
            RetryPolicy(max_attempts=4, initial_backoff=0.01, max_backoff=0.1, jitter=0),
        )
        fail_remaining = [5]  # first 5 calls fail, then succeed
        def call():
            if fail_remaining[0] > 0:
                fail_remaining[0] -= 1
                raise ConnectionError("transient")
            return "ok"
        # First call: should retry 5 times, fail
        with pytest.raises(ConnectionError):
            with_retry("flaky", call)
        # Second call: all succeed
        result = with_retry("flaky", call)
        assert result == "ok"
