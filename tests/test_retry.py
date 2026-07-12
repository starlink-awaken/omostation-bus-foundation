"""R90: tests for retry policy + circuit breaker + middleware."""

from __future__ import annotations

import time

import pytest

import bus_foundation
from bus_foundation.retry import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    RetryPolicy,
    RetryRegistry,
    with_retry,
)


@pytest.fixture(autouse=True)
def _reset_retry():
    RetryRegistry.reset()
    bus_foundation._MetricsRegistry.reset()
    yield
    RetryRegistry.reset()
    bus_foundation._MetricsRegistry.reset()


class TestRetryPolicy:
    def test_backoff_exponential(self):
        p = RetryPolicy(initial_backoff=0.1, backoff_multiplier=2.0, jitter=0)
        assert p.compute_backoff(1) == 0.1
        assert p.compute_backoff(2) == 0.2
        assert p.compute_backoff(3) == 0.4
        assert p.compute_backoff(4) == 0.8

    def test_backoff_capped(self):
        p = RetryPolicy(
            initial_backoff=0.1,
            max_backoff=0.5,
            backoff_multiplier=2.0,
            jitter=0,
        )
        assert p.compute_backoff(5) == 0.5  # would be 1.6, capped

    def test_backoff_jitter_range(self):
        p = RetryPolicy(initial_backoff=1.0, jitter=0.5)
        for _ in range(100):
            v = p.compute_backoff(1)
            assert 0.5 <= v <= 1.5


class TestCircuitBreaker:
    def test_initial_closed(self):
        cb = CircuitBreaker()
        assert cb.state == CircuitState.CLOSED
        assert cb.allow()

    def test_open_after_threshold(self):
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=10.0)
        for _ in range(3):
            cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert not cb.allow()

    def test_half_open_after_recovery(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.05)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        time.sleep(0.06)
        assert cb.allow()  # transitions to HALF_OPEN
        assert cb.state == CircuitState.HALF_OPEN

    def test_half_open_success_closes(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.05)
        cb.record_failure()
        cb.record_failure()
        time.sleep(0.06)
        cb.allow()  # → HALF_OPEN
        cb.record_success()
        assert cb.state == CircuitState.CLOSED

    def test_half_open_failure_reopens(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.05)
        cb.record_failure()
        cb.record_failure()
        time.sleep(0.06)
        cb.allow()  # → HALF_OPEN
        cb.record_failure()
        assert cb.state == CircuitState.OPEN


class TestWithRetry:
    def test_no_policy_passthrough(self):
        call_count = 0

        def func():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = with_retry("test_backend", func)
        assert result == "ok"
        assert call_count == 1

    def test_retry_then_succeed(self):
        bus_foundation.set_retry_policy(
            "test_backend",
            RetryPolicy(max_attempts=3, initial_backoff=0.01, jitter=0),
        )
        call_count = 0

        def func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("transient")
            return "recovered"

        result = with_retry("test_backend", func)
        assert result == "recovered"
        assert call_count == 3

    def test_retry_exhausted(self):
        bus_foundation.set_retry_policy(
            "test_backend",
            RetryPolicy(max_attempts=2, initial_backoff=0.01, jitter=0),
        )

        def func():
            raise ConnectionError("never")

        with pytest.raises(ConnectionError):
            with_retry("test_backend", func)

    def test_circuit_open_raises(self):
        bus_foundation.set_retry_policy(
            "test_backend",
            RetryPolicy(max_attempts=1, initial_backoff=0.01, jitter=0),
        )
        # Trigger breaker open via repeated failures
        for _ in range(6):  # default threshold is 5
            try:
                with_retry("test_backend", lambda: (_ for _ in ()).throw(ConnectionError("x")))
            except (ConnectionError, CircuitOpenError):
                pass
        # Now circuit should be open
        with pytest.raises(CircuitOpenError):
            with_retry("test_backend", lambda: "x")

    def test_non_retryable_exception_no_retry(self):
        bus_foundation.set_retry_policy(
            "test_backend",
            RetryPolicy(max_attempts=3, initial_backoff=0.01, retry_on=(ValueError,)),
        )
        call_count = 0

        def func():
            nonlocal call_count
            call_count += 1
            raise TypeError("not in retry_on")

        with pytest.raises(TypeError):
            with_retry("test_backend", func)
        assert call_count == 1  # no retry


class TestPublicAPI:
    def test_set_retry_policy_and_stats(self):
        bus_foundation.set_retry_policy(
            "my_backend",
            RetryPolicy(max_attempts=5),
        )
        stats = bus_foundation.retry_stats()
        assert "my_backend" in stats
        assert stats["my_backend"]["policy"].max_attempts == 5
