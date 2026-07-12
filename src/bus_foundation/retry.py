"""R90: Retry policy and circuit breaker for bus-foundation backends.

Public surface:
    from bus_foundation.retry import RetryPolicy, CircuitBreaker, with_retry

Per RETRY-OWNERSHIP.md: backend adapters DO NOT retry. This module
provides application-layer retry on top of the Router path, opt-in
per backend via `bus_foundation.retry.set_policy(backend, policy)`.

Default: disabled. Per-backend opt-in. data_plane and croniter are
explicitly no-retry (fire-and-forget / scheduled semantics).
"""

from __future__ import annotations

import logging
import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

from .metrics import Counter, Gauge, Registry

logger = logging.getLogger(__name__)

T = TypeVar("T")


# ── Retry policy ────────────────────────────────────────────────


@dataclass
class RetryPolicy:
    """Exponential backoff with jitter.

    backoff_n = min(initial_backoff * (multiplier ** (n-1)), max_backoff)
    jitter: ±jitter * backoff_n
    """

    max_attempts: int = 3
    initial_backoff: float = 0.1
    max_backoff: float = 5.0
    backoff_multiplier: float = 2.0
    jitter: float = 0.1
    retry_on: tuple[type[BaseException], ...] = (Exception,)

    def compute_backoff(self, attempt: int) -> float:
        if attempt < 1:
            return 0.0
        base = min(
            self.initial_backoff * (self.backoff_multiplier ** (attempt - 1)),
            self.max_backoff,
        )
        if self.jitter > 0:
            base = base * (1.0 + random.uniform(-self.jitter, self.jitter))
        return max(0.0, base)


# ── Circuit breaker ─────────────────────────────────────────────


class CircuitState:
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    """Classic 3-state circuit breaker.

    CLOSED → (failure_threshold failures) → OPEN
    OPEN → (recovery_timeout elapsed) → HALF_OPEN
    HALF_OPEN → (probe succeeds) → CLOSED
                (probe fails) → OPEN
    """

    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_max_calls: int = 1

    state: str = field(default=CircuitState.CLOSED)
    failure_count: int = 0
    last_failure_at: float = 0.0
    half_open_in_flight: int = 0

    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def allow(self) -> bool:
        """Check if a call is allowed under current state."""
        with self._lock:
            if self.state == CircuitState.CLOSED:
                return True
            if self.state == CircuitState.OPEN:
                if time.monotonic() - self.last_failure_at >= self.recovery_timeout:
                    self.state = CircuitState.HALF_OPEN
                    self.half_open_in_flight = 1
                    return True
                return False
            # HALF_OPEN
            if self.half_open_in_flight < self.half_open_max_calls:
                self.half_open_in_flight += 1
                return True
            return False

    def record_success(self) -> None:
        with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.half_open_in_flight = 0
            else:
                self.failure_count = 0

    def record_failure(self) -> None:
        with self._lock:
            self.failure_count += 1
            self.last_failure_at = time.monotonic()
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                self.half_open_in_flight = 0
            elif self.failure_count >= self.failure_threshold:
                self.state = CircuitState.OPEN

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "state": self.state,
                "failure_count": self.failure_count,
                "last_failure_at": self.last_failure_at,
            }


# ── Per-backend registry ────────────────────────────────────────


class RetryRegistry:
    """Per-backend policy + circuit breaker. Singleton."""

    _policies: dict[str, RetryPolicy] = {}
    _breakers: dict[str, CircuitBreaker] = {}
    _lock = threading.Lock()

    @classmethod
    def get_policy(cls, backend: str) -> RetryPolicy | None:
        with cls._lock:
            return cls._policies.get(backend)

    @classmethod
    def set_policy(cls, backend: str, policy: RetryPolicy) -> None:
        with cls._lock:
            cls._policies[backend] = policy
            cls._breakers.setdefault(backend, CircuitBreaker())

    @classmethod
    def get_breaker(cls, backend: str) -> CircuitBreaker | None:
        with cls._lock:
            return cls._breakers.get(backend)

    @classmethod
    def all(cls) -> dict[str, dict[str, Any]]:
        with cls._lock:
            return {
                name: {
                    "policy": p,
                    "breaker": cb.snapshot(),
                }
                for name, (p, cb) in zip(
                    cls._policies.keys(),
                    zip(cls._policies.values(), [cls._breakers[n] for n in cls._policies], strict=True),
                    strict=True,
                )
            }

    @classmethod
    def reset(cls) -> None:
        """Test helper."""
        with cls._lock:
            cls._policies = {}
            cls._breakers = {}


# ── Metrics ─────────────────────────────────────────────────────

RETRY_ATTEMPTED = Registry.register(Counter("bus_retry_attempted_total", labels=("backend", "outcome")))
CIRCUIT_BREAKER_STATE = Registry.register(Gauge("bus_circuit_breaker_state", labels=("backend",)))


def _publish_breaker_state(backend: str, cb: CircuitBreaker) -> None:
    if cb.state == CircuitState.CLOSED:
        CIRCUIT_BREAKER_STATE.set(0.0, backend=backend)
    elif cb.state == CircuitState.HALF_OPEN:
        CIRCUIT_BREAKER_STATE.set(1.0, backend=backend)
    else:
        CIRCUIT_BREAKER_STATE.set(2.0, backend=backend)


# ── with_retry helper ───────────────────────────────────────────


def with_retry[T](backend: str, func: Callable[[], T]) -> T:
    """Run `func` under the backend's retry policy + circuit breaker.

    Raises:
        CircuitOpenError: if breaker is OPEN.
        The last exception if all attempts fail.
    """
    policy = RetryRegistry.get_policy(backend)
    if policy is None:
        return func()

    breaker = RetryRegistry.get_breaker(backend) or CircuitBreaker()
    RetryRegistry._breakers[backend] = breaker

    if not breaker.allow():
        RETRY_ATTEMPTED.inc(backend=backend, outcome="circuit_open")
        raise CircuitOpenError(f"circuit breaker open for {backend}")

    last_exc: BaseException | None = None
    for attempt in range(1, policy.max_attempts + 1):
        try:
            result = func()
            breaker.record_success()
            _publish_breaker_state(backend, breaker)
            if attempt > 1:
                RETRY_ATTEMPTED.inc(backend=backend, outcome="success")
            return result
        except policy.retry_on as e:  # noqa: PERF203  # intentional retry
            last_exc = e
            breaker.record_failure()
            _publish_breaker_state(backend, breaker)
            if attempt < policy.max_attempts:
                backoff = policy.compute_backoff(attempt)
                logger.debug(
                    "bus_retry_attempt backend=%s attempt=%d/%d backoff=%.3fs error=%s",
                    backend,
                    attempt,
                    policy.max_attempts,
                    backoff,
                    e,
                )
                time.sleep(backoff)
                RETRY_ATTEMPTED.inc(backend=backend, outcome="retrying")
            else:
                RETRY_ATTEMPTED.inc(backend=backend, outcome="exhausted")
                logger.warning(
                    "bus_retry_exhausted backend=%s attempts=%d error=%s",
                    backend,
                    policy.max_attempts,
                    e,
                )

    assert last_exc is not None
    raise last_exc


class CircuitOpenError(RuntimeError):
    """Raised when the circuit breaker is open."""


__all__ = [
    "RetryPolicy",
    "CircuitBreaker",
    "CircuitState",
    "CircuitOpenError",
    "RetryRegistry",
    "with_retry",
]
