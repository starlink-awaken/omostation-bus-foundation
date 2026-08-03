# pyright: reportCallIssue=false

"""Tests for bus_foundation.__init__ error paths (publish, enable_metrics, schedule).

These test module-level singletons via monkeypatching.
"""

from __future__ import annotations

import pytest

import bus_foundation
from bus_foundation import (
    BusEnvelope,
    disable_metrics,
    enable_metrics,
    publish,
    schedule,
    set_retry_policy,
)
from bus_foundation.retry import RetryPolicy


class TestEnableMetrics:
    def test_enable_metrics_with_env_port(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """enable_metrics reads BUS_METRICS_PORT from env."""
        monkeypatch.setenv("BUS_METRICS_PORT", "18745")
        enable_metrics(server=False)
        # Should not raise
        disable_metrics()

    def test_enable_metrics_without_port_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """enable_metrics raises ValueError when no port is set."""
        monkeypatch.delenv("BUS_METRICS_PORT", raising=False)
        with pytest.raises(ValueError, match="port"):
            enable_metrics()


class TestPublishErrorPaths:
    """Test publish() error handling paths via module-level singletons."""

    def test_publish_backend_unavailable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When backend.is_available() is False, publish DLQs the event."""
        from bus_foundation import _backends

        class UnavailableBackend:
            name = "test_unavail"

            def is_available(self) -> bool:
                return False

            def publish(self, envelope: object) -> str:
                return ""

            def subscribe(self, *args: object) -> str:
                return ""

            def unsubscribe(self, *args: object) -> bool:
                return False

        monkeypatch.setitem(_backends, "test_unavail", UnavailableBackend())
        env = BusEnvelope(type="test:event", source="ut", backend="test_unavail")
        result = publish(env)
        assert result == env.id

    def test_publish_circuit_open(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When circuit breaker is OPEN, publish DLQs the event."""
        from bus_foundation import _backends

        class OpenCircuitBackend:
            name = "test_circuit"

            def is_available(self) -> bool:
                return True

            def publish(self, envelope: object) -> str:
                from bus_foundation.retry import CircuitOpenError

                raise CircuitOpenError("test_circuit")

            def subscribe(self, *args: object) -> str:
                return ""

            def unsubscribe(self, *args: object) -> bool:
                return False

        monkeypatch.setitem(_backends, "test_circuit", OpenCircuitBackend())
        set_retry_policy("test_circuit", RetryPolicy(max_attempts=1))
        env = BusEnvelope(type="test:event", source="ut", backend="test_circuit")
        result = publish(env)
        assert result == env.id

    def test_publish_generic_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When backend.publish() raises, event goes to DLQ."""
        from bus_foundation import _backends

        class FailingBackend:
            name = "test_fail"

            def is_available(self) -> bool:
                return True

            def publish(self, envelope: object) -> str:
                raise RuntimeError("simulated failure")

            def subscribe(self, *args: object) -> str:
                return ""

            def unsubscribe(self, *args: object) -> bool:
                return False

        monkeypatch.setitem(_backends, "test_fail", FailingBackend())
        env = BusEnvelope(type="test:event", source="ut", backend="test_fail")
        result = publish(env)
        assert result == env.id


class TestSchedule:
    def test_schedule_invalid_type_raises(self) -> None:
        with pytest.raises(TypeError):
            schedule(123)  # type: ignore[arg-type]


class TestVersion:
    def test_version_is_string(self) -> None:
        assert isinstance(bus_foundation.__version__, str)
        assert bus_foundation.__version__ == "0.3.0"
