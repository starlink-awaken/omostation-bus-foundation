"""R89: tests for metrics abstraction + instrumentation + HTTP server."""

from __future__ import annotations

import urllib.request

import pytest

import bus_foundation
from bus_foundation.envelope import BusEnvelope, EventType
from bus_foundation.metrics import Counter, Gauge, Histogram, Registry
from bus_foundation.metrics_instrumentation import (
    PUBLISH_TOTAL,
    instrument_publish,
    record_backend_health,
    record_dlq_depth,
    record_subscribe,
)


@pytest.fixture(autouse=True)
def _reset_metrics():
    Registry.reset()
    Registry.disable()
    yield
    Registry.reset()
    Registry.disable()


class TestMetricsAbstraction:
    def test_counter_default_zero(self):
        Registry.enable()
        c = Counter("test_counter", labels=("a",))
        assert c.value(a="x") == 0.0

    def test_counter_inc(self):
        Registry.enable()
        c = Counter("test_counter", labels=("a",))
        c.inc(a="x")
        c.inc(amount=2.0, a="x")
        assert c.value(a="x") == 3.0

    def test_gauge_set_inc_dec(self):
        Registry.enable()
        g = Gauge("test_gauge", labels=("a",))
        g.set(5.0, a="x")
        assert g.value(a="x") == 5.0
        g.inc(a="x")
        assert g.value(a="x") == 6.0
        g.dec(amount=2.0, a="x")
        assert g.value(a="x") == 4.0

    def test_histogram_observe(self):
        Registry.enable()
        h = Histogram("test_hist", labels=())
        h.observe(0.001)
        h.observe(0.5)
        snap = h.snapshot()
        assert len(snap) == 1
        assert snap[()]["count"] == 2
        assert snap[()]["sum"] == pytest.approx(0.501, abs=1e-6)

    def test_no_op_when_disabled(self):
        c = Counter("test_noop", labels=())
        c.inc()
        assert c.value() == 0.0

    def test_registry_enable_disable(self):
        assert not Registry.is_enabled()
        Registry.enable()
        assert Registry.is_enabled()
        c = Counter("test_reg", labels=())
        c.inc()
        assert c.value() == 1.0
        Registry.disable()
        c.inc()
        assert c.value() == 1.0  # no-op when disabled


class TestInstrumentation:
    def test_instrument_publish_success(self):
        Registry.enable()
        with instrument_publish("test_backend") as ctx:
            ctx["status"] = "success"
        assert PUBLISH_TOTAL.value(backend="test_backend", status="success") == 1.0

    def test_instrument_publish_fail(self):
        Registry.enable()
        with instrument_publish("test_backend") as _:
            pass  # no status set → defaults to "fail"
        assert PUBLISH_TOTAL.value(backend="test_backend", status="fail") == 1.0

    def test_record_helpers(self):
        Registry.enable()
        record_subscribe("eventbus", "test:*", 5)
        record_dlq_depth("eventbus", "PENDING", 12)
        record_backend_health("eventbus", True)
        snapshot = bus_foundation.metrics_snapshot()
        assert "bus_subscribe_active" in snapshot
        assert "bus_dlq_depth" in snapshot
        assert "bus_backend_healthy" in snapshot


class TestEndToEndPublish:
    def test_publish_increments_metrics(self):
        """Verify that bus.publish() updates publish_total + publish_latency."""
        Registry.enable()
        bus_foundation.enable_metrics(server=False)
        env = BusEnvelope(topic="test:r89", payload={"k": 1}, event_type=EventType.PIPELINE_COMPLETED)  # type: ignore[reportCallIssue]
        bus_foundation.publish(env)
        assert PUBLISH_TOTAL.value(backend="eventbus", status="success") >= 1.0


class TestHTTPServer:
    def test_metrics_endpoint(self):
        bus_foundation.enable_metrics(port=18745, server=True)
        Registry.enable()
        record_backend_health("eventbus", True)
        try:
            resp = urllib.request.urlopen("http://127.0.0.1:18745/metrics", timeout=2.0)
            assert resp.status == 200
            body = resp.read().decode("utf-8")
            assert "bus_backend_healthy" in body
        finally:
            bus_foundation.disable_metrics()

    def test_404_for_unknown_path(self):
        bus_foundation.enable_metrics(port=18746, server=True)
        try:
            try:
                urllib.request.urlopen("http://127.0.0.1:18746/health", timeout=2.0)
            except urllib.error.HTTPError as e:  # type: ignore[reportAttributeAccessIssue]
                assert e.code == 404
        finally:
            bus_foundation.disable_metrics()
