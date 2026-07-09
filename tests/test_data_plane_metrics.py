"""Test DataPlaneBackend metrics (R84)."""

import time

from bus_foundation.backends.data_plane import DataPlaneBackend
from bus_foundation.envelope import OmniEnvelope


class TestDataPlaneMetrics:
    def test_metrics_after_publish(self) -> None:
        be = DataPlaneBackend(capacity=10, flush_interval=10.0)
        be.publish(OmniEnvelope(topic="m:1", source="bos://t"))
        m = be.metrics()
        assert m["publish_count"] == 1
        assert m["drop_count"] == 0
        assert m["drop_rate_pct"] == 0.0
        assert isinstance(m["buffer_usage"], float)
        assert m["drain_lag_ms"] >= 0
        be.close()

    def test_drop_count_increments(self) -> None:
        be = DataPlaneBackend(capacity=3, flush_interval=10.0)
        for _ in range(100):
            be.publish(OmniEnvelope(topic="m:1", source="bos://t", payload={"x": 0}))
        m = be.metrics()
        assert m["publish_count"] == 100
        assert m["drop_count"] > 0
        assert m["drop_rate_pct"] > 0
        be.close()

    def test_metrics_after_flush(self) -> None:
        be = DataPlaneBackend(capacity=100, flush_interval=0.1)
        for _ in range(5):
            be.publish(OmniEnvelope(topic="m:2", source="bos://t"))
        time.sleep(0.3)
        m = be.metrics()
        assert m["flush_count"] >= 1
        assert m["buffer_usage"] < 0.5
        be.close()
