"""R94: tests for trace context propagation."""

from __future__ import annotations

import bus_foundation
from bus_foundation.envelope import BusEnvelope, EventType
from bus_foundation.observability import (
    Span,
    attach_to_envelope,
    get_current_span_id,
    get_current_trace_id,
    new_span_id,
    new_trace_id,
    trace,
)


class TestSpanBasics:
    def test_new_trace_id_format(self):
        tid = new_trace_id()
        assert isinstance(tid, str)
        assert len(tid) == 32
        int(tid, 16)  # valid hex

    def test_new_span_id_format(self):
        sid = new_span_id()
        assert isinstance(sid, str)
        assert len(sid) == 16
        int(sid, 16)

    def test_unique_ids(self):
        ids = {new_trace_id() for _ in range(100)}
        assert len(ids) == 100


class TestTraceContext:
    def test_trace_yields_span(self):
        with trace("test_op") as span:
            assert isinstance(span, Span)
            assert span.name == "test_op"
            assert span.trace_id is not None
            assert span.span_id is not None

    def test_trace_sets_current_context(self):
        assert get_current_trace_id() is None
        with trace("outer") as span:
            assert get_current_trace_id() == span.trace_id
            assert get_current_span_id() == span.span_id
        assert get_current_trace_id() is None

    def test_nested_trace_inherits_trace_id(self):
        with trace("outer") as outer:
            with trace("inner") as inner:
                assert inner.trace_id == outer.trace_id
                assert inner.parent_span_id == outer.span_id

    def test_trace_restores_context_on_exit(self):
        with trace("outer") as outer:
            outer_tid = outer.trace_id
            with trace("inner"):
                pass
            # Back to outer's context
            assert get_current_trace_id() == outer_tid

    def test_trace_with_attribute(self):
        with trace("pub", backend="eventbus") as span:
            span.set_attribute("event_id", "abc123")
        assert span.attributes["backend"] == "eventbus"
        assert span.attributes["event_id"] == "abc123"


class TestEnvelopeAttachment:
    def test_attach_propagates_trace_id(self):
        env = BusEnvelope(topic="t", payload={"x": 1})
        env.trace_id = ""  # clear
        with trace("pub") as span:
            attach_to_envelope(env)
        assert env.trace_id == span.trace_id

    def test_attach_does_not_overwrite_existing(self):
        env = BusEnvelope(topic="t", payload={"x": 1})
        existing = "abc123"
        env.trace_id = existing
        with trace("pub") as span:
            attach_to_envelope(env)
        # Should not overwrite
        assert env.trace_id == existing

    def test_attach_no_current_context_noop(self):
        env = BusEnvelope(topic="t", payload={"x": 1})
        original = env.trace_id
        attach_to_envelope(env)
        assert env.trace_id == original


class TestEndToEndPublish:
    def test_publish_inherits_trace_id(self):
        """Trace context in caller propagates to envelope at publish."""
        from bus_foundation import _backends, publish
        from bus_foundation.metrics_instrumentation import record_dlq_depth

        env = BusEnvelope(topic="trace:test", payload={"x": 1}, event_type=EventType.PIPELINE_COMPLETED)
        env.trace_id = ""  # clear
        with trace("caller_op") as span:
            publish(env)
        assert env.trace_id == span.trace_id
