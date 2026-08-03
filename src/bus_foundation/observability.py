"""R94: Trace context propagation for bus-foundation.

Pure-stdlib minimal W3C trace-context-style propagation:
  - bus_foundation.envelope.trace_id (already exists) carries
    the logical trace identifier
  - bus_foundation.envelope.span_id (already exists) is the
    current span
  - On publish, R94 attaches the current span context to the
    envelope so downstream consumers (subscribers / R90 retry /
    R91 DLQ) can stitch traces together

If opentelemetry-api is installed, R94 will also create real
OTel spans via a thin adapter. Falls back to a NoOp implementation
otherwise. The `[otel]` extra installs opentelemetry-api.

Public API:
    from bus_foundation.observability import trace, get_current_trace_id
    with trace("bus.publish", backend="eventbus") as span:
        span.set_attribute("event_id", env.id)
        publish(env)
"""

from __future__ import annotations

import contextvars
import logging
import uuid
from collections.abc import Iterator
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Context variable: current trace_id (string or None)
_current_trace_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "bus_current_trace_id",
    default=None,
)
# Context variable: current span_id (string or None)
_current_span_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "bus_current_span_id",
    default=None,
)


def new_trace_id() -> str:
    """Generate a new W3C-style 32-hex-char trace ID."""
    return uuid.uuid4().hex


def new_span_id() -> str:
    """Generate a new W3C-style 16-hex-char span ID."""
    return uuid.uuid4().hex[:16]


def get_current_trace_id() -> str | None:
    """Return the trace_id of the current span context, or None."""
    return _current_trace_id.get()


def get_current_span_id() -> str | None:
    return _current_span_id.get()


class Span:
    """Minimal span object. If OTel is installed, wraps an OTel span."""

    def __init__(
        self,
        name: str,
        trace_id: str | None = None,
        parent_span_id: str | None = None,
    ):
        self.name = name
        self.trace_id = trace_id or new_trace_id()
        self.span_id = new_span_id()
        self.parent_span_id = parent_span_id
        self.attributes: dict[str, object] = {}
        self._otel_span: object | None = None
        self._token_trace: contextvars.Token | None = None
        self._token_span: contextvars.Token | None = None

    def set_attribute(self, key: str, value: object) -> None:
        self.attributes[key] = value
        if self._otel_span is not None:
            try:
                self._otel_span.set_attribute(key, value)  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001
                pass

    def end(self) -> None:
        if self._otel_span is not None:
            try:
                self._otel_span.end()  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001
                pass


@contextmanager
def trace(
    name: str,
    backend: str | None = None,
    parent: Span | None = None,
) -> Iterator[Span]:
    """Open a trace span. Yields the Span object; call .set_attribute() etc.

    On exit, restores the previous context (parent span).
    """
    parent_trace_id = _current_trace_id.get() or (parent.trace_id if parent else None)
    parent_span_id = _current_span_id.get() or (parent.span_id if parent else None)

    span = Span(
        name=name,
        trace_id=parent_trace_id,
        parent_span_id=parent_span_id,
    )
    if backend:
        span.set_attribute("backend", backend)

    # Try OTel: optional dependency
    try:
        from opentelemetry import trace as otel_trace

        tracer = otel_trace.get_tracer("bus_foundation")
        otel_span = tracer.start_span(name)
        # Carry our trace_id into OTel span context
        otel_span.set_attribute("bus.trace_id", span.trace_id)
        otel_span.set_attribute("bus.span_id", span.span_id)
        span._otel_span = otel_span
    except ImportError:
        pass  # OTel not installed, use pure-stdlib
    except Exception as e:  # noqa: BLE001
        logger.debug("otel_adapter_init_failed: %s", e)

    token_trace = _current_trace_id.set(span.trace_id)
    token_span = _current_span_id.set(span.span_id)
    try:
        yield span
    finally:
        span.end()
        _current_trace_id.reset(token_trace)
        _current_span_id.reset(token_span)


def attach_to_envelope(envelope: object) -> None:
    """Copy current trace context onto an envelope before publish.

    Best-effort: if envelope has no trace_id field, do nothing.
    """
    trace_id = get_current_trace_id()
    if trace_id is None:
        return
    if hasattr(envelope, "trace_id"):
        try:
            if not envelope.trace_id:  # type: ignore[reportAttributeAccessIssue]
                envelope.trace_id = trace_id  # type: ignore[reportAttributeAccessIssue]
        except (AttributeError, TypeError):
            pass


__all__ = [
    "Span",
    "trace",
    "new_trace_id",
    "new_span_id",
    "get_current_trace_id",
    "get_current_span_id",
    "attach_to_envelope",
]
