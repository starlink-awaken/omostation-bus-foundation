#!/usr/bin/env python3
"""SSE + FastAPI example — wire bus-foundation SSEBackend to HTTP events.

Usage:
    pip install bus-foundation fastapi uvicorn
    python examples/sse_bridge.py

Then in another terminal:
    curl http://127.0.0.1:9876/events

And in yet another:
    python -c "
    from bus_foundation import publish, BusEnvelope
    publish(BusEnvelope(topic='status:hello', source_uri='bos://example'))
    "
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import FastAPI, Request  # type: ignore[reportMissingImports]
from fastapi.responses import StreamingResponse  # type: ignore[reportMissingImports]

from bus_foundation import BusEnvelope, publish
from bus_foundation.backends.sse import SSEBackend

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="bus-foundation SSE Bridge")

# The bus-foundation SSEBackend acts as an in-process fan-out; our
# FastAPI endpoint drains it via subscribe() and streams events as
# SSE. Each connected client gets its own asyncio.Queue.
#
# For a multi-client setup, the SSEBackend supports
# client_connected() / client_disconnected() counters; use them
# from a middleware to track total concurrent connections.

sse = SSEBackend()

HOST = "127.0.0.1"
PORT = 9876


@app.get("/health")
async def health():
    return {"status": "ok", "sse_clients": sse.client_count()}


@app.get("/events")
async def sse_stream(request: Request):
    """SSE stream: server-sent events matching all patterns."""
    queue: asyncio.Queue = asyncio.Queue(maxsize=500)

    def callback(env: BusEnvelope) -> None:
        try:
            queue.put_nowait(env)
        except asyncio.QueueFull:
            logger.warning("SSE client queue full, dropping event")

    sub_id = sse.subscribe("*", callback)
    sse.client_connected()

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    env = await asyncio.wait_for(queue.get(), timeout=30.0)
                    data = env.to_json()
                    yield f"data: {data}\n\n"
                except TimeoutError:
                    yield ": keepalive\n\n"  # SSE keepalive
        finally:
            sse.unsubscribe(sub_id)
            sse.client_disconnected()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/emit")
async def emit(topic: str = "demo:ping", message: str = "hello"):
    """Emit an envelope via the bus facade (for curl-based testing)."""
    env = BusEnvelope(
        topic=topic,
        source_uri="bos://example/sse_bridge",
        payload={"message": message},
    )
    publish(env)
    return {"event_id": env.id, "topic": topic}


if __name__ == "__main__":
    import uvicorn  # type: ignore[reportMissingImports]

    print(f"SSE Bridge listening at http://{HOST}:{PORT}/events")
    print(f"  Emit test:   curl http://{HOST}:{PORT}/emit")
    print(f"  Stream SSE:  curl -N http://{HOST}:{PORT}/events")
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
