#!/usr/bin/env python3
"""Observability bridge example — DataPlaneBackend → Langfuse.

This example subscribes to the bus-foundation data plane and forwards
every batch of events to Langfuse's API for tracing and analytics.

Architecture:
  Any consumer -> bus.data.emit() -> DataPlaneBackend (ring buffer)
    -> flush thread -> this bridge -> Langfuse API

Usage:
  1. Start Langfuse (docker compose -p observability up -d)
  2. Set LANGFUSE_* env vars (or use defaults)
  3. python examples/observability_bridge.py

Env vars:
  LANGFUSE_HOST    (default: http://127.0.0.1:3000)
  LANGFUSE_SECRET  (default: sk-lf-...)
  LANGFUSE_PUBLIC  (default: pk-lf-...)
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import httpx

from bus_foundation import BusEnvelope  # noqa: F401
from bus_foundation.backends.data_plane import DataPlaneBackend
from bus_foundation.envelope import OmniEnvelope

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LANGFUSE_HOST = os.environ.get("LANGFUSE_HOST", "http://127.0.0.1:3000")
LANGFUSE_SECRET = os.environ.get("LANGFUSE_SECRET", "sk-lf-...")
LANGFUSE_PUBLIC = os.environ.get("LANGFUSE_PUBLIC", "pk-lf-...")

# How often we flush to Langfuse (seconds).
FLUSH_INTERVAL = 2.0
# How many events max per batch API call.
MAX_BATCH_SIZE = 100


def _to_langfuse_generation(
    env: OmniEnvelope,
) -> dict[str, Any]:
    """Translate a bus envelope to a Langfuse generation event."""
    return {
        "name": env.topic,
        "trace_id": env.trace_id or env.id,
        "input": env.payload,
        "metadata": {
            "source": env.source_uri,
            "plane": env.plane,
            "version": env.version,
        },
        "timestamp": int(env.timestamp * 1000),
    }


class LangfuseBridge:
    """Subscribe to DataPlane, forward batches to Langfuse."""

    def __init__(
        self,
        data_plane: DataPlaneBackend,
        host: str = LANGFUSE_HOST,
        secret_key: str = LANGFUSE_SECRET,
        public_key: str = LANGFUSE_PUBLIC,
    ):
        self._dp = data_plane
        self._host = host.rstrip("/")
        self._auth = httpx.BasicAuth(public_key, secret_key)
        self._client = httpx.Client(base_url=self._host, auth=self._auth, timeout=10.0)

    def start(self) -> str:
        """Subscribe to all data plane events and forward to Langfuse."""
        sub_id = self._dp.subscribe("*", self._on_batch)
        logger.info("observability_bridge subscribed=%s langfuse=%s", sub_id, self._host)
        return sub_id

    def _on_batch(self, batch: list[OmniEnvelope]) -> None:
        """Called by DataPlaneBackend when a batch is flushed."""
        if not batch:
            return

        generations = []
        for env in batch[:MAX_BATCH_SIZE]:
            generations.append(_to_langfuse_generation(env))

        try:
            resp = self._client.post(
                "/api/public/generations",
                json={"generations": generations},
            )
            if resp.status_code not in (200, 201):
                logger.error(
                    "langfuse_post_error status=%s body=%s",
                    resp.status_code,
                    resp.text[:200],
                )
            else:
                logger.debug("langfuse_batch_sent count=%d", len(generations))
        except httpx.RequestError as e:
            logger.warning("langfuse_connection_error err=%s", e)

    def close(self) -> None:
        self._client.close()


if __name__ == "__main__":
    dp = DataPlaneBackend(flush_interval=FLUSH_INTERVAL)
    bridge = LangfuseBridge(dp)
    bridge.start()

    # Publish a test event every 5 seconds
    i = 0
    try:
        while True:
            env = OmniEnvelope(
                topic="telemetry:heartbeat",
                source_uri="bos://capability/bus/data",
                payload={"seq": i, "ts": time.time()},
            )
            dp.publish(env)
            logger.info("published seq=%d", i)
            i += 1
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("shutting down")
    finally:
        bridge.close()
        dp.close()
