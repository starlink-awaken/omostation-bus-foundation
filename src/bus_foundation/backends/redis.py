"""R93: Redis backend for bus-foundation.

Distributed pub/sub across processes via Redis pub/sub channels.

Usage:
    pip install bus-foundation[redis]

    from bus_foundation.backends.redis import RedisBackend
    backend = RedisBackend(url="redis://localhost:6379", channel="omni-bus")
    backend.publish(envelope)

Requires the `redis` extra (`redis>=5.0`). Default off — no hard dependency.
"""

from __future__ import annotations

import logging
import threading
import uuid
from collections.abc import Callable
from typing import Any

from bus_foundation.backends.pattern_match import match_pattern
from bus_foundation.envelope import BusEnvelope

logger = logging.getLogger(__name__)


class RedisBackend:
    """Distributed pub/sub via Redis.

    Uses Redis PUBLISH / SUBSCRIBE for fan-out. Messages are JSON-serialized
    BusEnvelope dicts. Subscribers filter by pattern match locally (same
    pattern_match() as other backends).

    Thread safety: uses a dedicated subscription thread (daemon) per
    subscribe() call, similar to CroniterBackend's poller pattern.
    Connection pooling is handled by the redis-py library automatically.
    """

    name = "redis"

    def __init__(
        self,
        url: str = "redis://localhost:6379",
        channel: str = "omni-bus",
        max_queue_size: int = 10000,
    ):
        self._url = url
        self._channel = channel
        self._max_queue_size = max_queue_size
        self._lock = threading.Lock()
        self._subscribers: dict[str, tuple[str, Callable, Any, threading.Thread]] = {}
        self._redis_module = None
        self._pub_conn: Any = None

    def _lazy_import(self) -> Any:
        if self._redis_module is None:
            import redis  # type: ignore[reportMissingImports]

            self._redis_module = redis
        return self._redis_module

    def _get_conn(self) -> Any:
        redis = self._lazy_import()
        return redis.from_url(self._url, decode_responses=True, health_check_interval=30)

    def is_available(self) -> bool:
        try:
            r = self._get_conn()
            return r.ping()
        except Exception:  # noqa: BLE001
            return False

    def publish(self, envelope: BusEnvelope) -> str:
        try:
            r = self._get_conn()
            r.publish(self._channel, envelope.to_json())
            return envelope.id
        except Exception as e:  # noqa: BLE001
            logger.error("redis_publish_failed channel=%s error=%s", self._channel, e)
            raise

    def subscribe(self, pattern: str, callback: Callable) -> str:
        self._lazy_import()  # verify redis is available
        r = self._get_conn()
        pubsub = r.pubsub()
        pubsub.subscribe(self._channel)
        sub_id = f"redis-{len(self._subscribers)}-{uuid.uuid4().hex[:8]}"
        stop_event = threading.Event()

        def _listener() -> None:
            try:
                for message in pubsub.listen():
                    if stop_event.is_set():
                        break
                    if message["type"] != "message":
                        continue
                    data = message.get("data", "")
                    if not isinstance(data, str) or not data:
                        continue
                    try:
                        env = BusEnvelope.from_json(data)
                    except Exception:  # noqa: BLE001
                        continue
                    if match_pattern(pattern, env.type):
                        try:
                            callback(env)
                        except Exception as e:  # noqa: BLE001
                            logger.error("redis_subscribe_callback_error sub_id=%s error=%s", sub_id, e)
            except Exception as e:  # noqa: BLE001
                logger.error("redis_listener_exception sub_id=%s error=%s", sub_id, e)
            finally:
                try:
                    pubsub.close()
                except Exception:  # noqa: BLE001
                    pass

        thread = threading.Thread(target=_listener, daemon=True, name=f"bus-redis-{sub_id[:8]}")
        thread.start()
        with self._lock:
            self._subscribers[sub_id] = (pattern, callback, pubsub, thread)
        from bus_foundation.metrics_instrumentation import record_subscribe

        record_subscribe("redis", pattern, len(self._subscribers))
        return sub_id

    def unsubscribe(self, sub_id: str) -> bool:
        with self._lock:
            entry = self._subscribers.pop(sub_id, None)
            if entry is None:
                return False
            _pattern, _cb, pubsub, thread = entry
        try:
            pubsub.close()
        except Exception:  # noqa: BLE001
            pass
        thread.join(timeout=2.0)
        return True

    def close(self) -> None:
        with self._lock:
            for sub_id in list(self._subscribers.keys()):
                self.unsubscribe(sub_id)


__all__ = ["RedisBackend"]
