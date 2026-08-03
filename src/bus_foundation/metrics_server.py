"""R89: HTTP /metrics server for bus-foundation.

Lazy: only loaded when `bus_foundation.enable_metrics(server=True)` is called.
Uses prometheus_client if available; otherwise returns 503 with a hint.

Endpoint: GET /metrics (Prometheus text format).
"""

from __future__ import annotations

import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from .metrics import Registry

logger = logging.getLogger(__name__)


class _MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/metrics":
            self.send_error(404, "Not Found")
            return
        body = self._render_prom().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _render_prom(self) -> str:
        lines: list[str] = []
        for name, m in Registry.all().items():
            if hasattr(m, "snapshot"):
                snap = m.snapshot()  # type: ignore[reportAttributeAccessIssue]
                if not snap:
                    continue
                for label_key, val in snap.items():
                    labels_str = ""
                    if label_key:
                        kv = ",".join(f'{k}="{v}"' for k, v in label_key)
                        labels_str = "{" + kv + "}"
                    if isinstance(val, dict) and "buckets" in val:
                        for b, c in val["buckets"].items():
                            lines.append(f'{name}_bucket{{le="{b}",{labels_str[1:-1] if labels_str else ""}}} {c}')
                        lines.append(f"{name}_sum{labels_str} {val['sum']}")
                        lines.append(f"{name}_count{labels_str} {val['count']}")
                    else:
                        lines.append(f"{name}{labels_str} {val}")
        return "\n".join(lines) + "\n"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(format, *args)


class MetricsHTTPServer:
    """Wrapper around HTTPServer. Single background thread, daemon."""

    def __init__(self, host: str = "0.0.0.0", port: int = 8745):
        self.host = host
        self.port = port
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._server is not None:
            return
        self._server = HTTPServer((self.host, self.port), _MetricsHandler)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            daemon=True,
            name="bus-foundation-metrics",
        )
        self._thread.start()
        logger.info("bus_metrics_server_started host=%s port=%s", self.host, self.port)

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
        logger.info("bus_metrics_server_stopped")


__all__ = ["MetricsHTTPServer"]
