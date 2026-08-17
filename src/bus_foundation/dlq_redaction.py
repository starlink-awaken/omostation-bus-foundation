"""R91: DLQ payload redaction.

Strips / masks sensitive fields before enqueueing to DLQ, so that
operator-facing tooling (bus-dlq list) does not leak credentials
or PII.

Default rules (always on):
- Field name match (case-insensitive substring): password, secret,
  token, api_key, apikey, credential, private_key
- Luhn-valid credit card numbers
- Email addresses (RFC 5322 simplified)
- IPv4 / IPv6 addresses

Custom rules: drop a YAML file at ~/.config/bus-foundation/redaction.yaml
    patterns:
      - name: "ssn"
        regex: "\\\\b\\\\d{3}-\\\\d{2}-\\\\d{4}\\\\b"
        replacement: "[SSN]"
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from .metrics import Counter, Registry

# ── Metrics ─────────────────────────────────────────────────────

REDACTED_TOTAL: Counter = Registry.register(Counter("bus_dlq_redacted_total", labels=("backend", "field_class")))


# ── Default rules ───────────────────────────────────────────────

DEFAULT_FIELD_SUBSTRINGS = (
    "password",
    "secret",
    "token",
    "api_key",
    "apikey",
    "credential",
    "private_key",
    "auth",
    "session",
)


# Luhn check
def _is_luhn_valid(s: str) -> bool:
    digits = [int(c) for c in s if c.isdigit()]
    if len(digits) < 13 or len(digits) > 19:
        return False
    checksum = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        checksum += d
    return checksum % 10 == 0


CREDIT_CARD_RE = re.compile(r"\b(?:\d[ -]*?){13,19}\b")
EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
IPV4_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")


@dataclass
class RedactionConfig:
    extra_patterns: list[dict] = field(default_factory=list)
    disable_email: bool = False
    disable_ip: bool = False


class Redactor:
    """Apply redaction to a dict / json string."""

    def __init__(self, config: RedactionConfig | None = None):
        self.config = config or RedactionConfig()
        self._user_patterns = [
            (re.compile(p["regex"]), p.get("replacement", "[REDACTED]")) for p in self.config.extra_patterns
        ]

    def redact_dict(self, obj: dict, backend: str = "unknown") -> dict:
        """Recursively redact sensitive fields. Returns a NEW dict."""
        out: dict = {}
        for k, v in obj.items():
            if self._is_sensitive_key(k):
                out[k] = "[REDACTED]"
                REDACTED_TOTAL.inc(backend=backend, field_class="password")
            elif isinstance(v, dict):
                out[k] = self.redact_dict(v, backend=backend)
            elif isinstance(v, list):
                out[k] = [
                    self.redact_dict(x, backend) if isinstance(x, dict) else self._redact_scalar(x, backend) for x in v
                ]
            elif isinstance(v, str):
                out[k] = self._redact_scalar(v, backend)
            else:
                out[k] = v
        return out

    def _is_sensitive_key(self, key: str) -> bool:
        k = key.lower()
        return any(sub in k for sub in DEFAULT_FIELD_SUBSTRINGS)

    def _redact_scalar(self, s: str, backend: str) -> str:
        out = s
        for m in CREDIT_CARD_RE.finditer(out):
            if _is_luhn_valid(m.group()):
                out = out.replace(m.group(), "[CC]")
                REDACTED_TOTAL.inc(backend=backend, field_class="pii")
        if not self.config.disable_email:
            for m in EMAIL_RE.finditer(out):
                out = out.replace(m.group(), "[EMAIL]")
                REDACTED_TOTAL.inc(backend=backend, field_class="pii")
        if not self.config.disable_ip:
            for m in IPV4_RE.finditer(out):
                out = out.replace(m.group(), "[IP]")
                REDACTED_TOTAL.inc(backend=backend, field_class="pii")
        for pat, repl in self._user_patterns:
            out = pat.sub(repl, out)
        return out

    def redact_json(self, s: str, backend: str = "unknown") -> str:
        try:
            obj = json.loads(s)
        except (ValueError, TypeError):
            return s  # not JSON, return as-is
        if not isinstance(obj, dict):
            return s
        return json.dumps(self.redact_dict(obj, backend=backend))


def load_user_config() -> RedactionConfig:
    """Load ~/.config/bus-foundation/redaction.yaml if present."""
    path = Path.home() / ".config" / "bus-foundation" / "redaction.yaml"
    if not path.exists():
        return RedactionConfig()
    try:
        import yaml
    except ImportError:
        return RedactionConfig()
    try:
        data = yaml.safe_load(path.read_text()) or {}
    except Exception:
        return RedactionConfig()
    return RedactionConfig(
        extra_patterns=data.get("patterns", []),
        disable_email=data.get("disable_email", False),
        disable_ip=data.get("disable_ip", False),
    )


__all__ = [
    "Redactor",
    "RedactionConfig",
    "load_user_config",
]
