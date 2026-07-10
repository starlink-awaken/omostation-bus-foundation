"""R91: tests for DLQ redaction + admin + CLI."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from bus_foundation.dlq import DLQ
from bus_foundation.dlq_admin import DLQAdmin
from bus_foundation.dlq_redaction import Redactor, RedactionConfig


@pytest.fixture
def tmp_dlq(tmp_path: Path) -> DLQ:
    dlq = DLQ(db_path=tmp_path / "test_dlq.db")
    yield dlq
    dlq.close()


class TestRedaction:
    def test_password_field_redacted(self):
        r = Redactor()
        out = r.redact_dict({"username": "alice", "password": "secret123"}, backend="eventbus")
        assert out["username"] == "alice"
        assert out["password"] == "[REDACTED]"

    def test_nested_redaction(self):
        r = Redactor()
        out = r.redact_dict(
            {"user": {"name": "alice", "token": "abc"}},
            backend="eventbus",
        )
        assert out["user"]["name"] == "alice"
        assert out["user"]["token"] == "[REDACTED]"

    def test_credit_card_redacted(self):
        r = Redactor()
        cc = "4532015112830366"  # valid Luhn
        out = r.redact_dict({"note": f"card is {cc}"}, backend="eventbus")
        assert "[CC]" in out["note"]
        assert cc not in out["note"]

    def test_email_redacted(self):
        r = Redactor()
        out = r.redact_dict({"contact": "alice@example.com"}, backend="eventbus")
        assert "[EMAIL]" in out["contact"]
        assert "alice" not in out["note"] if "note" in out else "[EMAIL]" in out["contact"]

    def test_ipv4_redacted(self):
        r = Redactor()
        out = r.redact_dict({"host": "192.168.1.1"}, backend="eventbus")
        assert "[IP]" in out["host"]

    def test_custom_pattern(self):
        config = RedactionConfig(
            extra_patterns=[{"regex": r"\b\d{3}-\d{4}\b", "replacement": "[PHONE]"}],
        )
        r = Redactor(config)
        out = r.redact_dict({"tel": "555-1234"}, backend="eventbus")
        assert out["tel"] == "[PHONE]"

    def test_disable_email(self):
        r = Redactor(RedactionConfig(disable_email=True))
        out = r.redact_dict({"contact": "alice@example.com"}, backend="eventbus")
        assert "alice@example.com" in out["contact"]


class TestDLQEnqueueRedaction:
    def test_enqueue_redacts_password(self, tmp_dlq: DLQ):
        env = json.dumps({"topic": "t", "payload": {"password": "leaked", "data": "ok"}})
        tmp_dlq.enqueue("evt-1", "eventbus", env, "test error")
        # Read back raw
        rows = tmp_dlq.list_all()
        assert len(rows) == 1
        stored = json.loads(rows[0]["envelope_json"])
        assert stored["payload"]["password"] == "[REDACTED]"
        assert stored["payload"]["data"] == "ok"

    def test_enqueue_redacts_credit_card(self, tmp_dlq: DLQ):
        env = json.dumps({"payload": {"note": "card 4532015112830366 expired"}})
        tmp_dlq.enqueue("evt-2", "eventbus", env, "x")
        rows = tmp_dlq.list_all()
        stored = json.loads(rows[0]["envelope_json"])
        assert "[CC]" in stored["payload"]["note"]


class TestDLQAdmin:
    def test_list_filter_backend(self, tmp_dlq: DLQ):
        tmp_dlq.enqueue("a", "eventbus", '{"x":1}', "e1")
        tmp_dlq.enqueue("b", "croniter", '{"x":2}', "e2")
        admin = DLQAdmin(db_path=tmp_dlq.db_path)
        try:
            all_entries = admin.list()
            assert len(all_entries) == 2
            eventbus_entries = admin.list(backend="eventbus")
            assert len(eventbus_entries) == 1
            assert eventbus_entries[0].backend == "eventbus"
        finally:
            admin.close()

    def test_stats_counts(self, tmp_dlq: DLQ):
        for i in range(3):
            tmp_dlq.enqueue(f"e{i}", "eventbus", '{"x":1}', "err")
        admin = DLQAdmin(db_path=tmp_dlq.db_path)
        try:
            stats = admin.stats()
            assert stats["total"] == 3
            assert stats["by_backend"].get("eventbus") == 3
            assert stats["by_status"].get("PENDING") == 3
        finally:
            admin.close()

    def test_purge_dry_run(self, tmp_dlq: DLQ):
        for i in range(2):
            tmp_dlq.enqueue(f"e{i}", "eventbus", '{}', "x")
        admin = DLQAdmin(db_path=tmp_dlq.db_path)
        try:
            n = admin.purge(backend="eventbus")
            assert n == 2
            # Without --yes, no actual delete
            assert len(admin.list()) == 2
        finally:
            admin.close()


class TestBusDLQCLI:
    @pytest.fixture
    def workspace_root(self) -> Path:
        # tests live at projects/bus-foundation/tests/
        # bin/bus-dlq is at <repo_root>/bin/bus-dlq
        return Path(__file__).resolve().parent.parent.parent.parent

    def test_cli_help(self, workspace_root: Path):
        result = subprocess.run(
            [sys.executable, "bin/bus-dlq", "--help"],
            capture_output=True, text=True,
            cwd=str(workspace_root),
        )
        assert result.returncode == 0
        assert "bus-foundation DLQ admin CLI" in result.stdout

    def test_cli_list(self, tmp_path: Path, workspace_root: Path):
        # Use isolated DLQ
        db = tmp_path / "cli_dlq.db"
        env_setup = json.dumps({"topic": "t", "payload": {"password": "leaked"}})
        dlq = DLQ(db_path=db)
        dlq.enqueue("evt-1", "eventbus", env_setup, "err")
        dlq.close()

        result = subprocess.run(
            [
                sys.executable, "bin/bus-dlq",
                "--db-path", str(db),
                "list", "--json",
            ],
            capture_output=True, text=True,
            cwd=str(workspace_root),
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        entries = json.loads(result.stdout)
        assert len(entries) == 1
        # Password was redacted before enqueue
        env = json.loads(entries[0]["envelope_json"])
        assert env["payload"]["password"] == "[REDACTED]"
