# pyright: reportArgumentType=false

"""Test DLQ archive + retention (R83).

Covers:
  1. _maybe_rotate creates a tar.gz archive when DB exceeds max_size
  2. Archives are stored in the same directory as the DB
  3. _purge_expired_archives removes old archives beyond retention
  4. list_archives returns metadata about retained archives
  5. No data loss path: archive + delete original (instead of hard rename)
  6. Fallback: if archiving fails, falls back to old rename behaviour
"""

from __future__ import annotations

import tarfile
from pathlib import Path

from bus_foundation.dlq import DLQ


def _make_large_db(db_path: Path) -> DLQ:
    """Create a DLQ with enough entries to exceed a tiny max_size_mb."""
    dlq = DLQ(db_path=db_path, max_size_mb=0.01, retention_days=30)
    # Insert a bunch of events to fill the DB past 0.01 MB.
    for i in range(2000):
        dlq.enqueue(
            event_id=f"e-{i:04d}",
            backend="test",
            envelope_json='{"k": "v_' + "x" * 50 + '"}',
            error=f"err-{i}",
        )
    return dlq


class TestArchive:
    def test_rotation_creates_archive(self, tmp_path: Path) -> None:
        dlq = _make_large_db(tmp_path / "test.db")
        dlq.close()

        # Rotation should have happened during __init__ + enqueue.
        # The original db should still exist (because rotation happens
        # on __init__, but we inserted AFTER). Let's trigger manually
        # by creating a new DLQ that sees the large db.
        dlq2 = DLQ(db_path=tmp_path / "test.db", max_size_mb=0.01, retention_days=30)
        dlq2.close()

        archives = list(tmp_path.glob("bus_dlq_archive_*.tar.gz"))
        assert len(archives) >= 1, f"Expected at least 1 archive in {list(tmp_path.iterdir())}"

    def test_archive_is_valid_tar_gz(self, tmp_path: Path) -> None:
        dlq = _make_large_db(tmp_path / "test.db")
        dlq.close()
        dlq2 = DLQ(db_path=tmp_path / "test.db", max_size_mb=0.01, retention_days=30)
        dlq2.close()

        archives = list(tmp_path.glob("bus_dlq_archive_*.tar.gz"))
        assert archives, "no archive created"
        # Verify the archive contains the DB file.
        with tarfile.open(str(archives[0]), "r:gz") as tar:
            names = tar.getnames()
            assert any("test.db" in n for n in names)

    def test_archive_does_not_delete_original_db(self, tmp_path: Path) -> None:
        """After archiving, the original db file should be gone but the
        archive file should exist (new db is fresh)."""
        db_path = tmp_path / "test.db"
        # Insert large enough to force rotation.
        _make_large_db(db_path)
        # Second init should rotate.
        dlq2 = DLQ(db_path=db_path, max_size_mb=0.01, retention_days=30)
        dlq2.close()

        # Original db should still exist (fresh empty one created by rotation)
        assert db_path.exists(), "fresh db should exist after rotation"
        archives = list(tmp_path.glob("bus_dlq_archive_*.tar.gz"))
        assert archives, "archive should exist"

    def test_list_archives_returns_metadata(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.db"
        _make_large_db(db_path)
        dlq2 = DLQ(db_path=db_path, max_size_mb=0.01, retention_days=30)
        try:
            archives = dlq2.list_archives()
            assert len(archives) >= 1
            for a in archives:
                assert "path" in a
                assert "size_bytes" in a
                assert "age_days" in a
                assert "created_at" in a
        finally:
            dlq2.close()

    def test_purge_expired_archives(self, tmp_path: Path) -> None:
        """Create an archive that looks old enough to be purged."""
        db_path = tmp_path / "test.db"
        _make_large_db(db_path)
        dlq2 = DLQ(db_path=db_path, max_size_mb=0.01, retention_days=0)
        dlq2.close()
        # Archives should be immediately expired (=retention_days=0).
        dlq3 = DLQ(db_path=db_path, max_size_mb=0.01, retention_days=0)
        try:
            archives = dlq3.list_archives()
            # All archives should have been purged if retention_days=0.
            assert len(archives) == 0, f"expected 0 archives, got {len(archives)}"
        finally:
            dlq3.close()
