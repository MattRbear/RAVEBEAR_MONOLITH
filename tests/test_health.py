"""Tests for health check utilities."""

import sys
from pathlib import Path
from unittest.mock import patch

from ravebear_monolith.util.health import (
    HealthSnapshot,
    check_disk_free,
    check_python_version,
    check_write_access,
    collect_health_snapshot,
)


class TestCheckDiskFree:
    """Tests for check_disk_free."""

    def test_sufficient_disk_space(self, tmp_path: Path) -> None:
        """Returns ok=True when disk has enough space."""
        result = check_disk_free(tmp_path, min_free_mb=1)
        assert result["ok"] is True
        assert "free_mb" in result["detail"]
        assert result["detail"]["min_free_mb"] == 1

    def test_insufficient_disk_space(self, tmp_path: Path) -> None:
        """Returns ok=False when disk space is below threshold."""
        # Request absurdly high amount
        result = check_disk_free(tmp_path, min_free_mb=999_999_999)
        assert result["ok"] is False
        assert result["detail"]["min_free_mb"] == 999_999_999

    def test_nonexistent_path_checks_parent(self, tmp_path: Path) -> None:
        """Checks parent when path doesn't exist."""
        nonexistent = tmp_path / "does" / "not" / "exist"
        result = check_disk_free(nonexistent, min_free_mb=1)
        # Should still work by checking parent
        assert "free_mb" in result["detail"]


class TestCheckPythonVersion:
    """Tests for check_python_version."""

    def test_current_version_passes(self) -> None:
        """Current Python version passes its own check."""
        major = sys.version_info.major
        minor = sys.version_info.minor
        result = check_python_version(min_major=major, min_minor=minor)
        assert result["ok"] is True
        assert result["detail"]["min"] == f"{major}.{minor}"

    def test_higher_requirement_fails(self) -> None:
        """Fails when minimum is higher than current."""
        result = check_python_version(min_major=99, min_minor=0)
        assert result["ok"] is False

    def test_lower_requirement_passes(self) -> None:
        """Passes when minimum is lower than current."""
        result = check_python_version(min_major=2, min_minor=7)
        assert result["ok"] is True

    def test_detail_contains_versions(self) -> None:
        """Detail contains current and min version strings."""
        result = check_python_version(min_major=3, min_minor=10)
        assert "current" in result["detail"]
        assert "min" in result["detail"]
        assert result["detail"]["min"] == "3.10"


class TestCheckWriteAccess:
    """Tests for check_write_access."""

    def test_writable_directory(self, tmp_path: Path) -> None:
        """Returns ok=True for writable directory."""
        result = check_write_access(tmp_path)
        assert result["ok"] is True
        assert result["detail"]["path"] == str(tmp_path)

    def test_creates_directory_if_needed(self, tmp_path: Path) -> None:
        """Creates directory if it doesn't exist."""
        new_dir = tmp_path / "new_data_dir"
        result = check_write_access(new_dir)
        assert result["ok"] is True
        assert new_dir.exists()

    def test_permission_error_returns_false(self, tmp_path: Path) -> None:
        """Returns ok=False on permission error."""
        with patch("tempfile.mkstemp", side_effect=PermissionError("denied")):
            result = check_write_access(tmp_path)
        assert result["ok"] is False
        assert "error" in result["detail"]


class TestCollectHealthSnapshot:
    """Tests for collect_health_snapshot."""

    def test_all_checks_pass(self, tmp_path: Path) -> None:
        """Returns ok=True when all checks pass."""
        snapshot = collect_health_snapshot(
            data_dir=tmp_path,
            min_free_disk_mb=1,
            min_python_major=2,
            min_python_minor=7,
        )
        assert isinstance(snapshot, HealthSnapshot)
        assert snapshot.ok is True
        assert "disk_free" in snapshot.checks
        assert "python_version" in snapshot.checks
        assert "write_access" in snapshot.checks

    def test_one_check_fails_makes_snapshot_not_ok(self, tmp_path: Path) -> None:
        """Returns ok=False when any check fails."""
        snapshot = collect_health_snapshot(
            data_dir=tmp_path,
            min_free_disk_mb=999_999_999,  # Will fail
            min_python_major=2,
            min_python_minor=7,
        )
        assert snapshot.ok is False

    def test_snapshot_has_timestamp(self, tmp_path: Path) -> None:
        """Snapshot includes UTC timestamp."""
        snapshot = collect_health_snapshot(
            data_dir=tmp_path,
            min_free_disk_mb=1,
            min_python_major=2,
            min_python_minor=7,
        )
        assert snapshot.ts_utc is not None
        assert "T" in snapshot.ts_utc  # ISO format
