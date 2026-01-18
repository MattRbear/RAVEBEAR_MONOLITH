"""Health check utilities for system monitoring.

Provides:
- HealthSnapshot: Aggregated health status
- check_disk_free: Disk space verification
- check_python_version: Python version check
- check_write_access: Filesystem write permission check
- collect_health_snapshot: Aggregate all health checks
"""

import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel


class HealthSnapshot(BaseModel):
    """Aggregated health check results."""

    ok: bool
    ts_utc: str
    checks: dict[str, dict[str, Any]]


def check_disk_free(path: Path, *, min_free_mb: int) -> dict[str, Any]:
    """Check if disk has sufficient free space.

    Args:
        path: Path to check disk space for.
        min_free_mb: Minimum required free space in MB.

    Returns:
        Check result with ok status and details.
    """
    try:
        # Get disk usage stats
        if path.exists():
            stat = os.statvfs(path) if hasattr(os, "statvfs") else None
            if stat:
                free_bytes = stat.f_bavail * stat.f_frsize
            else:
                # Windows fallback
                import shutil

                total, used, free = shutil.disk_usage(path)
                free_bytes = free
        else:
            # Path doesn't exist, check parent
            parent = path.parent
            while not parent.exists() and parent != parent.parent:
                parent = parent.parent
            import shutil

            total, used, free = shutil.disk_usage(parent)
            free_bytes = free

        free_mb = free_bytes // (1024 * 1024)
        ok = free_mb >= min_free_mb

        return {
            "ok": ok,
            "detail": {
                "path": str(path),
                "free_mb": free_mb,
                "min_free_mb": min_free_mb,
            },
        }
    except Exception as e:
        return {
            "ok": False,
            "detail": {
                "path": str(path),
                "free_mb": 0,
                "min_free_mb": min_free_mb,
                "error": str(e),
            },
        }


def check_python_version(*, min_major: int, min_minor: int) -> dict[str, Any]:
    """Check if Python version meets minimum requirements.

    Args:
        min_major: Minimum major version (e.g., 3).
        min_minor: Minimum minor version (e.g., 12).

    Returns:
        Check result with ok status and details.
    """
    current_major = sys.version_info.major
    current_minor = sys.version_info.minor
    current_micro = sys.version_info.micro

    current_str = f"{current_major}.{current_minor}.{current_micro}"
    min_str = f"{min_major}.{min_minor}"

    ok = (current_major, current_minor) >= (min_major, min_minor)

    return {
        "ok": ok,
        "detail": {
            "current": current_str,
            "min": min_str,
        },
    }


def check_write_access(path: Path) -> dict[str, Any]:
    """Check if path is writable by creating and deleting a temp file.

    Args:
        path: Directory path to check write access.

    Returns:
        Check result with ok status and details.
    """
    try:
        # Ensure directory exists
        path.mkdir(parents=True, exist_ok=True)

        # Create temp file, write, close, delete
        fd, temp_path = tempfile.mkstemp(dir=path, prefix=".health_check_")
        try:
            os.write(fd, b"health_check")
        finally:
            os.close(fd)
        os.unlink(temp_path)

        return {
            "ok": True,
            "detail": {"path": str(path)},
        }
    except Exception as e:
        return {
            "ok": False,
            "detail": {"path": str(path), "error": str(e)},
        }


def collect_health_snapshot(
    data_dir: Path,
    min_free_disk_mb: int,
    min_python_major: int,
    min_python_minor: int,
) -> HealthSnapshot:
    """Collect all health checks into a snapshot.

    Args:
        data_dir: Data directory path for disk and write checks.
        min_free_disk_mb: Minimum free disk space in MB.
        min_python_major: Minimum Python major version.
        min_python_minor: Minimum Python minor version.

    Returns:
        Aggregated health snapshot.
    """
    checks: dict[str, dict[str, Any]] = {}

    # Run all checks
    checks["disk_free"] = check_disk_free(data_dir, min_free_mb=min_free_disk_mb)
    checks["python_version"] = check_python_version(
        min_major=min_python_major, min_minor=min_python_minor
    )
    checks["write_access"] = check_write_access(data_dir)

    # Overall ok is True only if all checks pass
    all_ok = all(check.get("ok", False) for check in checks.values())

    return HealthSnapshot(
        ok=all_ok,
        ts_utc=datetime.now(timezone.utc).isoformat(),
        checks=checks,
    )
