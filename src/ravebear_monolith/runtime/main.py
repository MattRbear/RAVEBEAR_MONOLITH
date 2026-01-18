"""Minimal runtime entry point for Ravebear Monolith.

Lifecycle only: load config → init orchestrator → run briefly → shutdown.
"""

import asyncio
import sys
from pathlib import Path

from ravebear_monolith.foundation.config import load_config
from ravebear_monolith.foundation.orchestrator import run as orchestrator_run


async def _run_lifecycle(config_path: Path, max_beats: int = 1) -> int:
    """Execute the minimal lifecycle.

    Args:
        config_path: Path to configuration file.
        max_beats: Number of heartbeats before shutdown (default 1 for ~1s run).

    Returns:
        Exit code (0 for success).
    """
    config = load_config(config_path)
    return await orchestrator_run(config, max_beats=max_beats)


def run(config_path: Path | None = None, max_beats: int = 1) -> int:
    """Synchronous entry point for runtime.

    Args:
        config_path: Path to configuration file. Defaults to config/settings.yaml.
        max_beats: Number of heartbeats before shutdown.

    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    if config_path is None:
        config_path = Path("config/settings.yaml")

    try:
        return asyncio.run(_run_lifecycle(config_path, max_beats=max_beats))
    except Exception as e:
        print(f"FATAL: Runtime error: {e}", file=sys.stderr)
        return 1
