"""Orchestrator skeleton with lifecycle management and structured logging."""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from ravebear_monolith.foundation.config import AppConfig, load_config
from ravebear_monolith.util.logging import configure_logging, log_event

logger = logging.getLogger(__name__)


async def run(config: AppConfig, *, max_beats: int | None = None) -> int:
    """Run the orchestrator main loop.

    Args:
        config: Validated application configuration.
        max_beats: Optional limit on heartbeat iterations (for testing).
                   If None, runs indefinitely until cancelled.

    Returns:
        0 on clean shutdown.
    """
    # Configure logging at startup
    configure_logging(config)

    log_event(logger, logging.INFO, f"Starting {config.app_name}", event="orchestrator_start")

    beat_count = 0

    try:
        while True:
            log_event(
                logger,
                logging.DEBUG,
                f"heartbeat [{config.app_name}]",
                event="heartbeat",
                beat_count=beat_count,
            )
            beat_count += 1

            if max_beats is not None and beat_count >= max_beats:
                break

            await asyncio.sleep(config.heartbeat_interval_s)

    except asyncio.CancelledError:
        log_event(logger, logging.INFO, "Orchestrator shutting down", event="orchestrator_stop")

    return 0


def main(argv: list[str] | None = None) -> int:
    """Synchronous entry point for the orchestrator.

    Args:
        argv: Command-line arguments. If None, uses sys.argv[1:].

    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    parser = argparse.ArgumentParser(description="Ravebear Monolith Orchestrator")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/settings.yaml"),
        help="Path to configuration file (default: config/settings.yaml)",
    )
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"FATAL: Failed to load configuration: {e}", file=sys.stderr)
        return 1

    return asyncio.run(run(config))
