"""Orchestrator skeleton with lifecycle management, structured logging, and health checks."""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from ravebear_monolith.foundation.config import AppConfig, load_config
from ravebear_monolith.storage.event_sink import EventSink
from ravebear_monolith.util.health import collect_health_snapshot
from ravebear_monolith.util.kill_switch import KillSwitch
from ravebear_monolith.util.logging import configure_logging, log_event

logger = logging.getLogger(__name__)


def trigger_kill_switch(path: Path, reason: str) -> None:
    """Write kill switch file with reason."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"KILL\n{reason}", encoding="utf-8")
    except Exception:
        pass  # Best effort


async def run(config: AppConfig, *, max_beats: int | None = None) -> int:
    """Run the orchestrator main loop.

    Args:
        config: Validated application configuration.
        max_beats: Optional limit on heartbeat iterations (for testing).
                   If None, runs indefinitely until cancelled.

    Returns:
        0 on clean shutdown, 2 on kill switch triggered.
    """
    # Configure logging at startup
    configure_logging(config)

    log_event(logger, logging.INFO, f"Starting {config.app_name}", event="orchestrator_start")

    # Collect and log health snapshot
    health = collect_health_snapshot(
        data_dir=config.data_dir,
        min_free_disk_mb=config.min_free_disk_mb,
        min_python_major=config.min_python_major,
        min_python_minor=config.min_python_minor,
    )
    log_event(
        logger,
        logging.INFO,
        f"Health check: {'OK' if health.ok else 'DEGRADED'}",
        event="health_snapshot",
        ok=health.ok,
        checks=health.checks,
    )

    # Initialize kill switch
    kill_switch = KillSwitch(config.kill_switch_path)

    # Initialize event sink
    event_sink = EventSink(config.storage.db_path)
    try:
        await event_sink.open()
    except Exception as e:
        log_event(
            logger,
            logging.ERROR,
            f"Failed to open event sink: {e}",
            event="event_sink_open_failed",
            error=str(e),
        )
        return 1

    beat_count = 0

    try:
        while True:
            # Check kill switch each heartbeat
            if kill_switch.should_halt():
                log_event(
                    logger,
                    logging.WARNING,
                    "Kill switch triggered",
                    event="kill_switch_triggered",
                    reason=kill_switch.reason(),
                )
                return 2

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

    finally:
        await event_sink.close()

    return 0


async def run_with_collectors(
    config: AppConfig,
    collectors: list,
    *,
    max_events: int | None = None,
) -> int:
    """Run orchestrator with collectors and EventSink integration.

    Args:
        config: Validated application configuration.
        collectors: List of CollectorBase instances.
        max_events: Maximum events to process. None for unlimited.

    Returns:
        0 on clean shutdown, 2 on kill switch or fatal error.
    """
    from ravebear_monolith.collectors.router import CollectorRouter
    from ravebear_monolith.util.rate_limit import BudgetRegistry

    # Configure logging at startup
    configure_logging(config)

    log_event(logger, logging.INFO, f"Starting {config.app_name}", event="orchestrator_start")

    # Collect and log health snapshot
    health = collect_health_snapshot(
        data_dir=config.data_dir,
        min_free_disk_mb=config.min_free_disk_mb,
        min_python_major=config.min_python_major,
        min_python_minor=config.min_python_minor,
    )
    log_event(
        logger,
        logging.INFO,
        f"Health check: {'OK' if health.ok else 'DEGRADED'}",
        event="health_snapshot",
        ok=health.ok,
        checks=health.checks,
    )

    # Initialize event sink
    event_sink = EventSink(config.storage.db_path)
    try:
        await event_sink.open()
    except Exception as e:
        log_event(
            logger,
            logging.ERROR,
            f"Failed to open event sink: {e}",
            event="event_sink_open_failed",
            error=str(e),
        )
        return 1

    # Initialize router
    budget_registry = BudgetRegistry()
    router = CollectorRouter(
        collectors=collectors,
        budget_registry=budget_registry,
        kill_switch_path=config.kill_switch_path,
    )

    try:
        # Drain events from router to sink
        async for event in router.events(max_events=max_events):
            try:
                await event_sink.write(event)
            except Exception as e:
                # Fatal: log error, trigger kill switch, exit
                log_event(
                    logger,
                    logging.ERROR,
                    f"Event sink write failed: {e}",
                    event="event_sink_write_failed",
                    error=str(e),
                )
                trigger_kill_switch(
                    config.kill_switch_path,
                    f"EventSink write failed: {e}",
                )
                return 2

        log_event(
            logger,
            logging.INFO,
            f"Processing complete, {router.event_count} events",
            event="orchestrator_complete",
            event_count=router.event_count,
        )

    except asyncio.CancelledError:
        log_event(logger, logging.INFO, "Orchestrator shutting down", event="orchestrator_stop")

    finally:
        await event_sink.close()

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
