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
    live_mode: bool = False,
) -> int:
    """Run orchestrator with collectors and EventSink integration.

    Args:
        config: Validated application configuration.
        collectors: List of CollectorBase instances.
        max_events: Maximum events to process. None for unlimited.
        live_mode: If True, don't exit when collectors return None (for live streams).

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

    # Initialize router with live_mode flag
    budget_registry = BudgetRegistry()
    router = CollectorRouter(
        collectors=collectors,
        budget_registry=budget_registry,
        kill_switch_path=config.kill_switch_path,
        live_mode=live_mode,
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


async def run_live_with_processing(
    config: AppConfig,
    collectors: list,
    *,
    cursor_name: str = "default",
    poll_interval_s: float = 1.0,
    max_events: int | None = None,
) -> int:
    """Run live collection and replay processing in parallel.

    Continuously collects events from collectors and writes to SQLite,
    while simultaneously replaying new events into 1s bars.

    Args:
        config: Validated application configuration.
        collectors: List of CollectorBase instances.
        cursor_name: Cursor name for replay processor.
        poll_interval_s: Sleep interval when replay is caught up.
        max_events: Maximum events to collect. None for unlimited.

    Returns:
        0 on clean shutdown, 2 on kill switch or fatal error.
    """
    from ravebear_monolith.collectors.router import CollectorRouter
    from ravebear_monolith.core.replay_runner import ReplayRunner
    from ravebear_monolith.processors.okx.trades_to_bars_1s import TradesToBars1sProcessor
    from ravebear_monolith.util.kill_switch import KillSwitch
    from ravebear_monolith.util.rate_limit import BudgetRegistry

    # Configure logging at startup
    configure_logging(config)

    log_event(
        logger,
        logging.INFO,
        f"Starting {config.app_name} (live-with-processing)",
        event="orchestrator_start",
        mode="live-with-processing",
    )

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

    # Initialize event sink (single connection for entire lifecycle)
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

    # Track result code
    result_code = 0

    async def collector_loop() -> None:
        """Collect events with live_mode=True (never exits on idle)."""
        nonlocal result_code

        # Create router with live_mode=True - it will run forever until cancelled
        budget_registry = BudgetRegistry()
        router = CollectorRouter(
            collectors=collectors,
            budget_registry=budget_registry,
            kill_switch_path=config.kill_switch_path,
            live_mode=True,  # Don't exit when collectors return None
        )

        try:
            async for event in router.events(max_events=max_events):
                try:
                    await event_sink.write(event)
                except Exception as e:
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
                    result_code = 2
                    return

            # Only reaches here if max_events hit
            log_event(
                logger,
                logging.INFO,
                f"Collector loop complete, {router.event_count} events",
                event="collector_complete",
                event_count=router.event_count,
            )

        except asyncio.CancelledError:
            log_event(
                logger, logging.DEBUG, "Collector loop cancelled", event="collector_cancelled"
            )
            raise

    async def processing_loop() -> None:
        """Continuously replay new events into bars."""
        nonlocal result_code
        try:
            while not kill_switch.should_halt():
                # Create processor for this replay run
                processor = TradesToBars1sProcessor(
                    db_path=config.storage.db_path,
                    symbol_default=config.okx.inst_id,
                )
                runner = ReplayRunner(
                    db_path=config.storage.db_path,
                    cursor_name=cursor_name,
                    processor=processor,
                    kill_switch_path=config.kill_switch_path,
                )

                try:
                    exit_code = await runner.run()
                    if exit_code != 0:
                        log_event(
                            logger,
                            logging.WARNING,
                            f"Replay runner exited with code {exit_code}",
                            event="replay_exit",
                            exit_code=exit_code,
                        )
                        result_code = exit_code
                        return
                except Exception as e:
                    log_event(
                        logger,
                        logging.ERROR,
                        f"Replay processing error: {e}",
                        event="replay_error",
                        error=str(e),
                    )
                    result_code = 2
                    return

                # Sleep before next poll (replay is caught up)
                await asyncio.sleep(poll_interval_s)

        except asyncio.CancelledError:
            log_event(
                logger, logging.DEBUG, "Processing loop cancelled", event="processing_cancelled"
            )
            raise

    collector_task: asyncio.Task[None] | None = None
    processing_task: asyncio.Task[None] | None = None

    try:
        # Start both tasks
        collector_task = asyncio.create_task(collector_loop())
        processing_task = asyncio.create_task(processing_loop())

        # Wait for either task to complete (or error)
        done, pending = await asyncio.wait(
            [collector_task, processing_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Cancel remaining tasks
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Check for exceptions in completed tasks
        for task in done:
            exc = task.exception()
            if exc is not None:
                raise exc

        log_event(
            logger,
            logging.INFO,
            "Live-with-processing complete",
            event="orchestrator_complete",
        )

    except asyncio.CancelledError:
        log_event(logger, logging.INFO, "Orchestrator shutting down", event="orchestrator_stop")
        # Cancel both tasks on external cancellation
        if collector_task and not collector_task.done():
            collector_task.cancel()
            try:
                await collector_task
            except asyncio.CancelledError:
                pass
        if processing_task and not processing_task.done():
            processing_task.cancel()
            try:
                await processing_task
            except asyncio.CancelledError:
                pass

    finally:
        await event_sink.close()

    return result_code


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
    parser.add_argument(
        "--mode",
        choices=["live", "replay", "live-with-processing"],
        default="live",
        help="Run mode: live (heartbeat), replay (stored events), or live-with-processing",
    )
    parser.add_argument(
        "--cursor-name",
        default="default",
        help="Cursor name for replay/processing mode (default: default)",
    )
    parser.add_argument(
        "--poll-interval-s",
        type=float,
        default=1.0,
        help="Poll interval when replay is caught up (default: 1.0)",
    )
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"FATAL: Failed to load configuration: {e}", file=sys.stderr)
        return 1

    if args.mode == "replay":
        from ravebear_monolith.core.processor import NoopProcessor
        from ravebear_monolith.core.replay_runner import ReplayRunner

        runner = ReplayRunner(
            db_path=config.storage.db_path,
            cursor_name=args.cursor_name,
            processor=NoopProcessor(),
            kill_switch_path=config.kill_switch_path,
        )
        return asyncio.run(runner.run())

    if args.mode == "live-with-processing":
        from ravebear_monolith.collectors.okx.live import OKXTradesLiveCollector

        collectors = [OKXTradesLiveCollector(inst_id=config.okx.inst_id)]
        return asyncio.run(
            run_live_with_processing(
                config,
                collectors,
                cursor_name=args.cursor_name,
                poll_interval_s=args.poll_interval_s,
            )
        )

    return asyncio.run(run(config))
