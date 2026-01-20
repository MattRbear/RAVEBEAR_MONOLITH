"""CLI entry points for Ravebear Monolith.

Provides signal handling for graceful shutdown on SIGINT/SIGTERM.
Works on Windows (KeyboardInterrupt fallback) and Unix (signal handlers).
"""

import argparse
import asyncio
import signal
import sys
from pathlib import Path

from ravebear_monolith.foundation import orchestrator


async def _run_orchestrator_async(argv: list[str] | None = None) -> int:
    """Run orchestrator with signal handling.

    Creates a task for the orchestrator and installs signal handlers
    that cancel the task on SIGINT/SIGTERM.

    Args:
        argv: Command-line arguments to pass to orchestrator.

    Returns:
        Exit code from orchestrator.
    """
    loop = asyncio.get_running_loop()

    # Create the main task
    # Note: orchestrator.main() is sync and calls asyncio.run() internally,
    # so we need to run the async functions directly
    from ravebear_monolith.foundation.config import load_config

    # Parse arguments to get config and mode
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

    # Create the appropriate coroutine based on mode
    if args.mode == "replay":
        from ravebear_monolith.core.processor import NoopProcessor
        from ravebear_monolith.core.replay_runner import ReplayRunner

        runner = ReplayRunner(
            db_path=config.storage.db_path,
            cursor_name=args.cursor_name,
            processor=NoopProcessor(),
            kill_switch_path=config.kill_switch_path,
        )
        coro = runner.run()
    elif args.mode == "live-with-processing":
        from ravebear_monolith.collectors.okx.live import OKXTradesLiveCollector

        collectors = [OKXTradesLiveCollector(inst_id=config.okx.inst_id)]
        coro = orchestrator.run_live_with_processing(
            config,
            collectors,
            cursor_name=args.cursor_name,
            poll_interval_s=args.poll_interval_s,
        )
    else:
        coro = orchestrator.run(config)

    main_task = asyncio.create_task(coro)

    # Install signal handlers (Unix only - Windows uses KeyboardInterrupt)
    def signal_handler() -> None:
        """Cancel main task on signal."""
        if not main_task.done():
            main_task.cancel()

    # Try to install signal handlers (may not work on Windows)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except (NotImplementedError, ValueError):
            # Windows doesn't support add_signal_handler for SIGINT
            # ValueError can occur if not on main thread
            pass

    try:
        return await main_task
    except asyncio.CancelledError:
        return 130  # Standard exit code for SIGINT


def cli_main(argv: list[str] | None = None) -> int:
    """Main CLI entry point with graceful shutdown.

    Handles KeyboardInterrupt as fallback for Windows.
    """
    try:
        return asyncio.run(_run_orchestrator_async(argv))
    except KeyboardInterrupt:
        # Windows fallback - KeyboardInterrupt is raised instead of signal
        return 130


def main() -> None:
    """Entry point for poetry scripts."""
    sys.exit(cli_main())
