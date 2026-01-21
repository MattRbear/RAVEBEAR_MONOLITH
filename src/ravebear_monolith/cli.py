"""CLI entry points for Ravebear Monolith.

Provides signal handling for graceful shutdown on SIGINT/SIGTERM.
Works on Windows (KeyboardInterrupt fallback) and Unix (signal handlers).

Exit codes:
- 0: Clean shutdown
- 1: Error (config failure, fatal exception)
- 2: Kill switch triggered
- 130: Interrupted by SIGINT/Ctrl+C (128 + 2)
"""

import argparse
import asyncio
import signal
import sys
from pathlib import Path

from ravebear_monolith.foundation import orchestrator

# Exit code constants
EXIT_OK = 0
EXIT_ERROR = 1
EXIT_KILL_SWITCH = 2
EXIT_SIGINT = 130  # 128 + SIGINT(2)


def _is_cancellation(exc: BaseException) -> bool:
    """Check if exception represents cancellation/interrupt.

    Handles:
    - asyncio.CancelledError
    - KeyboardInterrupt
    - ExceptionGroup containing CancelledError (Python 3.11+)
    """
    if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt)):
        return True

    # Python 3.11+ can wrap CancelledError in ExceptionGroup
    if isinstance(exc, BaseExceptionGroup):
        for sub_exc in exc.exceptions:
            if _is_cancellation(sub_exc):
                return True

    return False


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
        return EXIT_ERROR

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

    # Track if we received an interrupt
    interrupted = False

    def signal_handler() -> None:
        """Cancel main task on signal."""
        nonlocal interrupted
        interrupted = True
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
        result = await main_task
        # If we got interrupted but task returned cleanly, still return 130
        return EXIT_SIGINT if interrupted else result
    except BaseException as e:
        if _is_cancellation(e):
            return EXIT_SIGINT
        raise


def cli_main(argv: list[str] | None = None) -> int:
    """Main CLI entry point with graceful shutdown.

    Handles all forms of cancellation/interrupt on all platforms.
    """
    try:
        return asyncio.run(_run_orchestrator_async(argv))
    except BaseException as e:
        # Catch everything that might represent cancellation
        if _is_cancellation(e):
            return EXIT_SIGINT
        # Re-raise actual errors
        raise


def main() -> None:
    """Entry point for poetry scripts.

    Uses sys.exit() directly to ensure correct exit code.
    """
    try:
        code = cli_main()
        sys.exit(code)
    except BaseException as e:
        if _is_cancellation(e):
            # Force exit code 130 on any cancellation that escapes
            sys.exit(EXIT_SIGINT)
        # Let other exceptions propagate (will exit with code 1)
        raise
