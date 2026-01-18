"""
Multi-venue candle collector entry point.
Supports Coinbase, Kraken, and OKX with WebSocket + REST.
"""
import argparse
import asyncio
import logging
import signal

from .config import load_config
from .runtime_multi import MultiVenueRuntime
from .utils.backoff import Backoff
from .utils.logging import setup_logging


async def _run_once(config, stop_event):
    """Run the collector once."""
    runtime = MultiVenueRuntime(config)
    try:
        await runtime.run(stop_event)
    finally:
        await runtime.close()


def _install_signal_handlers(stop_event):
    """Install signal handlers for graceful shutdown."""
    def _handler(signum, _frame):
        logging.getLogger(__name__).info("event=signal_received signum=%s", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


async def _run_with_restart(config, stop_event):
    """Run with automatic restart on failure."""
    logger = logging.getLogger(__name__)
    backoff = Backoff()
    
    while not stop_event.is_set():
        try:
            await _run_once(config, stop_event)
            if stop_event.is_set():
                break
            raise RuntimeError("collector_run_exited")
        except Exception as exc:
            if stop_event.is_set():
                break
            delay = backoff.next_delay()
            logger.error("event=collector_restart error=%s delay_sec=%.2f", exc, delay)
            await asyncio.sleep(delay)
    
    logger.info("event=collector_shutdown")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        prog="multi-venue-collector",
        description="Multi-venue candle collector (Coinbase, Kraken, OKX)"
    )
    sub = parser.add_subparsers(dest="command")
    run_parser = sub.add_parser("run", help="Run the multi-venue collector")
    run_parser.add_argument("--config", required=True, help="Path to config file")

    args = parser.parse_args()
    if args.command != "run":
        parser.print_help()
        return

    config = load_config(args.config)
    setup_logging(config.logging)

    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("MULTI-VENUE CANDLE COLLECTOR")
    logger.info("=" * 80)
    logger.info("Enabled venues: %s", [name for name, cfg in config.venues.items() if cfg.enabled])
    logger.info("Timeframes: %s", config.timeframes)
    logger.info("WebSocket timeframes: %s", config.ws_timeframes)
    logger.info("Derived timeframes: %s", config.derive_timeframes)
    logger.info("Gap detection: %s (lookback: %d days)", 
               "enabled" if config.gap_detection.enabled else "disabled",
               config.gap_detection.lookback_days)
    logger.info("=" * 80)

    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)

    asyncio.run(_run_with_restart(config, stop_event))


if __name__ == "__main__":
    main()
