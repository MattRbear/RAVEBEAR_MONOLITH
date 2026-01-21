"""Collector router for fan-in event aggregation.

Provides:
- CollectorRouter: Aggregates events from multiple collectors with
  rate limiting, retry, and kill switch support.
"""

import asyncio
import logging
from collections.abc import AsyncGenerator
from pathlib import Path

from ravebear_monolith.collectors.base import CollectorBase, CollectorEvent
from ravebear_monolith.util.kill_switch import KillSwitch
from ravebear_monolith.util.logging import log_event
from ravebear_monolith.util.rate_limit import BudgetRegistry
from ravebear_monolith.util.retry import RetryPolicy, classify_error, retry_async

logger = logging.getLogger(__name__)


class CollectorRouter:
    """Router that aggregates events from multiple collectors.

    Features:
    - Fan-in from multiple collectors
    - Rate limiting via BudgetRegistry
    - Retry on transient errors
    - Kill switch monitoring

    Args:
        collectors: List of collectors to aggregate.
        budget_registry: Rate limit budget registry.
        kill_switch_path: Path to kill switch file.
        retry_policy: Policy for retrying failed next_event calls.
        live_mode: If True, don't exit when collectors return None (they may be idle).
    """

    def __init__(
        self,
        collectors: list[CollectorBase],
        budget_registry: BudgetRegistry,
        kill_switch_path: Path,
        retry_policy: RetryPolicy | None = None,
        live_mode: bool = False,
    ) -> None:
        self._collectors = collectors
        self._budget = budget_registry
        self._kill_switch = KillSwitch(kill_switch_path)
        self._retry_policy = retry_policy or RetryPolicy()
        self._live_mode = live_mode
        self._running = False
        self._event_count = 0

    @property
    def event_count(self) -> int:
        """Number of events processed."""
        return self._event_count

    async def start_collectors(self) -> None:
        """Start all registered collectors."""
        for collector in self._collectors:
            await collector.start()
            log_event(
                logger,
                logging.INFO,
                f"Started collector: {collector.name}",
                event="collector_started",
                collector=collector.name,
            )

    async def stop_collectors(self) -> None:
        """Stop all registered collectors."""
        for collector in self._collectors:
            try:
                await collector.stop()
                log_event(
                    logger,
                    logging.INFO,
                    f"Stopped collector: {collector.name}",
                    event="collector_stopped",
                    collector=collector.name,
                )
            except Exception as e:
                log_event(
                    logger,
                    logging.WARNING,
                    f"Error stopping collector {collector.name}: {e}",
                    event="collector_stop_error",
                    collector=collector.name,
                    error=str(e),
                )

    async def _get_event_with_retry(self, collector: CollectorBase) -> CollectorEvent | None:
        """Get next event from collector with retry on transient errors."""

        async def _fetch() -> CollectorEvent | None:
            return await collector.next_event()

        def _on_retry(attempt: int, classification: str, delay: float, exc: Exception) -> None:
            log_event(
                logger,
                logging.WARNING,
                f"Retry {attempt} for {collector.name}: {exc}",
                event="collector_retry",
                collector=collector.name,
                attempt=attempt,
                classification=classification,
                delay_s=delay,
            )

        try:
            return await retry_async(_fetch, policy=self._retry_policy, on_attempt=_on_retry)
        except Exception as e:
            classification = classify_error(e)
            if classification == "fatal":
                log_event(
                    logger,
                    logging.ERROR,
                    f"Fatal error in {collector.name}: {e}",
                    event="collector_fatal_error",
                    collector=collector.name,
                    error=str(e),
                )
                raise
            # Transient errors after max retries: log and return None
            log_event(
                logger,
                logging.ERROR,
                f"Max retries exceeded for {collector.name}: {e}",
                event="collector_max_retries",
                collector=collector.name,
                error=str(e),
            )
            return None

    async def run(self, max_events: int | None = None) -> int:
        """Run the router, collecting events from all collectors.

        Args:
            max_events: Maximum events to process. None for unlimited.

        Returns:
            0 on clean exit, 2 on kill switch.
        """
        self._running = True
        self._event_count = 0

        log_event(
            logger,
            logging.INFO,
            f"Router starting with {len(self._collectors)} collectors",
            event="router_start",
            collector_count=len(self._collectors),
        )

        try:
            await self.start_collectors()

            while self._running:
                # Check kill switch
                if self._kill_switch.should_halt():
                    log_event(
                        logger,
                        logging.WARNING,
                        "Kill switch triggered",
                        event="kill_switch_triggered",
                        reason=self._kill_switch.reason(),
                    )
                    return 2

                # Check max events
                if max_events is not None and self._event_count >= max_events:
                    log_event(
                        logger,
                        logging.INFO,
                        f"Max events reached: {max_events}",
                        event="router_max_events",
                        max_events=max_events,
                    )
                    break

                # Collect from all collectors (round-robin)
                events_this_round = 0
                exhausted_count = 0
                for collector in self._collectors:
                    # Rate limit check
                    if "collector" in self._budget:
                        await self._budget.get("collector").acquire()

                    event = await self._get_event_with_retry(collector)
                    if event:
                        self._event_count += 1
                        events_this_round += 1
                        log_event(
                            logger,
                            logging.DEBUG,
                            f"Event from {collector.name}: {event.event_type}",
                            event="collector_event",
                            collector=collector.name,
                            event_type=event.event_type,
                            source=event.source,
                        )

                        # Check max events after each event
                        if max_events is not None and self._event_count >= max_events:
                            break
                    else:
                        exhausted_count += 1

                # In live mode, don't exit on "all None" - collectors are just idle
                # In batch mode (max_events set or not live_mode), exit when exhausted
                if not self._live_mode:
                    if exhausted_count == len(self._collectors) and len(self._collectors) > 0:
                        log_event(
                            logger,
                            logging.INFO,
                            "All collectors exhausted",
                            event="router_exhausted",
                        )
                        break

                # If no events from any collector, yield control briefly
                if events_this_round == 0:
                    await asyncio.sleep(0.01)

        except asyncio.CancelledError:
            log_event(logger, logging.INFO, "Router cancelled", event="router_cancelled")
            raise
        finally:
            self._running = False
            await self.stop_collectors()
            log_event(
                logger,
                logging.INFO,
                f"Router stopped, processed {self._event_count} events",
                event="router_stop",
                event_count=self._event_count,
            )

        return 0

    def stop(self) -> None:
        """Signal the router to stop."""
        self._running = False

    async def events(self, max_events: int | None = None) -> AsyncGenerator[CollectorEvent, None]:
        """Async generator that yields events from collectors.

        This allows external code (like orchestrator) to process events
        without the router owning storage.

        Args:
            max_events: Maximum events to yield. None for unlimited.

        Yields:
            CollectorEvent from collectors.
        """
        self._running = True
        self._event_count = 0

        log_event(
            logger,
            logging.INFO,
            f"Router starting with {len(self._collectors)} collectors",
            event="router_start",
            collector_count=len(self._collectors),
            live_mode=self._live_mode,
        )

        try:
            await self.start_collectors()

            while self._running:
                # Check kill switch
                if self._kill_switch.should_halt():
                    log_event(
                        logger,
                        logging.WARNING,
                        "Kill switch triggered",
                        event="kill_switch_triggered",
                        reason=self._kill_switch.reason(),
                    )
                    return

                # Check max events
                if max_events is not None and self._event_count >= max_events:
                    log_event(
                        logger,
                        logging.INFO,
                        f"Max events reached: {max_events}",
                        event="router_max_events",
                        max_events=max_events,
                    )
                    return

                # Collect from all collectors (round-robin)
                events_this_round = 0
                exhausted_count = 0
                for collector in self._collectors:
                    # Rate limit check
                    if "collector" in self._budget:
                        await self._budget.get("collector").acquire()

                    event = await self._get_event_with_retry(collector)
                    if event:
                        self._event_count += 1
                        events_this_round += 1
                        log_event(
                            logger,
                            logging.DEBUG,
                            f"Event from {collector.name}: {event.event_type}",
                            event="collector_event",
                            collector=collector.name,
                            event_type=event.event_type,
                            source=event.source,
                        )
                        yield event

                        # Check max events after each event
                        if max_events is not None and self._event_count >= max_events:
                            return
                    else:
                        exhausted_count += 1

                # In live mode, don't exit on "all None" - collectors are just idle
                # In batch mode (not live_mode), exit when all collectors return None
                if not self._live_mode:
                    if exhausted_count == len(self._collectors) and len(self._collectors) > 0:
                        log_event(
                            logger,
                            logging.INFO,
                            "All collectors exhausted",
                            event="router_exhausted",
                        )
                        return

                # If no events from any collector, yield control briefly
                if events_this_round == 0:
                    await asyncio.sleep(0.01)

        except asyncio.CancelledError:
            log_event(logger, logging.INFO, "Router cancelled", event="router_cancelled")
            raise
        finally:
            self._running = False
            await self.stop_collectors()
            log_event(
                logger,
                logging.INFO,
                f"Router stopped, processed {self._event_count} events",
                event="router_stop",
                event_count=self._event_count,
            )
