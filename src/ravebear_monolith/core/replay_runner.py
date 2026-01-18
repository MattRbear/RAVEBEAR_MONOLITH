"""Replay runner for deterministic event processing.

Replays stored events through a processor with restart-safe cursor commits.
"""

import logging
from pathlib import Path

from ravebear_monolith.core.processor import (
    ProcessorBase,
    ProcessResult,  # Added
)
from ravebear_monolith.core.processor_router import FailurePolicy, ProcessorRouter
from ravebear_monolith.storage.cursor_store import CursorStore
from ravebear_monolith.storage.event_reader import EventReader
from ravebear_monolith.storage.replayer import EventReplayer, ReplayerConfig
from ravebear_monolith.util.logging import log_event

logger = logging.getLogger(__name__)


def _trigger_kill_switch(path: Path, reason: str) -> None:
    """Write kill switch file with reason."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"KILL\n{reason}", encoding="utf-8")
    except Exception:
        pass  # Best effort


def _get_failure_policy(processor: ProcessorBase) -> FailurePolicy:
    """Get failure policy from processor if it's a router."""
    if isinstance(processor, ProcessorRouter):
        return processor.policy
    return FailurePolicy.FAIL_CLOSED  # Default for non-routers


class ReplayRunner:
    """Runner for deterministic event replay with cursor-commit semantics.

    Processes events through a processor and commits cursor only on success.
    Exit codes:
        0: Success
        2: FAIL_CLOSED failure (kills switch written)
        3: BEST_EFFORT failure (no kill switch)

    Args:
        db_path: Path to SQLite database.
        cursor_name: Name of the replay cursor.
        processor: Processor to handle events.
        chunk_size: Events per chunk for streaming.
        max_events: Maximum events to process (None for all).
        kill_switch_path: Path to kill switch file.
    """

    def __init__(
        self,
        db_path: Path | str,
        *,
        cursor_name: str,
        processor: ProcessorBase,
        chunk_size: int = 1000,
        max_events: int | None = None,
        kill_switch_path: Path | None = None,
    ) -> None:
        self._db_path = Path(db_path)
        self._cursor_name = cursor_name
        self._processor = processor
        self._chunk_size = chunk_size
        self._max_events = max_events
        self._kill_switch_path = kill_switch_path or Path("config/kill_switch.txt")
        self._processed_count = 0
        self._policy = _get_failure_policy(processor)

    @property
    def processed_count(self) -> int:
        """Number of events processed."""
        return self._processed_count

    async def run(self) -> int:
        """Run the replay pipeline.

        Processes events through the processor, committing cursor after each
        successful processing.

        Returns:
            0 on success, 2 on FAIL_CLOSED error, 3 on BEST_EFFORT error.
        """
        async with EventReader(self._db_path) as reader, CursorStore(self._db_path) as cursors:
            log_event(
                logger,
                logging.INFO,
                f"Replay runner starting: cursor={self._cursor_name}",
                event="replay_runner_start",
                cursor_name=self._cursor_name,
            )

            # Get starting point for logging
            if self._cursor_name:
                cursor = await cursors.get(self._cursor_name)
                if cursor:
                    log_event(
                        logger,
                        logging.INFO,
                        f"Resuming from cursor {self._cursor_name} @ {cursor.last_ts_ms}",
                        event="replay_resume",
                        cursor_name=self._cursor_name,
                        last_ts_ms=cursor.last_ts_ms,
                    )

            # Replayer orchestration
            config = ReplayerConfig(
                cursor_name=self._cursor_name,
                chunk_size=self._chunk_size,
                max_events=self._max_events,
            )
            replayer = EventReplayer(reader, cursors, config)

            async for event in replayer.iter_events():
                # Loop control handled by replayer (cursor, max_events)

                try:
                    result = await self._processor.process(event)
                except Exception as e:
                    # Convert unhandled exception to failed result
                    result = ProcessResult(ok=False, reason=f"Exception: {e}")

                if not result.ok:
                    log_event(
                        logger,
                        logging.ERROR,
                        f"Processor failed: {result.reason}",
                        event="processor_failed",
                        event_id=event.id,
                        event_ts_ms=event.ts_ms,
                        reason=result.reason,
                    )
                    if self._policy == FailurePolicy.FAIL_CLOSED:
                        _trigger_kill_switch(
                            self._kill_switch_path,
                            f"Processor failed: {result.reason}",
                        )
                        await self._finalize_processor()
                        return 2
                    else:
                        # BEST_EFFORT: no kill switch, exit 3
                        await self._finalize_processor()
                        return 3

                # Commit cursor ONLY after successful processing
                try:
                    await replayer.commit_cursor(event)
                    self._processed_count += 1
                except Exception as e:
                    log_event(
                        logger,
                        logging.ERROR,
                        f"Cursor commit failed: {e}",
                        event="cursor_commit_failed",
                        event_id=event.id,
                        error=str(e),
                    )
                    if self._policy == FailurePolicy.FAIL_CLOSED:
                        _trigger_kill_switch(
                            self._kill_switch_path,
                            f"Cursor commit failed: {e}",
                        )
                        await self._finalize_processor()
                        return 2
                    else:
                        await self._finalize_processor()
                        return 3

            log_event(
                logger,
                logging.INFO,
                f"Replay runner complete: {self._processed_count} events",
                event="replay_runner_complete",
                processed_count=self._processed_count,
            )

            # Call processor finalize() if it has one
            exit_code = await self._finalize_processor()
            if exit_code != 0:
                return exit_code

            return 0

    async def _finalize_processor(self) -> int:
        """Call processor finalize() if it exists.

        Returns:
            0 on success, 2/3 on error (policy-dependent).
        """
        if hasattr(self._processor, "finalize") and callable(self._processor.finalize):
            try:
                result = await self._processor.finalize()
                # Check if finalize returned a result with ok=False
                if result is not None and hasattr(result, "ok") and not result.ok:
                    reason = getattr(result, "outcomes", [])
                    log_event(
                        logger,
                        logging.ERROR,
                        f"Processor finalize failed: {reason}",
                        event="processor_finalize_failed",
                        outcomes=str(reason),
                    )
                    if self._policy == FailurePolicy.FAIL_CLOSED:
                        _trigger_kill_switch(
                            self._kill_switch_path,
                            f"Processor finalize failed: {reason}",
                        )
                        return 2
                    else:
                        return 3
            except Exception as e:
                log_event(
                    logger,
                    logging.ERROR,
                    f"Processor finalize exception: {e}",
                    event="processor_finalize_exception",
                    error=str(e),
                )
                if self._policy == FailurePolicy.FAIL_CLOSED:
                    _trigger_kill_switch(
                        self._kill_switch_path,
                        f"Processor finalize failed: {e}",
                    )
                    return 2
                else:
                    return 3
        return 0
