"""Replay runner for deterministic event processing.

Replays stored events through a processor with restart-safe cursor commits.
"""

import logging
from pathlib import Path

from ravebear_monolith.core.processor import ProcessorBase
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


class ReplayRunner:
    """Runner for deterministic event replay with cursor-commit semantics.

    Processes events through a processor and commits cursor only on success.
    Fail-closed: any exception triggers kill switch and non-zero exit.

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

    @property
    def processed_count(self) -> int:
        """Number of events processed."""
        return self._processed_count

    async def run(self) -> int:
        """Run the replay pipeline.

        Processes events through the processor, committing cursor after each
        successful processing. Fail-closed on any error.

        Returns:
            0 on success, 2 on error.
        """
        reader = EventReader(self._db_path)
        cursors = CursorStore(self._db_path)

        try:
            await reader.connect()
            await cursors.connect()

            config = ReplayerConfig(
                cursor_name=self._cursor_name,
                chunk_size=self._chunk_size,
                max_events=self._max_events,
            )
            replayer = EventReplayer(reader, cursors, config)

            log_event(
                logger,
                logging.INFO,
                f"Replay runner starting: cursor={self._cursor_name}",
                event="replay_runner_start",
                cursor_name=self._cursor_name,
            )

            async for event in replayer.iter_events():
                try:
                    result = await self._processor.process(event)
                except Exception as e:
                    # Exception in processor - fail-closed
                    log_event(
                        logger,
                        logging.ERROR,
                        f"Processor exception: {e}",
                        event="processor_exception",
                        event_id=event.id,
                        event_ts_ms=event.ts_ms,
                        error=str(e),
                    )
                    _trigger_kill_switch(
                        self._kill_switch_path,
                        f"Processor exception on event {event.id}: {e}",
                    )
                    return 2

                if not result.ok:
                    # Processor returned failure - fail-closed
                    log_event(
                        logger,
                        logging.ERROR,
                        f"Processor failed: {result.reason}",
                        event="processor_failed",
                        event_id=event.id,
                        event_ts_ms=event.ts_ms,
                        reason=result.reason,
                    )
                    _trigger_kill_switch(
                        self._kill_switch_path,
                        f"Processor failed on event {event.id}: {result.reason}",
                    )
                    return 2

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
                    _trigger_kill_switch(
                        self._kill_switch_path,
                        f"Cursor commit failed: {e}",
                    )
                    return 2

            log_event(
                logger,
                logging.INFO,
                f"Replay runner complete: {self._processed_count} events",
                event="replay_runner_complete",
                processed_count=self._processed_count,
            )
            return 0

        finally:
            await cursors.close()
            await reader.close()
