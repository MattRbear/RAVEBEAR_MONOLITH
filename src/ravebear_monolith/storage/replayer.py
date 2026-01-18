"""Deterministic event replayer for restart-safe processing.

Reads events from EventReader using a cursor for exact resume position.
"""

from collections.abc import AsyncIterator
from typing import Annotated, Literal

from pydantic import BaseModel, Field

from ravebear_monolith.storage.cursor_store import CursorStore
from ravebear_monolith.storage.event_reader import EventReader, EventRow, QuerySpec


class ReplayerConfig(BaseModel, extra="forbid"):
    """Configuration for event replayer."""

    cursor_name: str
    chunk_size: Annotated[int, Field(ge=1, le=10_000)] = 1000
    max_events: int | None = None
    order: Literal["asc"] = "asc"  # Only ascending for determinism


class EventReplayer:
    """Deterministic event replayer with restart-safe cursor.

    Uses cursor to track last processed event and resumes exactly
    where processing left off. No duplicates across restarts.

    Args:
        reader: EventReader for querying events.
        cursors: CursorStore for cursor persistence.
        config: ReplayerConfig with settings.
    """

    def __init__(
        self,
        reader: EventReader,
        cursors: CursorStore,
        config: ReplayerConfig,
    ) -> None:
        self._reader = reader
        self._cursors = cursors
        self._config = config

    async def iter_events(self) -> AsyncIterator[EventRow]:
        """Iterate events from last cursor position.

        Yields events deterministically, skipping already-processed events
        based on cursor position. Uses (ts_ms, id) for stable ordering.

        Yields:
            EventRow for each unprocessed event.
        """
        cursor = await self._cursors.get(self._config.cursor_name)

        # Build query spec
        ts_min = cursor.last_ts_ms if cursor else None

        spec = QuerySpec(
            ts_min=ts_min,
            limit=50_000,  # Large limit, controlled by max_events
            order="asc",
        )

        yielded_count = 0

        async for event in self._reader.iter_query(spec, chunk_size=self._config.chunk_size):
            # Boundary deduplication
            if cursor is not None and event.ts_ms == cursor.last_ts_ms:
                # Same timestamp as cursor - need to check ID
                if event.id == cursor.last_event_id:
                    # Exact match - skip (already processed)
                    continue
                elif event.id < cursor.last_event_id:
                    # ID less than cursor - already processed (stable tie-break)
                    continue
                # ID greater than cursor - process normally

            # Check max_events limit
            if self._config.max_events is not None and yielded_count >= self._config.max_events:
                return

            yield event
            yielded_count += 1

    async def commit_cursor(self, event: EventRow) -> None:
        """Commit cursor after successful event processing.

        Must be called by consumer after each event is fully processed.

        Args:
            event: The successfully processed event.
        """
        await self._cursors.upsert(
            name=self._config.cursor_name,
            last_ts_ms=event.ts_ms,
            last_event_id=event.id,
        )
