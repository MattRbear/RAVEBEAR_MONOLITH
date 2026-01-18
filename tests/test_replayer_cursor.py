"""Tests for replay cursor and deterministic replayer."""

from pathlib import Path

import pytest

from ravebear_monolith.collectors.base import CollectorEvent
from ravebear_monolith.storage.cursor_store import CursorStore, ReplayCursor
from ravebear_monolith.storage.event_reader import EventReader
from ravebear_monolith.storage.event_sink import EventSink
from ravebear_monolith.storage.replayer import EventReplayer, ReplayerConfig


def make_event(source: str, event_type: str, payload: dict, ts_suffix: int = 0) -> CollectorEvent:
    """Create a test CollectorEvent."""
    return CollectorEvent(
        source=source,
        event_type=event_type,
        ts_utc=f"2024-01-18T12:00:{ts_suffix:02d}+00:00",
        payload=payload,
    )


async def seed_events(db_path: Path, events: list[CollectorEvent]) -> None:
    """Seed database with events via EventSink."""
    sink = EventSink(db_path)
    await sink.open()
    try:
        for event in events:
            await sink.write(event)
    finally:
        await sink.close()


class TestCursorStore:
    """Tests for CursorStore."""

    @pytest.mark.asyncio
    async def test_cursor_upsert_and_get_roundtrip(self, tmp_path: Path) -> None:
        """Cursor can be upserted and retrieved."""
        db_path = tmp_path / "test.db"

        # Initialize schema via EventSink
        sink = EventSink(db_path)
        await sink.open()
        await sink.close()

        cursors = CursorStore(db_path)
        await cursors.connect()
        try:
            # Initially no cursor
            cursor = await cursors.get("test_cursor")
            assert cursor is None

            # Upsert cursor
            await cursors.upsert("test_cursor", last_ts_ms=1705536000000, last_event_id="abc123")

            # Retrieve it
            cursor = await cursors.get("test_cursor")
            assert cursor is not None
            assert isinstance(cursor, ReplayCursor)
            assert cursor.name == "test_cursor"
            assert cursor.last_ts_ms == 1705536000000
            assert cursor.last_event_id == "abc123"

            # Update cursor
            await cursors.upsert("test_cursor", last_ts_ms=1705536001000, last_event_id="def456")
            cursor = await cursors.get("test_cursor")
            assert cursor.last_ts_ms == 1705536001000
            assert cursor.last_event_id == "def456"
        finally:
            await cursors.close()


class TestEventReplayer:
    """Tests for EventReplayer."""

    @pytest.mark.asyncio
    async def test_starts_from_beginning_when_no_cursor(self, tmp_path: Path) -> None:
        """Replayer starts from beginning when no cursor exists."""
        db_path = tmp_path / "test.db"
        events = [make_event("okx", "trade", {"seq": i}, ts_suffix=i) for i in range(5)]
        await seed_events(db_path, events)

        reader = EventReader(db_path)
        await reader.connect()
        cursors = CursorStore(db_path)
        await cursors.connect()

        try:
            config = ReplayerConfig(cursor_name="test")
            replayer = EventReplayer(reader, cursors, config)

            results = []
            async for event in replayer.iter_events():
                results.append(event)

            assert len(results) == 5
        finally:
            await reader.close()
            await cursors.close()

    @pytest.mark.asyncio
    async def test_resumes_after_cursor(self, tmp_path: Path) -> None:
        """Replayer resumes at N+1 after processing N events."""
        db_path = tmp_path / "test.db"
        events = [make_event("okx", "trade", {"seq": i}, ts_suffix=i) for i in range(10)]
        await seed_events(db_path, events)

        reader = EventReader(db_path)
        await reader.connect()
        cursors = CursorStore(db_path)
        await cursors.connect()

        try:
            config = ReplayerConfig(cursor_name="test", max_events=3)
            replayer = EventReplayer(reader, cursors, config)

            # Process first 3 events
            processed = []
            async for event in replayer.iter_events():
                processed.append(event)
                await replayer.commit_cursor(event)

            assert len(processed) == 3

            # Resume - should get next 3
            config2 = ReplayerConfig(cursor_name="test", max_events=3)
            replayer2 = EventReplayer(reader, cursors, config2)

            resumed = []
            async for event in replayer2.iter_events():
                resumed.append(event)

            assert len(resumed) == 3
            # Should not overlap
            assert processed[-1].id != resumed[0].id
        finally:
            await reader.close()
            await cursors.close()

    @pytest.mark.asyncio
    async def test_boundary_dedup_same_event_not_reemitted(self, tmp_path: Path) -> None:
        """Event at cursor position is not re-emitted."""
        db_path = tmp_path / "test.db"
        events = [make_event("okx", "trade", {"seq": i}, ts_suffix=i) for i in range(5)]
        await seed_events(db_path, events)

        reader = EventReader(db_path)
        await reader.connect()
        cursors = CursorStore(db_path)
        await cursors.connect()

        try:
            # Process all and commit last
            config = ReplayerConfig(cursor_name="test")
            replayer = EventReplayer(reader, cursors, config)

            async for event in replayer.iter_events():
                await replayer.commit_cursor(event)

            # Resume - should get nothing (all processed)
            config2 = ReplayerConfig(cursor_name="test")
            replayer2 = EventReplayer(reader, cursors, config2)

            count = 0
            async for _ in replayer2.iter_events():
                count += 1

            assert count == 0
        finally:
            await reader.close()
            await cursors.close()

    @pytest.mark.asyncio
    async def test_deterministic_tie_break_same_ts_orders_by_id(self, tmp_path: Path) -> None:
        """Events with same timestamp are ordered by ID."""
        db_path = tmp_path / "test.db"
        # Create events with same timestamp but different payloads (different IDs)
        events = [make_event("okx", "trade", {"seq": i}, ts_suffix=5) for i in range(5)]
        await seed_events(db_path, events)

        reader = EventReader(db_path)
        await reader.connect()
        cursors = CursorStore(db_path)
        await cursors.connect()

        try:
            config = ReplayerConfig(cursor_name="test")
            replayer = EventReplayer(reader, cursors, config)

            ids = []
            async for event in replayer.iter_events():
                ids.append(event.id)

            # IDs should be sorted
            assert ids == sorted(ids)
        finally:
            await reader.close()
            await cursors.close()

    @pytest.mark.asyncio
    async def test_max_events_stops_early(self, tmp_path: Path) -> None:
        """max_events limits number of events yielded."""
        db_path = tmp_path / "test.db"
        events = [make_event("okx", "trade", {"seq": i}, ts_suffix=i) for i in range(10)]
        await seed_events(db_path, events)

        reader = EventReader(db_path)
        await reader.connect()
        cursors = CursorStore(db_path)
        await cursors.connect()

        try:
            config = ReplayerConfig(cursor_name="test", max_events=3)
            replayer = EventReplayer(reader, cursors, config)

            count = 0
            async for _ in replayer.iter_events():
                count += 1

            assert count == 3
        finally:
            await reader.close()
            await cursors.close()
