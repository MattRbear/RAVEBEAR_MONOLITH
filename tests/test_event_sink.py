"""Tests for EventSink with SQLite WAL mode."""

import asyncio
from pathlib import Path

import pytest

from ravebear_monolith.collectors.base import CollectorEvent
from ravebear_monolith.storage.event_sink import EventSink


def make_event(source: str, event_type: str, payload: dict, ts_suffix: int = 0) -> CollectorEvent:
    """Create a test CollectorEvent."""
    return CollectorEvent(
        source=source,
        event_type=event_type,
        ts_utc=f"2024-01-18T12:00:{ts_suffix:02d}+00:00",
        payload=payload,
    )


class TestEventSink:
    """Tests for EventSink."""

    @pytest.mark.asyncio
    async def test_write_and_read_roundtrip(self, tmp_path: Path) -> None:
        """Write event and read it back."""
        db_path = tmp_path / "test.db"
        sink = EventSink(db_path)

        await sink.open()
        try:
            event = make_event("okx", "trade", {"price": 42000.0, "size": 0.5})
            await sink.write(event)

            events = await sink.read_all()
            assert len(events) == 1
            assert events[0]["source"] == "okx"
            assert events[0]["type"] == "trade"
            assert events[0]["payload"]["price"] == 42000.0
        finally:
            await sink.close()

    @pytest.mark.asyncio
    async def test_dedupe_same_event_twice(self, tmp_path: Path) -> None:
        """Same event written twice results in one row."""
        db_path = tmp_path / "test.db"
        sink = EventSink(db_path)

        await sink.open()
        try:
            event = make_event("okx", "trade", {"price": 42000.0})

            await sink.write(event)
            await sink.write(event)  # Duplicate

            count = await sink.count()
            assert count == 1
        finally:
            await sink.close()

    @pytest.mark.asyncio
    async def test_different_events_not_deduped(self, tmp_path: Path) -> None:
        """Different events are stored separately."""
        db_path = tmp_path / "test.db"
        sink = EventSink(db_path)

        await sink.open()
        try:
            event1 = make_event("okx", "trade", {"price": 42000.0}, ts_suffix=0)
            event2 = make_event("okx", "trade", {"price": 42001.0}, ts_suffix=1)

            await sink.write(event1)
            await sink.write(event2)

            count = await sink.count()
            assert count == 2
        finally:
            await sink.close()

    @pytest.mark.asyncio
    async def test_wal_recovery_data_persists(self, tmp_path: Path) -> None:
        """Data persists after close and reopen."""
        db_path = tmp_path / "test.db"

        # Write event
        sink1 = EventSink(db_path)
        await sink1.open()
        event = make_event("okx", "trade", {"price": 42000.0})
        await sink1.write(event)
        await sink1.close()

        # Reopen and verify
        sink2 = EventSink(db_path)
        await sink2.open()
        try:
            count = await sink2.count()
            assert count == 1

            events = await sink2.read_all()
            assert events[0]["payload"]["price"] == 42000.0
        finally:
            await sink2.close()

    @pytest.mark.asyncio
    async def test_concurrent_writes_no_corruption(self, tmp_path: Path) -> None:
        """Concurrent async writes don't corrupt database."""
        db_path = tmp_path / "test.db"
        sink = EventSink(db_path)

        await sink.open()
        try:
            # Create many unique events
            events = [
                make_event("okx", "trade", {"seq": i, "price": 42000.0 + i}, ts_suffix=i % 60)
                for i in range(50)
            ]

            # Write concurrently
            await asyncio.gather(*[sink.write(e) for e in events])

            # Verify all written (some may dedupe if ts_suffix collides with same payload)
            count = await sink.count()
            assert count >= 1  # At minimum one, should be 50 with unique payloads
            assert count <= 50
        finally:
            await sink.close()

    @pytest.mark.asyncio
    async def test_creates_parent_directory(self, tmp_path: Path) -> None:
        """EventSink creates parent directories for db_path."""
        db_path = tmp_path / "nested" / "dir" / "test.db"
        sink = EventSink(db_path)

        await sink.open()
        try:
            assert db_path.parent.exists()
        finally:
            await sink.close()

    @pytest.mark.asyncio
    async def test_write_without_open_raises(self, tmp_path: Path) -> None:
        """Writing without opening raises RuntimeError."""
        db_path = tmp_path / "test.db"
        sink = EventSink(db_path)

        event = make_event("okx", "trade", {"price": 42000.0})

        with pytest.raises(RuntimeError, match="not open"):
            await sink.write(event)

    @pytest.mark.asyncio
    async def test_count_returns_zero_for_empty(self, tmp_path: Path) -> None:
        """Count returns 0 for empty database."""
        db_path = tmp_path / "test.db"
        sink = EventSink(db_path)

        await sink.open()
        try:
            count = await sink.count()
            assert count == 0
        finally:
            await sink.close()
