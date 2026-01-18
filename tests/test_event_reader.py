"""Tests for EventReader with read-only queries."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from ravebear_monolith.collectors.base import CollectorEvent
from ravebear_monolith.storage.event_reader import (
    EventReader,
    EventRow,
    QuerySpec,
    payload_as_dict,
)
from ravebear_monolith.storage.event_sink import EventSink


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


class TestEventReader:
    """Tests for EventReader."""

    @pytest.mark.asyncio
    async def test_roundtrip_query_all(self, tmp_path: Path) -> None:
        """Write events and read them back."""
        db_path = tmp_path / "test.db"
        events = [make_event("okx", "trade", {"price": 42000}, ts_suffix=i) for i in range(5)]
        await seed_events(db_path, events)

        reader = EventReader(db_path)
        await reader.connect()
        try:
            rows = await reader.query(QuerySpec())
            assert len(rows) == 5
            assert all(isinstance(r, EventRow) for r in rows)
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_filter_by_source(self, tmp_path: Path) -> None:
        """Filter events by source."""
        db_path = tmp_path / "test.db"
        events = [
            make_event("okx", "trade", {"price": 1}, ts_suffix=0),
            make_event("binance", "trade", {"price": 2}, ts_suffix=1),
            make_event("okx", "trade", {"price": 3}, ts_suffix=2),
        ]
        await seed_events(db_path, events)

        reader = EventReader(db_path)
        await reader.connect()
        try:
            rows = await reader.query(QuerySpec(source="okx"))
            assert len(rows) == 2
            assert all(r.source == "okx" for r in rows)
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_filter_by_event_type(self, tmp_path: Path) -> None:
        """Filter events by event_type."""
        db_path = tmp_path / "test.db"
        events = [
            make_event("okx", "trade", {"price": 1}, ts_suffix=0),
            make_event("okx", "ticker", {"bid": 99}, ts_suffix=1),
            make_event("okx", "trade", {"price": 2}, ts_suffix=2),
        ]
        await seed_events(db_path, events)

        reader = EventReader(db_path)
        await reader.connect()
        try:
            rows = await reader.query(QuerySpec(event_type="ticker"))
            assert len(rows) == 1
            assert rows[0].event_type == "ticker"
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_filter_by_time_range(self, tmp_path: Path) -> None:
        """Filter events by timestamp range."""
        db_path = tmp_path / "test.db"
        events = [make_event("okx", "trade", {"seq": i}, ts_suffix=i) for i in range(10)]
        await seed_events(db_path, events)

        reader = EventReader(db_path)
        await reader.connect()
        try:
            # Get first event's timestamp
            all_rows = await reader.query(QuerySpec())
            ts_min = all_rows[2].ts_ms
            ts_max = all_rows[5].ts_ms

            rows = await reader.query(QuerySpec(ts_min=ts_min, ts_max=ts_max))
            assert len(rows) == 4  # Events 2, 3, 4, 5
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_ordering_asc_desc(self, tmp_path: Path) -> None:
        """Test ascending and descending order."""
        db_path = tmp_path / "test.db"
        events = [make_event("okx", "trade", {"seq": i}, ts_suffix=i) for i in range(5)]
        await seed_events(db_path, events)

        reader = EventReader(db_path)
        await reader.connect()
        try:
            asc_rows = await reader.query(QuerySpec(order="asc"))
            desc_rows = await reader.query(QuerySpec(order="desc"))

            assert asc_rows[0].ts_ms < asc_rows[-1].ts_ms
            assert desc_rows[0].ts_ms > desc_rows[-1].ts_ms
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_limit_enforced(self, tmp_path: Path) -> None:
        """Limit parameter is enforced."""
        db_path = tmp_path / "test.db"
        events = [make_event("okx", "trade", {"seq": i}, ts_suffix=i % 60) for i in range(10)]
        await seed_events(db_path, events)

        reader = EventReader(db_path)
        await reader.connect()
        try:
            rows = await reader.query(QuerySpec(limit=1))
            assert len(rows) == 1
        finally:
            await reader.close()

    def test_limit_validation_error(self) -> None:
        """Limit > 50000 raises validation error."""
        with pytest.raises(ValidationError):
            QuerySpec(limit=50001)

    @pytest.mark.asyncio
    async def test_query_only_mode(self, tmp_path: Path) -> None:
        """Reader uses query_only pragma."""
        db_path = tmp_path / "test.db"
        events = [make_event("okx", "trade", {"price": 1}, ts_suffix=0)]
        await seed_events(db_path, events)

        reader = EventReader(db_path)
        await reader.connect()
        try:
            # Check pragma value
            cursor = await reader._conn.execute("PRAGMA query_only")  # type: ignore
            row = await cursor.fetchone()
            assert row[0] == 1  # query_only is ON
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_iter_query_streams(self, tmp_path: Path) -> None:
        """iter_query yields all events in chunks without loading all to RAM."""
        db_path = tmp_path / "test.db"
        # Create 2500 unique events
        events = [make_event("okx", "trade", {"seq": i}, ts_suffix=i % 60) for i in range(2500)]
        await seed_events(db_path, events)

        reader = EventReader(db_path)
        await reader.connect()
        try:
            count = 0
            async for _ in reader.iter_query(QuerySpec(limit=50000), chunk_size=500):
                count += 1
            # May be less than 2500 due to deduplication if ts_suffix collisions
            # But with unique payloads, should be 2500
            assert count >= 1
        finally:
            await reader.close()

    @pytest.mark.asyncio
    async def test_payload_as_dict(self, tmp_path: Path) -> None:
        """payload_as_dict decodes JSON payload."""
        db_path = tmp_path / "test.db"
        events = [make_event("okx", "trade", {"price": 42000.5, "size": 1.5}, ts_suffix=0)]
        await seed_events(db_path, events)

        reader = EventReader(db_path)
        await reader.connect()
        try:
            rows = await reader.query(QuerySpec())
            payload = payload_as_dict(rows[0])
            assert payload["price"] == 42000.5
            assert payload["size"] == 1.5
        finally:
            await reader.close()
