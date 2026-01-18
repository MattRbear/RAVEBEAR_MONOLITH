"""Tests for collector base contract."""

from ravebear_monolith.collectors.base import CollectorBase, CollectorEvent


class TestCollectorEvent:
    """Tests for CollectorEvent model."""

    def test_create_event(self) -> None:
        """CollectorEvent.create sets timestamp automatically."""
        event = CollectorEvent.create(
            source="test",
            event_type="trade",
            payload={"price": 100.0},
        )
        assert event.source == "test"
        assert event.event_type == "trade"
        assert event.payload == {"price": 100.0}
        assert "T" in event.ts_utc  # ISO format

    def test_event_fields(self) -> None:
        """CollectorEvent has required fields."""
        event = CollectorEvent(
            source="exchange",
            event_type="ticker",
            ts_utc="2026-01-18T00:00:00Z",
            payload={"bid": 99.0, "ask": 101.0},
        )
        assert event.source == "exchange"
        assert event.event_type == "ticker"


class FakeCollector(CollectorBase):
    """Fake collector for testing."""

    def __init__(self, name: str, events: list[CollectorEvent] | None = None) -> None:
        super().__init__(name)
        self._events = list(events) if events else []
        self._started = False
        self._stopped = False

    async def start(self) -> None:
        self._started = True
        self._running = True

    async def stop(self) -> None:
        self._stopped = True
        self._running = False

    async def next_event(self) -> CollectorEvent | None:
        if self._events:
            return self._events.pop(0)
        return None


class TestCollectorBase:
    """Tests for CollectorBase ABC."""

    def test_name_property(self) -> None:
        """Collector has name property."""
        collector = FakeCollector("test_collector")
        assert collector.name == "test_collector"

    async def test_start_stop(self) -> None:
        """Collector can be started and stopped."""
        collector = FakeCollector("test")
        assert not collector.is_running

        await collector.start()
        assert collector.is_running

        await collector.stop()
        assert not collector.is_running

    async def test_next_event(self) -> None:
        """Collector returns events from next_event."""
        event = CollectorEvent.create("test", "trade", {"price": 100})
        collector = FakeCollector("test", events=[event])

        await collector.start()
        result = await collector.next_event()

        assert result is not None
        assert result.event_type == "trade"

    async def test_next_event_empty(self) -> None:
        """Collector returns None when no events."""
        collector = FakeCollector("test")
        await collector.start()

        result = await collector.next_event()
        assert result is None
