"""Tests for collector router."""

import asyncio
from pathlib import Path

import pytest

from ravebear_monolith.collectors.base import CollectorBase, CollectorEvent
from ravebear_monolith.collectors.router import CollectorRouter
from ravebear_monolith.util.rate_limit import BudgetRegistry
from ravebear_monolith.util.retry import RetryPolicy


class FakeCollector(CollectorBase):
    """Fake collector for testing."""

    def __init__(
        self,
        name: str,
        events: list[CollectorEvent] | None = None,
        error_count: int = 0,
    ) -> None:
        super().__init__(name)
        self._events = list(events) if events else []
        self._error_count = error_count
        self._errors_raised = 0

    async def start(self) -> None:
        self._running = True

    async def stop(self) -> None:
        self._running = False

    async def next_event(self) -> CollectorEvent | None:
        # Simulate transient errors
        if self._errors_raised < self._error_count:
            self._errors_raised += 1
            raise ConnectionError("transient error")

        if self._events:
            return self._events.pop(0)
        return None


def make_events(count: int, source: str = "test") -> list[CollectorEvent]:
    """Create test events."""
    return [CollectorEvent.create(source, "trade", {"seq": i}) for i in range(count)]


class TestCollectorRouter:
    """Tests for CollectorRouter."""

    @pytest.mark.asyncio
    async def test_events_flow_through_router(self, tmp_path: Path) -> None:
        """Events from collectors flow through router."""
        events = make_events(3, "fake")
        collector = FakeCollector("fake", events=events)
        registry = BudgetRegistry()

        router = CollectorRouter(
            collectors=[collector],
            budget_registry=registry,
            kill_switch_path=tmp_path / "kill.txt",
        )

        result = await router.run(max_events=3)

        assert result == 0
        assert router.event_count == 3

    @pytest.mark.asyncio
    async def test_multiple_collectors(self, tmp_path: Path) -> None:
        """Router aggregates from multiple collectors."""
        c1 = FakeCollector("c1", events=make_events(2, "c1"))
        c2 = FakeCollector("c2", events=make_events(2, "c2"))
        registry = BudgetRegistry()

        router = CollectorRouter(
            collectors=[c1, c2],
            budget_registry=registry,
            kill_switch_path=tmp_path / "kill.txt",
        )

        result = await router.run(max_events=4)

        assert result == 0
        assert router.event_count == 4

    @pytest.mark.asyncio
    async def test_kill_switch_stops_router(self, tmp_path: Path) -> None:
        """Kill switch triggers return code 2."""
        # Create kill switch file
        kill_file = tmp_path / "kill.txt"
        kill_file.write_text("KILL", encoding="utf-8")

        collector = FakeCollector("test", events=make_events(10))
        registry = BudgetRegistry()

        router = CollectorRouter(
            collectors=[collector],
            budget_registry=registry,
            kill_switch_path=kill_file,
        )

        result = await router.run(max_events=100)

        assert result == 2
        # Should stop immediately, not process all events
        assert router.event_count == 0

    @pytest.mark.asyncio
    async def test_rate_limit_respected(self, tmp_path: Path) -> None:
        """Rate limiting is respected when bucket exists."""
        events = make_events(5)
        collector = FakeCollector("test", events=events)
        registry = BudgetRegistry()
        registry.register("collector", rate_per_sec=1000, burst=100)

        router = CollectorRouter(
            collectors=[collector],
            budget_registry=registry,
            kill_switch_path=tmp_path / "kill.txt",
        )

        result = await router.run(max_events=5)

        assert result == 0
        assert router.event_count == 5

    @pytest.mark.asyncio
    async def test_retry_on_transient_error(self, tmp_path: Path) -> None:
        """Router retries on transient errors."""
        events = make_events(2)
        # Fail twice then succeed
        collector = FakeCollector("test", events=events, error_count=2)
        registry = BudgetRegistry()

        policy = RetryPolicy(max_attempts=5, base_delay_s=0.01, jitter=0.0)
        router = CollectorRouter(
            collectors=[collector],
            budget_registry=registry,
            kill_switch_path=tmp_path / "kill.txt",
            retry_policy=policy,
        )

        result = await router.run(max_events=2)

        assert result == 0
        assert router.event_count == 2

    @pytest.mark.asyncio
    async def test_empty_collectors(self, tmp_path: Path) -> None:
        """Router handles empty collector list."""
        registry = BudgetRegistry()

        router = CollectorRouter(
            collectors=[],
            budget_registry=registry,
            kill_switch_path=tmp_path / "kill.txt",
        )

        # Use timeout to avoid infinite loop
        async def run_with_timeout() -> int:
            task = asyncio.create_task(router.run(max_events=0))
            return await asyncio.wait_for(task, timeout=1.0)

        result = await run_with_timeout()
        assert result == 0

    @pytest.mark.asyncio
    async def test_stop_method(self, tmp_path: Path) -> None:
        """Router stop() signals loop to exit."""
        collector = FakeCollector("test", events=make_events(100))
        registry = BudgetRegistry()

        router = CollectorRouter(
            collectors=[collector],
            budget_registry=registry,
            kill_switch_path=tmp_path / "kill.txt",
        )

        async def stop_after_delay() -> None:
            await asyncio.sleep(0.05)
            router.stop()

        asyncio.create_task(stop_after_delay())

        result = await asyncio.wait_for(router.run(), timeout=5.0)
        assert result == 0
