"""Tests for ReplayRunner with processor contract."""

from pathlib import Path

import pytest

from ravebear_monolith.collectors.base import CollectorEvent
from ravebear_monolith.core.processor import ProcessorBase, ProcessResult
from ravebear_monolith.core.replay_runner import ReplayRunner
from ravebear_monolith.storage.cursor_store import CursorStore
from ravebear_monolith.storage.event_reader import EventRow
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
    async with EventSink(db_path) as sink:
        for event in events:
            await sink.write(event)


class CollectingProcessor(ProcessorBase):
    """Processor that collects processed event IDs."""

    def __init__(self) -> None:
        self.processed_ids: list[str] = []

    async def process(self, event: EventRow) -> ProcessResult:
        self.processed_ids.append(event.id)
        return ProcessResult(ok=True)


class FailingProcessor(ProcessorBase):
    """Processor that fails on specific event index."""

    def __init__(self, fail_at: int) -> None:
        self._fail_at = fail_at
        self._count = 0

    async def process(self, event: EventRow) -> ProcessResult:
        self._count += 1
        if self._count == self._fail_at:
            return ProcessResult(ok=False, reason="Intentional failure")
        return ProcessResult(ok=True)


class ExceptionProcessor(ProcessorBase):
    """Processor that raises on specific event index."""

    def __init__(self, raise_at: int) -> None:
        self._raise_at = raise_at
        self._count = 0

    async def process(self, event: EventRow) -> ProcessResult:
        self._count += 1
        if self._count == self._raise_at:
            raise RuntimeError("Intentional exception")
        return ProcessResult(ok=True)


class TestReplayRunner:
    """Tests for ReplayRunner."""

    @pytest.mark.asyncio
    async def test_replay_runner_processes_and_commits_cursor(self, tmp_path: Path) -> None:
        """Runner processes events and commits cursor."""
        db_path = tmp_path / "test.db"
        events = [make_event("okx", "trade", {"seq": i}, ts_suffix=i) for i in range(5)]
        await seed_events(db_path, events)

        processor = CollectingProcessor()
        runner = ReplayRunner(
            db_path=db_path,
            cursor_name="test",
            processor=processor,
            kill_switch_path=tmp_path / "kill.txt",
        )

        result = await runner.run()

        assert result == 0
        assert len(processor.processed_ids) == 5

        # Verify cursor was committed
        async with CursorStore(db_path) as cursors:
            cursor = await cursors.get("test")
            assert cursor is not None
            # Cursor should be at last event
            assert cursor.last_event_id == processor.processed_ids[-1]

    @pytest.mark.asyncio
    async def test_replay_runner_stops_on_processor_failure(self, tmp_path: Path) -> None:
        """Runner stops and returns 2 when processor fails."""
        db_path = tmp_path / "test.db"
        events = [make_event("okx", "trade", {"seq": i}, ts_suffix=i) for i in range(5)]
        await seed_events(db_path, events)

        processor = FailingProcessor(fail_at=3)  # Fail on 3rd event
        runner = ReplayRunner(
            db_path=db_path,
            cursor_name="test",
            processor=processor,
            kill_switch_path=tmp_path / "kill.txt",
        )

        result = await runner.run()

        assert result == 2

        # Cursor should be committed only for first 2 events
        async with CursorStore(db_path) as cursors:
            cursor = await cursors.get("test")
            assert cursor is not None
            # We processed 2 events successfully before failure
            assert runner.processed_count == 2

    @pytest.mark.asyncio
    async def test_replay_runner_fail_closed_on_exception(self, tmp_path: Path) -> None:
        """Runner triggers kill switch on processor exception."""
        db_path = tmp_path / "test.db"
        kill_path = tmp_path / "kill.txt"
        events = [make_event("okx", "trade", {"seq": i}, ts_suffix=i) for i in range(5)]
        await seed_events(db_path, events)

        processor = ExceptionProcessor(raise_at=2)  # Raise on 2nd event
        runner = ReplayRunner(
            db_path=db_path,
            cursor_name="test",
            processor=processor,
            kill_switch_path=kill_path,
        )

        result = await runner.run()

        assert result == 2

        # Kill switch should be written
        assert kill_path.exists()
        content = kill_path.read_text(encoding="utf-8")
        assert "KILL" in content
        assert "Intentional exception" in content

    @pytest.mark.asyncio
    async def test_deterministic_resume(self, tmp_path: Path) -> None:
        """Runner resumes from cursor position after partial run."""
        db_path = tmp_path / "test.db"
        events = [make_event("okx", "trade", {"seq": i}, ts_suffix=i) for i in range(10)]
        await seed_events(db_path, events)

        # First run: process 3 events
        processor1 = CollectingProcessor()
        runner1 = ReplayRunner(
            db_path=db_path,
            cursor_name="test",
            processor=processor1,
            max_events=3,
            kill_switch_path=tmp_path / "kill.txt",
        )

        result1 = await runner1.run()
        assert result1 == 0
        assert len(processor1.processed_ids) == 3

        # Second run: process remaining
        processor2 = CollectingProcessor()
        runner2 = ReplayRunner(
            db_path=db_path,
            cursor_name="test",
            processor=processor2,
            kill_switch_path=tmp_path / "kill.txt",
        )

        result2 = await runner2.run()
        assert result2 == 0
        assert len(processor2.processed_ids) == 7  # Remaining 7 events

        # No overlap
        assert set(processor1.processed_ids).isdisjoint(set(processor2.processed_ids))


class TestReplayRunnerBestEffort:
    """Tests for ReplayRunner with BEST_EFFORT policy via ProcessorRouter."""

    @pytest.mark.asyncio
    async def test_best_effort_returns_3_on_failure(self, tmp_path: Path) -> None:
        """BEST_EFFORT returns exit 3 on processor failure."""
        from ravebear_monolith.core.processor_router import FailurePolicy, ProcessorRouter

        db_path = tmp_path / "test.db"
        kill_path = tmp_path / "kill.txt"
        events = [make_event("okx", "trade", {"seq": i}, ts_suffix=i) for i in range(5)]
        await seed_events(db_path, events)

        failing_processor = FailingProcessor(fail_at=2)
        router = ProcessorRouter(
            {"main": failing_processor},
            policy=FailurePolicy.BEST_EFFORT,
        )

        runner = ReplayRunner(
            db_path=db_path,
            cursor_name="test",
            processor=router,
            kill_switch_path=kill_path,
        )

        result = await runner.run()

        assert result == 3
        # Kill switch should NOT be written for BEST_EFFORT
        assert not kill_path.exists()

    @pytest.mark.asyncio
    async def test_best_effort_no_kill_switch_on_exception(self, tmp_path: Path) -> None:
        """BEST_EFFORT does not write kill switch on exception."""
        from ravebear_monolith.core.processor_router import FailurePolicy, ProcessorRouter

        db_path = tmp_path / "test.db"
        kill_path = tmp_path / "kill.txt"
        events = [make_event("okx", "trade", {"seq": i}, ts_suffix=i) for i in range(5)]
        await seed_events(db_path, events)

        router = ProcessorRouter(
            {"main": ExceptionProcessor(raise_at=1)},
            policy=FailurePolicy.BEST_EFFORT,
        )

        runner = ReplayRunner(
            db_path=db_path,
            cursor_name="test",
            processor=router,
            kill_switch_path=kill_path,
        )

        result = await runner.run()

        assert result == 3
        assert not kill_path.exists()

    @pytest.mark.asyncio
    async def test_best_effort_cursor_not_committed_on_failure(self, tmp_path: Path) -> None:
        """BEST_EFFORT does not commit cursor when processor fails."""
        from ravebear_monolith.core.processor_router import FailurePolicy, ProcessorRouter

        db_path = tmp_path / "test.db"
        events = [make_event("okx", "trade", {"seq": i}, ts_suffix=i) for i in range(5)]
        await seed_events(db_path, events)

        # Fail on first event
        router = ProcessorRouter(
            {"main": FailingProcessor(fail_at=1)},
            policy=FailurePolicy.BEST_EFFORT,
        )

        runner = ReplayRunner(
            db_path=db_path,
            cursor_name="test",
            processor=router,
            kill_switch_path=tmp_path / "kill.txt",
        )

        result = await runner.run()

        assert result == 3
        assert runner.processed_count == 0

        # Cursor should NOT exist (no successful commits)
        async with CursorStore(db_path) as cursors:
            cursor = await cursors.get("test")
            assert cursor is None
