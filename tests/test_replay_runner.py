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
    sink = EventSink(db_path)
    await sink.open()
    try:
        for event in events:
            await sink.write(event)
    finally:
        await sink.close()


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
        cursors = CursorStore(db_path)
        await cursors.connect()
        try:
            cursor = await cursors.get("test")
            assert cursor is not None
            # Cursor should be at last event
            assert cursor.last_event_id == processor.processed_ids[-1]
        finally:
            await cursors.close()

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
        cursors = CursorStore(db_path)
        await cursors.connect()
        try:
            cursor = await cursors.get("test")
            assert cursor is not None
            # We processed 2 events successfully before failure
            assert runner.processed_count == 2
        finally:
            await cursors.close()

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
