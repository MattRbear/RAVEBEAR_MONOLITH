"""E2E tests: EventSink -> ReplayRunner -> ProcessorRouter -> Bars."""

from pathlib import Path

import pytest

from ravebear_monolith.collectors.base import CollectorEvent
from ravebear_monolith.core.processor_router import FailurePolicy, ProcessorRouter
from ravebear_monolith.core.replay_runner import ReplayRunner
from ravebear_monolith.processors.okx.trades_to_bars_1s import TradesToBars1sProcessor
from ravebear_monolith.storage.bar_reader import BarQuerySpec, BarReader
from ravebear_monolith.storage.cursor_store import CursorStore
from ravebear_monolith.storage.event_sink import EventSink


def make_trade_event(
    symbol: str,
    price: float,
    size: float,
    ts_second: int,
    trade_id: str,
) -> CollectorEvent:
    """Create an OKX trade event."""
    return CollectorEvent(
        source="okx",
        event_type="trade",
        ts_utc=f"2024-01-18T12:00:{ts_second:02d}+00:00",
        payload={
            "inst_id": symbol,
            "price": price,
            "size": size,
            "trade_ts_ms": 1705579200000 + ts_second * 1000,
            "trade_id": trade_id,
        },
    )


async def seed_events(db_path: Path, events: list[CollectorEvent]) -> None:
    """Seed database with events via EventSink."""
    async with EventSink(db_path) as sink:
        for event in events:
            await sink.write(event)


class TestE2EReplayToBars:
    """End-to-end tests for replay pipeline to bars."""

    @pytest.mark.asyncio
    async def test_e2e_replay_writes_bars_and_commits_cursor(self, tmp_path: Path) -> None:
        """Full pipeline: events -> replay -> bars, cursor committed."""
        db_path = tmp_path / "test.db"

        # Seed trades across 3 seconds (3 buckets)
        events = [
            # Second 0: 3 trades
            make_trade_event("BTC-USDT", 42000, 0.1, 0, "t0a"),
            make_trade_event("BTC-USDT", 42010, 0.2, 0, "t0b"),
            make_trade_event("BTC-USDT", 42005, 0.15, 0, "t0c"),
            # Second 1: 2 trades
            make_trade_event("BTC-USDT", 42100, 0.3, 1, "t1a"),
            make_trade_event("BTC-USDT", 42050, 0.25, 1, "t1b"),
            # Second 2: 2 trades
            make_trade_event("BTC-USDT", 42200, 0.1, 2, "t2a"),
            make_trade_event("BTC-USDT", 42150, 0.2, 2, "t2b"),
        ]
        await seed_events(db_path, events)

        # Build processor pipeline
        processor = TradesToBars1sProcessor(db_path)
        router = ProcessorRouter(
            {"bars_1s": processor},
            policy=FailurePolicy.FAIL_CLOSED,
        )

        runner = ReplayRunner(
            db_path=db_path,
            cursor_name="e2e",
            processor=router,
            chunk_size=50,
            kill_switch_path=tmp_path / "kill.txt",
        )

        # Run
        exit_code = await runner.run()

        # Assertions
        assert exit_code == 0

        # Cursor exists
        async with CursorStore(db_path) as cursors:
            cursor = await cursors.get("e2e")
            assert cursor is not None
            assert cursor.last_ts_ms > 0

        # Bars written
        async with BarReader(db_path) as reader:
            bars = await reader.query(BarQuerySpec(symbol="BTC-USDT"))
            assert len(bars) == 3  # 3 seconds = 3 bars

            # Verify each bar has valid OHLCV
            for bar in bars:
                assert bar.trade_count > 0
                assert bar.volume > 0
                assert bar.low <= bar.open <= bar.high
                assert bar.low <= bar.close <= bar.high

            # Bars are ordered by ts_ms
            ts_values = [b.ts_ms for b in bars]
            assert ts_values == sorted(ts_values)

    @pytest.mark.asyncio
    async def test_e2e_fail_closed_bad_payload(self, tmp_path: Path) -> None:
        """Bad payload causes exit 2, cursor only advances to last good event."""
        db_path = tmp_path / "test.db"
        kill_path = tmp_path / "kill.txt"

        # Seed events with bad payload in middle
        async with EventSink(db_path) as sink:
            # Event 1: valid
            valid1 = CollectorEvent(
                source="okx",
                event_type="trade",
                ts_utc="2024-01-18T12:00:00+00:00",
                payload={"inst_id": "BTC-USDT", "price": 42000, "size": 0.1},
            )
            await sink.write(valid1)

            # Event 2: invalid (missing price)
            invalid = CollectorEvent(
                source="okx",
                event_type="trade",
                ts_utc="2024-01-18T12:00:01+00:00",
                payload={"inst_id": "BTC-USDT", "size": 0.1},  # No price!
            )
            await sink.write(invalid)

        # Build processor pipeline
        processor = TradesToBars1sProcessor(db_path)
        router = ProcessorRouter(
            {"bars_1s": processor},
            policy=FailurePolicy.FAIL_CLOSED,
        )

        runner = ReplayRunner(
            db_path=db_path,
            cursor_name="e2e",
            processor=router,
            kill_switch_path=kill_path,
        )

        exit_code = await runner.run()

        assert exit_code == 2
        assert kill_path.exists()

        # Cursor should be at event 1 (only 1 event processed successfully)
        async with CursorStore(db_path) as cursors:
            cursor = await cursors.get("e2e")
            assert cursor is not None
            # Only 1 event should have been processed
            assert runner.processed_count == 1

    @pytest.mark.asyncio
    async def test_e2e_resume_no_overlap(self, tmp_path: Path) -> None:
        """Resume from cursor processes remaining events only."""
        db_path = tmp_path / "test.db"

        # Seed 10 events across time
        events = [
            make_trade_event("BTC-USDT", 42000 + i * 10, 0.1, i % 60, f"trade_{i}")
            for i in range(10)
        ]
        await seed_events(db_path, events)

        # First run: max_events=4
        processor1 = TradesToBars1sProcessor(db_path)
        router1 = ProcessorRouter(
            {"bars_1s": processor1},
            policy=FailurePolicy.FAIL_CLOSED,
        )

        runner1 = ReplayRunner(
            db_path=db_path,
            cursor_name="e2e",
            processor=router1,
            max_events=4,
            kill_switch_path=tmp_path / "kill.txt",
        )

        exit_code1 = await runner1.run()
        assert exit_code1 == 0
        assert runner1.processed_count == 4

        # Second run: no max_events
        processor2 = TradesToBars1sProcessor(db_path)
        router2 = ProcessorRouter(
            {"bars_1s": processor2},
            policy=FailurePolicy.FAIL_CLOSED,
        )

        runner2 = ReplayRunner(
            db_path=db_path,
            cursor_name="e2e",
            processor=router2,
            kill_switch_path=tmp_path / "kill.txt",
        )

        exit_code2 = await runner2.run()
        assert exit_code2 == 0
        assert runner2.processed_count == 6  # Remaining 6 events

        # Total = 10 events processed
        total_processed = runner1.processed_count + runner2.processed_count
        assert total_processed == 10

        # Cursor at last event
        async with CursorStore(db_path) as cursors:
            cursor = await cursors.get("e2e")
            assert cursor is not None
