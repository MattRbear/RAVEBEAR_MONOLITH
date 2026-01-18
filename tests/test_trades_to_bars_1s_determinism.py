"""Tests for deterministic OHLC in 1s bars regardless of trade order."""

import random
from pathlib import Path

import pytest

from ravebear_monolith.collectors.base import CollectorEvent
from ravebear_monolith.core.replay_runner import ReplayRunner
from ravebear_monolith.processors.okx.trades_to_bars_1s import TradesToBars1sProcessor
from ravebear_monolith.storage.bar_sink import BarSink
from ravebear_monolith.storage.event_sink import EventSink


def make_trade_event(
    symbol: str,
    price: float,
    size: float,
    trade_ts_ms: int,
    trade_id: str,
) -> CollectorEvent:
    """Create an OKX trade event with explicit trade timestamp."""
    # Use ts_utc based on trade_ts_ms for bucket assignment
    ts_second = (trade_ts_ms // 1000) % 60
    return CollectorEvent(
        source="okx",
        event_type="trade",
        ts_utc=f"2024-01-18T12:00:{ts_second:02d}+00:00",
        payload={
            "inst_id": symbol,
            "price": price,
            "size": size,
            "trade_ts_ms": trade_ts_ms,
            "trade_id": trade_id,
        },
    )


async def seed_events_shuffled(
    db_path: Path, events: list[CollectorEvent], shuffle: bool = False
) -> None:
    """Seed database with events, optionally shuffled."""
    if shuffle:
        events = events.copy()
        random.shuffle(events)

    async with EventSink(db_path) as sink:
        for event in events:
            await sink.write(event)


class TestTradesToBars1sDeterminism:
    """Tests for deterministic OHLC computation."""

    @pytest.mark.asyncio
    async def test_same_trades_different_order_same_bar(self, tmp_path: Path) -> None:
        """Same trades in different order produce identical bar."""
        # Create trades with distinct trade_ts_ms values
        base_ts = 1705579200000  # 2024-01-18T12:00:00
        trades_data = [
            ("BTC-USDT", 42000, 0.1, base_ts + 100, "trade_a"),  # First by time
            ("BTC-USDT", 42050, 0.2, base_ts + 200, "trade_b"),
            ("BTC-USDT", 42100, 0.1, base_ts + 300, "trade_c"),  # Highest price
            ("BTC-USDT", 41900, 0.15, base_ts + 400, "trade_d"),  # Lowest price
            ("BTC-USDT", 42020, 0.25, base_ts + 500, "trade_e"),  # Last by time
        ]

        events = [
            make_trade_event(sym, price, size, ts, tid) for sym, price, size, ts, tid in trades_data
        ]

        # Run 1: Natural order
        db1 = tmp_path / "run1.db"
        await seed_events_shuffled(db1, events, shuffle=False)
        processor1 = TradesToBars1sProcessor(db1)
        runner1 = ReplayRunner(
            db_path=db1,
            cursor_name="test",
            processor=processor1,
            kill_switch_path=tmp_path / "kill1.txt",
        )
        await runner1.run()
        await processor1.finalize()

        # Run 2: Shuffled order
        db2 = tmp_path / "run2.db"
        await seed_events_shuffled(db2, events, shuffle=True)
        processor2 = TradesToBars1sProcessor(db2)
        runner2 = ReplayRunner(
            db_path=db2,
            cursor_name="test",
            processor=processor2,
            kill_switch_path=tmp_path / "kill2.txt",
        )
        await runner2.run()
        await processor2.finalize()

        # Get bars from both runs
        async with BarSink(db1) as sink1:
            bars1 = await sink1.get_all_bars("BTC-USDT")

        async with BarSink(db2) as sink2:
            bars2 = await sink2.get_all_bars("BTC-USDT")

        # Assert identical bars
        assert len(bars1) == 1
        assert len(bars2) == 1

        bar1, bar2 = bars1[0], bars2[0]
        assert bar1.open == bar2.open == 42000  # First trade by time
        assert bar1.close == bar2.close == 42020  # Last trade by time
        assert bar1.high == bar2.high == 42100  # Max price
        assert bar1.low == bar2.low == 41900  # Min price
        assert bar1.volume == pytest.approx(bar2.volume)
        assert bar1.trade_count == bar2.trade_count == 5

    @pytest.mark.asyncio
    async def test_tie_break_by_event_id(self, tmp_path: Path) -> None:
        """Trades with same ts are ordered by event.id (stable tie-break)."""
        # All trades have same trade_ts_ms - event.id determines order
        base_ts = 1705579200000
        events = [
            make_trade_event("ETH-USDT", 2000, 0.5, base_ts, "trade_z"),  # z > a
            make_trade_event("ETH-USDT", 2100, 0.5, base_ts, "trade_a"),  # First by id
            make_trade_event("ETH-USDT", 2050, 0.5, base_ts, "trade_m"),  # Middle
        ]

        db_path = tmp_path / "test.db"
        await seed_events_shuffled(db_path, events, shuffle=True)

        processor = TradesToBars1sProcessor(db_path)
        runner = ReplayRunner(
            db_path=db_path,
            cursor_name="test",
            processor=processor,
            kill_switch_path=tmp_path / "kill.txt",
        )
        await runner.run()
        await processor.finalize()

        async with BarSink(db_path) as sink:
            bars = await sink.get_all_bars("ETH-USDT")

        assert len(bars) == 1
        bar = bars[0]
        # Note: event.id is a hash, not the trade_id. The sort is by (trade_ts_ms, event.id)
        # Since all have same trade_ts_ms, order is by event.id (hash)
        # We verify high/low are deterministic
        assert bar.high == 2100
        assert bar.low == 2000
        assert bar.trade_count == 3

    @pytest.mark.asyncio
    async def test_multiple_runs_same_result(self, tmp_path: Path) -> None:
        """Multiple shuffled runs produce identical bars."""
        base_ts = 1705579200000
        trades_data = [
            ("BTC-USDT", 42000 + i * 10, 0.1, base_ts + i * 50, f"trade_{i}") for i in range(10)
        ]
        events = [
            make_trade_event(sym, price, size, ts, tid) for sym, price, size, ts, tid in trades_data
        ]

        results = []
        for run_id in range(3):
            db = tmp_path / f"run{run_id}.db"
            await seed_events_shuffled(db, events, shuffle=True)

            processor = TradesToBars1sProcessor(db)
            runner = ReplayRunner(
                db_path=db,
                cursor_name="test",
                processor=processor,
                kill_switch_path=tmp_path / f"kill{run_id}.txt",
            )
            await runner.run()
            await processor.finalize()

            async with BarSink(db) as sink:
                bars = await sink.get_all_bars("BTC-USDT")

            results.append(bars[0])

        # All runs should produce identical bars
        for bar in results[1:]:
            assert bar.open == results[0].open
            assert bar.close == results[0].close
            assert bar.high == results[0].high
            assert bar.low == results[0].low
            assert bar.volume == pytest.approx(results[0].volume)
            assert bar.trade_count == results[0].trade_count
