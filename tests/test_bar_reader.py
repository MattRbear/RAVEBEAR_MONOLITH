"""Tests for BarReader with read-only queries."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from ravebear_monolith.storage.bar_reader import BarQuerySpec, BarReader, BarRow
from ravebear_monolith.storage.bar_sink import Bar1s, BarSink


async def seed_bars(db_path: Path, bars: list[Bar1s]) -> None:
    """Seed database with bars via BarSink."""
    async with BarSink(db_path) as sink:
        for bar in bars:
            await sink.upsert_bar(bar)


def make_bar(symbol: str, ts_ms: int, price: float = 100.0) -> Bar1s:
    """Create a test bar."""
    return Bar1s(
        symbol=symbol,
        ts_ms=ts_ms,
        open=price,
        high=price + 10,
        low=price - 10,
        close=price + 5,
        volume=1.0,
        trade_count=10,
    )


class TestBarReader:
    """Tests for BarReader."""

    @pytest.mark.asyncio
    async def test_roundtrip_query_all(self, tmp_path: Path) -> None:
        """Write bars and read them back."""
        db_path = tmp_path / "test.db"
        bars = [make_bar("BTC-USDT", 1000 * (i + 1)) for i in range(10)]
        await seed_bars(db_path, bars)

        async with BarReader(db_path) as reader:
            rows = await reader.query(BarQuerySpec())
            assert len(rows) == 10
            assert all(isinstance(r, BarRow) for r in rows)

    @pytest.mark.asyncio
    async def test_filter_by_symbol(self, tmp_path: Path) -> None:
        """Filter bars by symbol."""
        db_path = tmp_path / "test.db"
        bars = [
            make_bar("BTC-USDT", 1000),
            make_bar("ETH-USDT", 2000),
            make_bar("BTC-USDT", 3000),
            make_bar("SOL-USDT", 4000),
        ]
        await seed_bars(db_path, bars)

        async with BarReader(db_path) as reader:
            rows = await reader.query(BarQuerySpec(symbol="BTC-USDT"))
            assert len(rows) == 2
            assert all(r.symbol == "BTC-USDT" for r in rows)

    @pytest.mark.asyncio
    async def test_filter_by_time_range(self, tmp_path: Path) -> None:
        """Filter bars by timestamp range."""
        db_path = tmp_path / "test.db"
        bars = [make_bar("BTC-USDT", 1000 * (i + 1)) for i in range(10)]
        await seed_bars(db_path, bars)

        async with BarReader(db_path) as reader:
            # Query bars from 3000 to 7000 (inclusive)
            rows = await reader.query(BarQuerySpec(ts_min=3000, ts_max=7000))
            assert len(rows) == 5
            assert all(3000 <= r.ts_ms <= 7000 for r in rows)

    @pytest.mark.asyncio
    async def test_ordering_asc_desc(self, tmp_path: Path) -> None:
        """Test ascending and descending order."""
        db_path = tmp_path / "test.db"
        bars = [make_bar("BTC-USDT", 1000 * (i + 1)) for i in range(5)]
        await seed_bars(db_path, bars)

        async with BarReader(db_path) as reader:
            asc_rows = await reader.query(BarQuerySpec(order="asc"))
            desc_rows = await reader.query(BarQuerySpec(order="desc"))

            assert asc_rows[0].ts_ms < asc_rows[-1].ts_ms
            assert desc_rows[0].ts_ms > desc_rows[-1].ts_ms

    @pytest.mark.asyncio
    async def test_limit_enforced(self, tmp_path: Path) -> None:
        """Limit parameter is enforced."""
        db_path = tmp_path / "test.db"
        bars = [make_bar("BTC-USDT", 1000 * (i + 1)) for i in range(10)]
        await seed_bars(db_path, bars)

        async with BarReader(db_path) as reader:
            rows = await reader.query(BarQuerySpec(limit=3))
            assert len(rows) == 3

    def test_limit_validation_error(self) -> None:
        """Limit > 50000 raises validation error."""
        with pytest.raises(ValidationError):
            BarQuerySpec(limit=50001)

    @pytest.mark.asyncio
    async def test_query_only_mode(self, tmp_path: Path) -> None:
        """Reader uses query_only pragma."""
        db_path = tmp_path / "test.db"
        bars = [make_bar("BTC-USDT", 1000)]
        await seed_bars(db_path, bars)

        async with BarReader(db_path) as reader:
            # Check pragma value
            cursor = await reader._conn.execute("PRAGMA query_only")  # type: ignore
            row = await cursor.fetchone()
            assert row[0] == 1  # query_only is ON

    @pytest.mark.asyncio
    async def test_iter_query_streams(self, tmp_path: Path) -> None:
        """iter_query yields all bars in chunks."""
        db_path = tmp_path / "test.db"
        # Create 2500 bars across multiple symbols
        bars = []
        for i in range(2500):
            symbol = ["BTC-USDT", "ETH-USDT", "SOL-USDT"][i % 3]
            bars.append(make_bar(symbol, 1000 * (i + 1)))
        await seed_bars(db_path, bars)

        async with BarReader(db_path) as reader:
            count = 0
            async for _ in reader.iter_query(BarQuerySpec(limit=50000), chunk_size=500):
                count += 1
            assert count == 2500
