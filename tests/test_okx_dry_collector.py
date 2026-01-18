"""Tests for OKX dry-run collector."""

from pathlib import Path

import pytest

from ravebear_monolith.collectors.base import CollectorEvent
from ravebear_monolith.collectors.okx.dry_run import OKXTradesDryRunCollector
from ravebear_monolith.collectors.okx.schemas import OKXTrade
from ravebear_monolith.collectors.router import CollectorRouter
from ravebear_monolith.util.rate_limit import BudgetRegistry

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestOKXTrade:
    """Tests for OKXTrade schema."""

    def test_from_raw_okx_format(self) -> None:
        """Parse OKX native format with instId, px, sz, ts."""
        raw = {
            "instId": "BTC-USDT",
            "tradeId": "12345",
            "px": "42000.50",
            "sz": "0.5",
            "side": "buy",
            "ts": "1705536000000",
        }
        trade = OKXTrade.from_raw(raw)

        assert trade.inst_id == "BTC-USDT"
        assert trade.trade_id == "12345"
        assert trade.price == 42000.50
        assert trade.size == 0.5
        assert trade.side == "buy"
        assert "2024-01" in trade.ts_utc  # Verify parsed correctly

    def test_from_raw_normalized_format(self) -> None:
        """Parse normalized field names."""
        raw = {
            "inst_id": "ETH-USDT",
            "trade_id": "67890",
            "price": 2500.0,
            "size": 1.25,
            "side": "sell",
            "ts_utc": "2024-01-18T12:00:00+00:00",
        }
        trade = OKXTrade.from_raw(raw)

        assert trade.inst_id == "ETH-USDT"
        assert trade.price == 2500.0
        assert trade.side == "sell"

    def test_timestamp_normalization_unix_ms(self) -> None:
        """Unix milliseconds are converted to ISO8601."""
        raw = {
            "instId": "BTC-USDT",
            "tradeId": "1",
            "px": "100",
            "sz": "1",
            "side": "buy",
            "ts": 1705536000000,
        }
        trade = OKXTrade.from_raw(raw)

        assert "T" in trade.ts_utc
        assert "+" in trade.ts_utc or "Z" in trade.ts_utc


class TestOKXTradesDryRunCollector:
    """Tests for OKXTradesDryRunCollector."""

    @pytest.mark.asyncio
    async def test_yields_valid_events(self) -> None:
        """Collector yields valid CollectorEvent instances."""
        collector = OKXTradesDryRunCollector(FIXTURES_DIR / "okx_trades.json")
        await collector.start()

        event = await collector.next_event()

        assert event is not None
        assert isinstance(event, CollectorEvent)
        assert event.source == "okx"
        assert event.event_type == "trade"

        await collector.stop()

    @pytest.mark.asyncio
    async def test_payload_matches_schema(self) -> None:
        """Event payload can be validated as OKXTrade."""
        collector = OKXTradesDryRunCollector(FIXTURES_DIR / "okx_trades.json")
        await collector.start()

        event = await collector.next_event()
        assert event is not None

        # Payload should be valid OKXTrade dict
        trade = OKXTrade.model_validate(event.payload)
        assert trade.inst_id == "BTC-USDT"
        assert trade.trade_id == "1001"

        await collector.stop()

    @pytest.mark.asyncio
    async def test_timestamps_normalized(self) -> None:
        """All timestamps are ISO8601 format."""
        collector = OKXTradesDryRunCollector(FIXTURES_DIR / "okx_trades.json")
        await collector.start()

        event = await collector.next_event()
        assert event is not None
        assert "T" in event.ts_utc
        assert "+" in event.ts_utc

        await collector.stop()

    @pytest.mark.asyncio
    async def test_stops_cleanly_at_eof(self) -> None:
        """Collector returns None and stops cleanly at EOF."""
        collector = OKXTradesDryRunCollector(FIXTURES_DIR / "okx_trades.json")
        await collector.start()

        # Exhaust all events
        count = 0
        while True:
            event = await collector.next_event()
            if event is None:
                break
            count += 1

        assert count == 10  # Fixture has 10 trades
        assert await collector.next_event() is None

        await collector.stop()

    @pytest.mark.asyncio
    async def test_missing_fixture_empty(self) -> None:
        """Missing fixture file results in no events."""
        collector = OKXTradesDryRunCollector(Path("/nonexistent/file.json"))
        await collector.start()

        event = await collector.next_event()
        assert event is None

        await collector.stop()


class TestOKXCollectorWithRouter:
    """Integration tests with CollectorRouter."""

    @pytest.mark.asyncio
    async def test_works_with_router(self, tmp_path: Path) -> None:
        """OKX collector integrates with CollectorRouter."""
        collector = OKXTradesDryRunCollector(FIXTURES_DIR / "okx_trades.json")
        registry = BudgetRegistry()

        router = CollectorRouter(
            collectors=[collector],
            budget_registry=registry,
            kill_switch_path=tmp_path / "kill.txt",
        )

        result = await router.run(max_events=5)

        assert result == 0
        assert router.event_count == 5

    @pytest.mark.asyncio
    async def test_router_exhausts_collector(self, tmp_path: Path) -> None:
        """Router processes all events from collector."""
        collector = OKXTradesDryRunCollector(FIXTURES_DIR / "okx_trades.json")
        registry = BudgetRegistry()

        router = CollectorRouter(
            collectors=[collector],
            budget_registry=registry,
            kill_switch_path=tmp_path / "kill.txt",
        )

        result = await router.run(max_events=100)

        assert result == 0
        assert router.event_count == 10  # All 10 trades
