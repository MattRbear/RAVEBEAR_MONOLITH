"""Tests for OKX live collector with mocked WebSocket."""

import asyncio
import json
from unittest.mock import patch

import pytest

from ravebear_monolith.collectors.base import CollectorEvent
from ravebear_monolith.collectors.okx.live import OKXTradesLiveCollector
from ravebear_monolith.collectors.okx.schemas import OKXTrade


class MockWebSocket:
    """Mock WebSocket for testing."""

    def __init__(self, messages: list[str] | None = None) -> None:
        self._messages = list(messages) if messages else []
        self._index = 0
        self._closed = False

    async def recv(self) -> str:
        """Return next message or raise ConnectionClosed."""
        if self._closed or self._index >= len(self._messages):
            # Simulate waiting then connection close
            await asyncio.sleep(0.1)
            from websockets.exceptions import ConnectionClosed

            raise ConnectionClosed(None, None)
        msg = self._messages[self._index]
        self._index += 1
        return msg

    async def send(self, message: str) -> None:
        """Mock send."""
        pass

    async def close(self) -> None:
        """Mock close."""
        self._closed = True

    async def __aenter__(self) -> "MockWebSocket":
        """Async context manager entry."""
        return self

    async def __aexit__(self, *args: object) -> None:
        """Async context manager exit."""
        await self.close()


def make_trade_message(inst_id: str, trade_id: str, price: str, size: str, side: str) -> str:
    """Create a mock OKX trade message."""
    return json.dumps(
        {
            "arg": {"channel": "trades", "instId": inst_id},
            "data": [
                {
                    "instId": inst_id,
                    "tradeId": trade_id,
                    "px": price,
                    "sz": size,
                    "side": side,
                    "ts": "1705536000000",
                }
            ],
        }
    )


async def mock_connect(*args: object, **kwargs: object) -> MockWebSocket:
    """Mock websockets.connect that returns a MockWebSocket."""
    return MockWebSocket([])


class TestOKXTradesLiveCollector:
    """Tests for OKXTradesLiveCollector."""

    @pytest.mark.asyncio
    async def test_emits_valid_collector_event(self) -> None:
        """Collector emits valid CollectorEvent from WebSocket message."""
        messages = [
            # Subscription confirmation (should be skipped)
            '{"event": "subscribe", "arg": {"channel": "trades"}}',
            # Trade message
            make_trade_message("BTC-USDT", "12345", "42000.50", "0.5", "buy"),
        ]
        mock_ws = MockWebSocket(messages)

        async def connect_mock(*args: object, **kwargs: object) -> MockWebSocket:
            return mock_ws

        with patch("websockets.connect", side_effect=connect_mock):
            collector = OKXTradesLiveCollector(inst_id="BTC-USDT")
            await collector.start()

            # Wait for message processing
            await asyncio.sleep(0.2)

            event = await collector.next_event()
            assert event is not None
            assert isinstance(event, CollectorEvent)
            assert event.source == "okx"
            assert event.event_type == "trade"

            await collector.stop()

    @pytest.mark.asyncio
    async def test_payload_matches_schema(self) -> None:
        """Event payload is valid OKXTrade."""
        messages = [make_trade_message("ETH-USDT", "67890", "2500.00", "1.0", "sell")]
        mock_ws = MockWebSocket(messages)

        async def connect_mock(*args: object, **kwargs: object) -> MockWebSocket:
            return mock_ws

        with patch("websockets.connect", side_effect=connect_mock):
            collector = OKXTradesLiveCollector(inst_id="ETH-USDT")
            await collector.start()
            await asyncio.sleep(0.2)

            event = await collector.next_event()
            assert event is not None

            # Validate payload as OKXTrade
            trade = OKXTrade.model_validate(event.payload)
            assert trade.inst_id == "ETH-USDT"
            assert trade.trade_id == "67890"
            assert trade.price == 2500.00
            assert trade.side == "sell"

            await collector.stop()

    @pytest.mark.asyncio
    async def test_handles_multiple_trades(self) -> None:
        """Collector processes multiple trade messages."""
        messages = [
            make_trade_message("BTC-USDT", "1", "42000", "0.1", "buy"),
            make_trade_message("BTC-USDT", "2", "42001", "0.2", "sell"),
            make_trade_message("BTC-USDT", "3", "42002", "0.3", "buy"),
        ]
        mock_ws = MockWebSocket(messages)

        async def connect_mock(*args: object, **kwargs: object) -> MockWebSocket:
            return mock_ws

        with patch("websockets.connect", side_effect=connect_mock):
            collector = OKXTradesLiveCollector()
            await collector.start()
            await asyncio.sleep(0.3)

            events = []
            for _ in range(3):
                event = await collector.next_event()
                if event:
                    events.append(event)

            assert len(events) == 3

            await collector.stop()

    @pytest.mark.asyncio
    async def test_clean_shutdown_on_cancel(self) -> None:
        """Collector stops cleanly when cancelled."""
        mock_ws = MockWebSocket([])

        async def connect_mock(*args: object, **kwargs: object) -> MockWebSocket:
            return mock_ws

        with patch("websockets.connect", side_effect=connect_mock):
            collector = OKXTradesLiveCollector()
            await collector.start()

            # Should stop without error
            await collector.stop()

            assert not collector.is_running

    @pytest.mark.asyncio
    async def test_returns_none_when_stopped(self) -> None:
        """next_event returns None when collector is stopped."""
        mock_ws = MockWebSocket([])

        async def connect_mock(*args: object, **kwargs: object) -> MockWebSocket:
            return mock_ws

        with patch("websockets.connect", side_effect=connect_mock):
            collector = OKXTradesLiveCollector()
            await collector.start()
            await collector.stop()

            event = await collector.next_event()
            assert event is None

    @pytest.mark.asyncio
    async def test_skips_non_trade_messages(self) -> None:
        """Collector ignores non-trade channel messages."""
        messages = [
            '{"event": "subscribe", "arg": {"channel": "trades"}}',
            '{"arg": {"channel": "ticker"}, "data": [{"last": "42000"}]}',
            make_trade_message("BTC-USDT", "1", "42000", "0.1", "buy"),
        ]
        mock_ws = MockWebSocket(messages)

        async def connect_mock(*args: object, **kwargs: object) -> MockWebSocket:
            return mock_ws

        with patch("websockets.connect", side_effect=connect_mock):
            collector = OKXTradesLiveCollector()
            await collector.start()
            await asyncio.sleep(0.2)

            event = await collector.next_event()
            assert event is not None
            # Should only get the trade event
            assert event.event_type == "trade"

            await collector.stop()

    @pytest.mark.asyncio
    async def test_handles_connection_error(self) -> None:
        """Collector handles connection errors gracefully."""
        with patch("websockets.connect", side_effect=ConnectionRefusedError("refused")):
            collector = OKXTradesLiveCollector()

            with pytest.raises(ConnectionRefusedError):
                await collector.start()

            assert not collector.is_running
