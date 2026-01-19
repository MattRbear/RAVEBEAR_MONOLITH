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
        # First call raises, simulating initial connect failure that triggers reconnect
        # We need to stop the collector to avoid infinite reconnect loop
        call_count = 0

        async def connect_raises(*args: object, **kwargs: object) -> MockWebSocket:
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise ConnectionRefusedError("refused")
            # After 2 failures, return a valid mock that closes immediately
            return MockWebSocket([])

        with patch("websockets.connect", side_effect=connect_raises):
            collector = OKXTradesLiveCollector()
            await collector.start()

            # Wait for reconnect attempts (backoff is 0.5s for first retry)
            await asyncio.sleep(0.8)

            # Stop the collector
            await collector.stop()

            # Should have attempted at least 2 connections
            assert call_count >= 2


class TestOKXReconnect:
    """Tests for OKX WebSocket reconnect behavior."""

    @pytest.mark.asyncio
    async def test_reconnects_after_disconnect(self) -> None:
        """Collector reconnects and continues yielding events after disconnect."""
        # Track connection attempts
        connect_count = 0
        messages_batch_1 = [make_trade_message("BTC-USDT", "1", "42000", "0.1", "buy")]
        messages_batch_2 = [make_trade_message("BTC-USDT", "2", "42001", "0.2", "sell")]

        async def connect_mock(*args: object, **kwargs: object) -> MockWebSocket:
            nonlocal connect_count
            connect_count += 1
            if connect_count == 1:
                # First connection: return messages then disconnect
                return MockWebSocket(messages_batch_1)
            else:
                # Second connection: return more messages
                return MockWebSocket(messages_batch_2)

        with patch("websockets.connect", side_effect=connect_mock):
            collector = OKXTradesLiveCollector()
            await collector.start()

            # Wait for first message
            await asyncio.sleep(0.2)
            event1 = await collector.next_event()
            assert event1 is not None
            assert event1.payload["trade_id"] == "1"

            # Wait for reconnect + second message
            # Backoff starts at 0.5s, so we need to wait a bit longer
            await asyncio.sleep(0.8)
            event2 = await collector.next_event()
            assert event2 is not None
            assert event2.payload["trade_id"] == "2"

            await collector.stop()

        # Should have connected at least twice
        assert connect_count >= 2

    @pytest.mark.asyncio
    async def test_cancelled_error_stops_immediately(self) -> None:
        """CancelledError stops collector without further reconnect attempts."""
        connect_count = 0

        async def connect_mock(*args: object, **kwargs: object) -> MockWebSocket:
            nonlocal connect_count
            connect_count += 1
            return MockWebSocket([])

        with patch("websockets.connect", side_effect=connect_mock):
            collector = OKXTradesLiveCollector()
            await collector.start()

            # Let it connect
            await asyncio.sleep(0.1)
            initial_count = connect_count

            # Stop the collector (sends CancelledError to receiver task)
            await collector.stop()

            # Give time for any errant reconnects
            await asyncio.sleep(0.3)

            # Should not have reconnected after stop
            assert connect_count == initial_count
            assert not collector.is_running

    @pytest.mark.asyncio
    async def test_reconnects_on_connection_refused(self) -> None:
        """Collector reconnects on ConnectionRefusedError."""
        connect_count = 0

        async def connect_mock(*args: object, **kwargs: object) -> MockWebSocket:
            nonlocal connect_count
            connect_count += 1
            if connect_count == 1:
                raise ConnectionRefusedError("refused")
            # Success on second attempt
            return MockWebSocket([make_trade_message("BTC-USDT", "1", "42000", "0.1", "buy")])

        with patch("websockets.connect", side_effect=connect_mock):
            collector = OKXTradesLiveCollector()
            await collector.start()

            # Wait for reconnect (initial backoff is 0.5s + jitter)
            await asyncio.sleep(0.8)

            event = await collector.next_event()
            assert event is not None
            assert event.payload["trade_id"] == "1"

            await collector.stop()

        assert connect_count >= 2

    @pytest.mark.asyncio
    async def test_backoff_increases_on_repeated_failures(self) -> None:
        """Backoff increases exponentially on repeated connection failures."""
        connect_times: list[float] = []
        import time

        async def connect_mock(*args: object, **kwargs: object) -> MockWebSocket:
            connect_times.append(time.monotonic())
            if len(connect_times) < 4:
                raise ConnectionRefusedError("refused")
            # Success on 4th attempt
            return MockWebSocket([make_trade_message("BTC-USDT", "1", "42000", "0.1", "buy")])

        with patch("websockets.connect", side_effect=connect_mock):
            collector = OKXTradesLiveCollector()
            await collector.start()

            # Wait for successful connection after retries
            # 0.5 + 1.0 + 2.0 = 3.5s minimum, plus some margin
            await asyncio.sleep(4.5)

            await collector.stop()

        # Should have 4 connection attempts
        assert len(connect_times) >= 4

        # Verify backoff increased (each gap should be roughly 2x previous)
        # Gap 1: ~0.5s, Gap 2: ~1.0s, Gap 3: ~2.0s
        if len(connect_times) >= 3:
            gap1 = connect_times[1] - connect_times[0]
            gap2 = connect_times[2] - connect_times[1]
            # Second gap should be larger than first (exponential)
            assert gap2 > gap1 * 1.5  # Allow for jitter variance
