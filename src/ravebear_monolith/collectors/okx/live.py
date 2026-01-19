"""OKX live trades collector using WebSocket.

Connects to OKX public WebSocket and streams trade events.
Auto-reconnects on network failures with exponential backoff.
"""

import asyncio
import json
import logging
import random
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from ravebear_monolith.collectors.base import CollectorBase, CollectorEvent
from ravebear_monolith.collectors.okx.schemas import OKXTrade
from ravebear_monolith.util.errors import ConfigError
from ravebear_monolith.util.logging import log_event

logger = logging.getLogger(__name__)

# Reconnect backoff settings (match retry.py patterns)
_INITIAL_BACKOFF_S = 0.5
_MAX_BACKOFF_S = 30.0
_BACKOFF_JITTER = 0.10


def _is_recoverable_error(exc: Exception) -> bool:
    """Check if error should trigger reconnect vs immediate re-raise.

    Reconnects on: network/websocket failures.
    Re-raises on: config errors, validation errors, programmer errors.
    """
    # Fatal errors - do not reconnect
    if isinstance(exc, (ConfigError, ValueError, TypeError, KeyError, AttributeError)):
        return False
    # WebSocket and network errors - reconnect
    if isinstance(exc, (ConnectionClosed, WebSocketException, OSError, asyncio.TimeoutError)):
        return True
    # ConnectionRefusedError, TimeoutError, etc. are OSError subclasses
    return True


class OKXTradesLiveCollector(CollectorBase):
    """Live OKX trades collector using WebSocket.

    Connects to OKX public WebSocket API and subscribes to trades channel.
    Auto-reconnects on disconnect with exponential backoff (cap 30s).

    Args:
        inst_id: Instrument ID to subscribe to (e.g., "BTC-USDT").
        ws_url: OKX WebSocket URL.
    """

    def __init__(
        self,
        inst_id: str = "BTC-USDT",
        ws_url: str = "wss://ws.okx.com:8443/ws/v5/public",
    ) -> None:
        super().__init__("okx_trades_live")
        self._inst_id = inst_id
        self._ws_url = ws_url
        self._ws: Any = None
        self._event_queue: asyncio.Queue[CollectorEvent] = asyncio.Queue()
        self._receiver_task: asyncio.Task[None] | None = None
        self._should_stop = False

    async def start(self) -> None:
        """Connect to WebSocket and start receiver with reconnect loop."""
        self._running = True
        self._should_stop = False

        # Start receiver task (handles connect + reconnect internally)
        self._receiver_task = asyncio.create_task(self._receive_loop())

    async def stop(self) -> None:
        """Close WebSocket and stop receiver."""
        self._should_stop = True
        self._running = False

        if self._receiver_task:
            self._receiver_task.cancel()
            try:
                await self._receiver_task
            except asyncio.CancelledError:
                pass
            self._receiver_task = None

        await self._close_ws()

        log_event(
            logger,
            logging.INFO,
            "OKX WebSocket disconnected",
            event="okx_ws_disconnected",
        )

    async def next_event(self) -> CollectorEvent | None:
        """Get next trade event from queue.

        Returns:
            CollectorEvent if available, None if not running or queue empty.
        """
        if not self._running and self._event_queue.empty():
            return None

        try:
            # Non-blocking get with short timeout
            event = await asyncio.wait_for(self._event_queue.get(), timeout=0.1)
            return event
        except asyncio.TimeoutError:
            return None

    async def _close_ws(self) -> None:
        """Close current WebSocket connection if open."""
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    async def _connect_and_subscribe(self) -> Any:
        """Connect to WebSocket and subscribe to trades channel.

        Returns:
            WebSocket connection handle.

        Raises:
            WebSocketException, OSError: On connection failure.
        """
        ws = await websockets.connect(self._ws_url)
        log_event(
            logger,
            logging.INFO,
            f"Connected to OKX WebSocket: {self._ws_url}",
            event="okx_ws_connected",
            url=self._ws_url,
        )

        # Subscribe to trades channel
        subscribe_msg = {
            "op": "subscribe",
            "args": [{"channel": "trades", "instId": self._inst_id}],
        }
        await ws.send(json.dumps(subscribe_msg))
        log_event(
            logger,
            logging.INFO,
            f"Subscribed to trades: {self._inst_id}",
            event="okx_subscribed",
            inst_id=self._inst_id,
        )

        return ws

    async def _receive_loop(self) -> None:
        """Background task with reconnect loop.

        Connects, receives messages, and reconnects on failure.
        Respects cancellation - exits immediately on CancelledError.
        """
        backoff_s = _INITIAL_BACKOFF_S
        reconnect_count = 0

        while not self._should_stop:
            try:
                # Connect and subscribe
                self._ws = await self._connect_and_subscribe()
                # Reset backoff on successful connect
                backoff_s = _INITIAL_BACKOFF_S
                reconnect_count = 0

                # Message receive loop
                while not self._should_stop and self._ws:
                    try:
                        message = await self._ws.recv()
                        await self._process_message(message)
                    except ConnectionClosed:
                        log_event(
                            logger,
                            logging.WARNING,
                            "OKX WebSocket connection closed",
                            event="okx_ws_closed",
                        )
                        break  # Exit inner loop to reconnect
                    except asyncio.CancelledError:
                        raise  # Propagate cancellation immediately
                    except WebSocketException as e:
                        log_event(
                            logger,
                            logging.ERROR,
                            f"OKX WebSocket error: {e}",
                            event="okx_ws_error",
                            error=str(e),
                        )
                        break  # Exit inner loop to reconnect

            except asyncio.CancelledError:
                # Exit immediately on cancellation - no reconnect
                return

            except Exception as e:
                # Check if we should reconnect or re-raise
                if not _is_recoverable_error(e):
                    log_event(
                        logger,
                        logging.ERROR,
                        f"Fatal error in OKX collector: {e}",
                        event="okx_fatal_error",
                        error=str(e),
                    )
                    self._running = False
                    raise

                reconnect_count += 1
                log_event(
                    logger,
                    logging.WARNING,
                    f"OKX connection error, will reconnect: {e}",
                    event="okx_reconnect_pending",
                    error=str(e),
                    attempt=reconnect_count,
                    backoff_s=round(backoff_s, 2),
                )

            finally:
                # Always close old connection before reconnect
                await self._close_ws()

            # Don't reconnect if stopped
            if self._should_stop:
                break

            # Backoff before reconnect (with jitter)
            jitter = 1 + random.uniform(-_BACKOFF_JITTER, _BACKOFF_JITTER)
            sleep_time = backoff_s * jitter

            try:
                await asyncio.sleep(sleep_time)
            except asyncio.CancelledError:
                return  # Exit on cancellation during sleep

            # Exponential backoff for next attempt
            backoff_s = min(backoff_s * 2, _MAX_BACKOFF_S)

    async def _process_message(self, message: str) -> None:
        """Process incoming WebSocket message.

        Args:
            message: Raw JSON message from WebSocket.
        """
        try:
            data = json.loads(message)

            # Skip subscription confirmations and pings
            if "event" in data:
                return

            # Process trade data
            if "data" in data and "arg" in data:
                arg = data["arg"]
                if arg.get("channel") == "trades":
                    for trade_raw in data["data"]:
                        trade = OKXTrade.from_raw(trade_raw)
                        event = CollectorEvent(
                            source="okx",
                            event_type="trade",
                            ts_utc=trade.ts_utc,
                            payload=trade.model_dump(),
                        )
                        await self._event_queue.put(event)

        except json.JSONDecodeError as e:
            log_event(
                logger,
                logging.WARNING,
                f"Invalid JSON from OKX: {e}",
                event="okx_json_error",
                error=str(e),
            )
        except Exception as e:
            log_event(
                logger,
                logging.WARNING,
                f"Error processing OKX message: {e}",
                event="okx_process_error",
                error=str(e),
            )
