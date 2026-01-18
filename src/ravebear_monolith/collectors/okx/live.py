"""OKX live trades collector using WebSocket.

Connects to OKX public WebSocket and streams trade events.
"""

import asyncio
import json
import logging
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from ravebear_monolith.collectors.base import CollectorBase, CollectorEvent
from ravebear_monolith.collectors.okx.schemas import OKXTrade
from ravebear_monolith.util.logging import log_event

logger = logging.getLogger(__name__)


class OKXTradesLiveCollector(CollectorBase):
    """Live OKX trades collector using WebSocket.

    Connects to OKX public WebSocket API and subscribes to trades channel.

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
        """Connect to WebSocket and subscribe to trades channel."""
        self._running = True
        self._should_stop = False

        try:
            self._ws = await websockets.connect(self._ws_url)
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
            await self._ws.send(json.dumps(subscribe_msg))
            log_event(
                logger,
                logging.INFO,
                f"Subscribed to trades: {self._inst_id}",
                event="okx_subscribed",
                inst_id=self._inst_id,
            )

            # Start receiver task
            self._receiver_task = asyncio.create_task(self._receive_loop())

        except Exception as e:
            log_event(
                logger,
                logging.ERROR,
                f"Failed to connect to OKX: {e}",
                event="okx_connect_error",
                error=str(e),
            )
            self._running = False
            raise

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

        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

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

    async def _receive_loop(self) -> None:
        """Background task to receive WebSocket messages."""
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
                break
            except WebSocketException as e:
                log_event(
                    logger,
                    logging.ERROR,
                    f"OKX WebSocket error: {e}",
                    event="okx_ws_error",
                    error=str(e),
                )
                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                log_event(
                    logger,
                    logging.ERROR,
                    f"Unexpected error in receive loop: {e}",
                    event="okx_receive_error",
                    error=str(e),
                )
                await asyncio.sleep(0.1)

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
