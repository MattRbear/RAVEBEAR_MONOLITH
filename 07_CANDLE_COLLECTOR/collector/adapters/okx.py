"""
OKX API adapter - FIXED VERSION.
Supports REST (candlesticks endpoint) and WebSocket (candlesticks channel).

FIXES:
1. Application-level ping/pong (OKX requires ping every 30s)
2. Better message type detection
3. Emit candles immediately without buffering (OKX confirms with 'confirm' field)
"""
import asyncio
import json
import logging
from typing import List, Optional

import aiohttp

from ..utils.time import now_ms


class OKXAdapter:
    """OKX API adapter with volume breakdown and confirmation status."""
    
    VENUE = "okx"
    REST_URL = "https://www.okx.com"
    WS_URL = "wss://ws.okx.com:8443/ws/v5/business"
    
    # Bar/interval mapping
    BAR_MAP = {
        "1m": "1m",
        "3m": "3m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1H",
        "2h": "2H",
        "4h": "4H",
        "6h": "6H",
        "12h": "12H",
        "1d": "1D",
    }
    
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.logger = logging.getLogger(__name__)
        self._ping_task: Optional[asyncio.Task] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
    
    async def fetch_candles(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int
    ) -> List[dict]:
        """
        Fetch historical candlesticks via REST API.
        """
        if timeframe not in self.BAR_MAP:
            raise ValueError(f"Unsupported timeframe: {timeframe}")
        
        bar = self.BAR_MAP[timeframe]
        
        url = f"{self.REST_URL}/api/v5/market/candles"
        params = {
            "instId": symbol,
            "bar": bar,
            "after": str(start_ms),
            "limit": "300",
        }
        
        try:
            async with self.session.get(url, params=params) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    self.logger.error("event=rest_error venue=%s status=%d body=%s",
                                    self.VENUE, resp.status, text)
                    return []
                
                data = await resp.json()
                
                if data.get("code") != "0":
                    self.logger.error("event=rest_api_error venue=%s code=%s msg=%s",
                                    self.VENUE, data.get("code"), data.get("msg"))
                    return []
                
                candles_data = data.get("data", [])
                candles = []
                
                for row in candles_data:
                    ts_ms = int(row[0])
                    if ts_ms > end_ms:
                        continue
                    
                    granularity_ms = self._get_granularity_ms(timeframe)
                    
                    candles.append({
                        "venue": self.VENUE,
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "open_time_ms": ts_ms,
                        "close_time_ms": ts_ms + granularity_ms - 1,
                        "open": float(row[1]),
                        "high": float(row[2]),
                        "low": float(row[3]),
                        "close": float(row[4]),
                        "volume": float(row[5]),
                        "quote_volume": float(row[7]) if len(row) > 7 else None,
                        "vwap": None,
                        "trades_count": None,
                        "vol_ccy": float(row[6]) if len(row) > 6 else None,
                        "vol_ccy_quote": float(row[7]) if len(row) > 7 else None,
                        "is_closed": row[8] == "1" if len(row) > 8 else True,
                        "source": "rest",
                        "ingest_time_ms": now_ms(),
                    })
                
                self.logger.info("event=rest_fetch venue=%s symbol=%s timeframe=%s count=%d",
                               self.VENUE, symbol, timeframe, len(candles))
                return candles
                
        except Exception as exc:
            self.logger.error("event=rest_exception venue=%s symbol=%s error=%s",
                            self.VENUE, symbol, exc)
            return []
    
    async def connect_ws(self) -> aiohttp.ClientWebSocketResponse:
        """Connect to OKX WebSocket."""
        self._ws = await self.session.ws_connect(self.WS_URL, heartbeat=20)
        self.logger.info("event=ws_connected venue=%s", self.VENUE)
        
        # Start application-level ping task
        self._start_ping_task()
        
        return self._ws
    
    def _start_ping_task(self):
        """Start the application-level ping task for OKX."""
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
        self._ping_task = asyncio.create_task(self._ping_loop())
    
    async def _ping_loop(self):
        """Send ping every 25 seconds to keep OKX connection alive."""
        try:
            while True:
                await asyncio.sleep(25)
                if self._ws and not self._ws.closed:
                    await self._ws.send_str('ping')
                    self.logger.debug("event=ws_ping_sent venue=%s", self.VENUE)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self.logger.warning("event=ws_ping_error venue=%s error=%s", self.VENUE, exc)
    
    def stop_ping(self):
        """Stop the ping task."""
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
    
    async def subscribe(self, ws: aiohttp.ClientWebSocketResponse, symbols: List[str], timeframes: List[str]):
        """Subscribe to candlesticks channel."""
        args = []
        for symbol in symbols:
            for timeframe in timeframes:
                channel = f"candle{self.BAR_MAP.get(timeframe, '1m')}"
                args.append({
                    "channel": channel,
                    "instId": symbol,
                })
        
        subscribe_msg = {
            "op": "subscribe",
            "args": args,
        }
        
        await ws.send_str(json.dumps(subscribe_msg))
        self.logger.info("event=ws_subscribe venue=%s symbols=%s timeframes=%s",
                        self.VENUE, symbols, timeframes)
    
    def parse_ws_message(self, msg: dict) -> Optional[dict]:
        """
        Parse WebSocket candlesticks message.
        
        OKX message types:
        - Subscription confirmation: {"event": "subscribe", "arg": {...}}
        - Pong response: "pong" (string, not dict)
        - Candle data: {"arg": {"channel": "candle1m", ...}, "data": [[...]]}
        - Error: {"event": "error", "code": "...", "msg": "..."}
        """
        try:
            # Handle string messages (pong response)
            if isinstance(msg, str):
                if msg == "pong":
                    self.logger.debug("event=ws_pong_received venue=%s", self.VENUE)
                return None
            
            # Skip subscription confirmations and other events
            if "event" in msg:
                event_type = msg.get("event")
                if event_type == "error":
                    self.logger.warning("event=ws_event_error venue=%s code=%s msg=%s",
                                      self.VENUE, msg.get("code"), msg.get("msg"))
                elif event_type == "subscribe":
                    self.logger.debug("event=ws_subscribed venue=%s arg=%s", 
                                    self.VENUE, msg.get("arg"))
                return None
            
            arg = msg.get("arg", {})
            channel = arg.get("channel", "")
            
            if not channel.startswith("candle"):
                return None
            
            data_list = msg.get("data", [])
            if not data_list:
                return None
            
            candle_data = data_list[0]
            
            # Extract timeframe from channel
            tf_raw = channel.replace("candle", "")
            timeframe = tf_raw.lower()
            if timeframe.endswith("h") and len(timeframe) == 2:
                timeframe = timeframe  # Already correct format like "1h"
            elif timeframe.endswith("d") and len(timeframe) == 2:
                timeframe = timeframe
            
            # Parse candle data
            ts_ms = int(candle_data[0])
            confirm = candle_data[8] if len(candle_data) > 8 else "0"
            
            # Only process confirmed/closed candles
            if confirm != "1":
                return None
            
            granularity_ms = self._get_granularity_ms(timeframe)
            
            return {
                "venue": self.VENUE,
                "symbol": arg.get("instId"),
                "timeframe": timeframe,
                "open_time_ms": ts_ms,
                "close_time_ms": ts_ms + granularity_ms - 1,
                "open": float(candle_data[1]),
                "high": float(candle_data[2]),
                "low": float(candle_data[3]),
                "close": float(candle_data[4]),
                "volume": float(candle_data[5]),
                "quote_volume": float(candle_data[7]) if len(candle_data) > 7 else None,
                "vwap": None,
                "trades_count": None,
                "vol_ccy": float(candle_data[6]) if len(candle_data) > 6 else None,
                "vol_ccy_quote": float(candle_data[7]) if len(candle_data) > 7 else None,
                "is_closed": True,
                "source": "websocket",
                "ingest_time_ms": now_ms(),
            }
            
        except (KeyError, ValueError, TypeError, IndexError) as exc:
            # Only log actual parsing errors, not expected non-candle messages
            if "data" in str(msg) and "candle" in str(msg):
                self.logger.error("event=ws_parse_error venue=%s error=%s msg=%s",
                                self.VENUE, exc, msg)
            return None
    
    def _get_granularity_ms(self, timeframe: str) -> int:
        """Get granularity in milliseconds for a timeframe."""
        mapping = {
            "1m": 60 * 1000,
            "3m": 3 * 60 * 1000,
            "5m": 5 * 60 * 1000,
            "15m": 15 * 60 * 1000,
            "30m": 30 * 60 * 1000,
            "1h": 60 * 60 * 1000,
            "2h": 2 * 60 * 60 * 1000,
            "4h": 4 * 60 * 60 * 1000,
            "6h": 6 * 60 * 60 * 1000,
            "12h": 12 * 60 * 60 * 1000,
            "1d": 24 * 60 * 60 * 1000,
        }
        return mapping.get(timeframe, 60 * 1000)
