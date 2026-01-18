"""
Coinbase Exchange API adapter (PUBLIC - no auth required).
Uses the public Exchange API, not the Advanced Trade API.
"""
import json
import logging
from typing import List, Optional

import aiohttp

from ..utils.time import now_ms
from ..utils.time_normalizer import normalize_timestamp_seconds


class CoinbaseAdapter:
    """Coinbase public Exchange API adapter."""
    
    VENUE = "coinbase"
    # PUBLIC Exchange API - no authentication needed
    REST_URL = "https://api.exchange.coinbase.com"
    WS_URL = "wss://ws-feed.exchange.coinbase.com"
    
    # Granularity in seconds
    GRANULARITY_MAP = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "1h": 3600,
        "6h": 21600,
        "1d": 86400,
    }
    
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.logger = logging.getLogger(__name__)
        self.ws_buffer = {}
    
    async def fetch_candles(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int
    ) -> List[dict]:
        """
        Fetch candles via PUBLIC Exchange API.
        Endpoint: GET /products/{product_id}/candles
        Response: [[time, low, high, open, close, volume], ...]
        """
        if timeframe not in self.GRANULARITY_MAP:
            raise ValueError(f"Unsupported timeframe: {timeframe}")
        
        granularity = self.GRANULARITY_MAP[timeframe]
        start_sec = int(start_ms / 1000)
        end_sec = int(end_ms / 1000)
        
        # Public Exchange API endpoint
        url = f"{self.REST_URL}/products/{symbol}/candles"
        params = {
            "start": start_sec,
            "end": end_sec,
            "granularity": granularity,
        }
        
        try:
            async with self.session.get(url, params=params) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    self.logger.error("REST error %s: %d - %s", symbol, resp.status, text[:100])
                    return []
                
                # Response is array: [[time, low, high, open, close, volume], ...]
                data = await resp.json()
                
                if not isinstance(data, list):
                    return []
                
                candles = []
                for row in data:
                    if len(row) < 6:
                        continue
                    
                    time_sec = int(row[0])
                    open_time_ms = normalize_timestamp_seconds(time_sec, timeframe)
                    
                    candles.append({
                        "venue": self.VENUE,
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "open_time_ms": open_time_ms,
                        "close_time_ms": open_time_ms + (granularity * 1000) - 1,
                        "open": float(row[3]),
                        "high": float(row[2]),
                        "low": float(row[1]),
                        "close": float(row[4]),
                        "volume": float(row[5]),
                        "quote_volume": None,
                        "vwap": None,
                        "trades_count": None,
                        "is_closed": True,
                        "source": "rest",
                        "ingest_time_ms": now_ms(),
                    })
                
                return candles
                
        except Exception as exc:
            self.logger.error("REST exception %s: %s", symbol, exc)
            return []
    
    async def connect_ws(self) -> aiohttp.ClientWebSocketResponse:
        """Connect to Coinbase Exchange WebSocket."""
        ws = await self.session.ws_connect(self.WS_URL, heartbeat=30)
        self.logger.info("event=ws_connected venue=%s", self.VENUE)
        return ws
    
    async def subscribe(self, ws: aiohttp.ClientWebSocketResponse, symbols: List[str], timeframes: List[str]):
        """
        Subscribe to matches channel (trade-by-trade).
        Note: Public Exchange WS doesn't have candle channel, we get matches.
        """
        subscribe_msg = {
            "type": "subscribe",
            "product_ids": symbols,
            "channels": ["ticker"]  # Use ticker for price updates
        }
        
        await ws.send_str(json.dumps(subscribe_msg))
        self.logger.info("event=ws_subscribe venue=%s symbols=%s", self.VENUE, symbols)
    
    def parse_ws_message(self, msg: dict) -> Optional[dict]:
        """
        Parse WebSocket ticker message.
        Coinbase public WS sends ticker updates, not candles.
        We'll build candles from ticker data.
        
        Ticker format: {
            "type": "ticker",
            "product_id": "BTC-USD",
            "price": "50000.00",
            "time": "2024-01-01T00:00:00.000000Z",
            ...
        }
        """
        try:
            msg_type = msg.get("type")
            
            # Skip non-ticker messages
            if msg_type == "subscriptions":
                return None
            if msg_type == "heartbeat":
                return None
            if msg_type != "ticker":
                return None
            
            symbol = msg.get("product_id")
            price_str = msg.get("price")
            time_str = msg.get("time")
            volume_24h = msg.get("volume_24h", "0")
            
            if not symbol or not price_str or not time_str:
                return None
            
            price = float(price_str)
            
            # Parse ISO timestamp to epoch ms
            from datetime import datetime
            try:
                dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                ts_ms = int(dt.timestamp() * 1000)
            except:
                return None
            
            # Round down to minute boundary
            minute_ms = (ts_ms // 60000) * 60000
            
            # Build/update 1-minute candle in buffer
            buffer_key = (symbol, "1m")
            
            if buffer_key not in self.ws_buffer:
                # Start new candle
                self.ws_buffer[buffer_key] = {
                    "venue": self.VENUE,
                    "symbol": symbol,
                    "timeframe": "1m",
                    "open_time_ms": minute_ms,
                    "close_time_ms": minute_ms + 60000 - 1,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": 0,  # Can't get volume from ticker
                    "quote_volume": None,
                    "vwap": None,
                    "trades_count": None,
                    "is_closed": False,
                    "source": "websocket",
                    "ingest_time_ms": now_ms(),
                }
                return None
            
            current = self.ws_buffer[buffer_key]
            
            # Check if new minute started
            if minute_ms > current["open_time_ms"]:
                # Finalize previous candle
                previous = current.copy()
                previous["is_closed"] = True
                
                # Start new candle
                self.ws_buffer[buffer_key] = {
                    "venue": self.VENUE,
                    "symbol": symbol,
                    "timeframe": "1m",
                    "open_time_ms": minute_ms,
                    "close_time_ms": minute_ms + 60000 - 1,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": 0,
                    "quote_volume": None,
                    "vwap": None,
                    "trades_count": None,
                    "is_closed": False,
                    "source": "websocket",
                    "ingest_time_ms": now_ms(),
                }
                
                return previous
            
            # Update current candle
            current["high"] = max(current["high"], price)
            current["low"] = min(current["low"], price)
            current["close"] = price
            current["ingest_time_ms"] = now_ms()
            
            return None
            
        except Exception as exc:
            # Don't log parse errors for non-candle messages
            return None
