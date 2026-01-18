"""
Kraken API adapter - handles REST vs WS v2 symbol format differences.

REST API: Uses XXBTZUSD, XETHZUSD format
WS v2 API: Uses BTC/USD, ETH/USD format
"""
import json
import logging
from typing import List, Optional

import aiohttp

from ..utils.time import now_ms
from ..utils.time_normalizer import normalize_timestamp_seconds, normalize_iso8601


class KrakenAdapter:
    """Kraken API adapter."""
    
    VENUE = "kraken"
    REST_URL = "https://api.kraken.com"
    WS_URL = "wss://ws.kraken.com/v2"
    
    INTERVAL_MAP = {
        "1m": 1,
        "5m": 5,
        "15m": 15,
        "30m": 30,
        "1h": 60,
        "4h": 240,
        "1d": 1440,
    }
    
    # Map WS v2 symbols to REST API symbols
    REST_SYMBOL_MAP = {
        "BTC/USD": "XBTUSD",
        "ETH/USD": "ETHUSD",
        "SOL/USD": "SOLUSD",
        "AVAX/USD": "AVAXUSD",
        "LINK/USD": "LINKUSD",
        "UNI/USD": "UNIUSD",
        "AAVE/USD": "AAVEUSD",
        "DOT/USD": "DOTUSD",
        "ATOM/USD": "ATOMUSD",
    }
    
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.logger = logging.getLogger(__name__)
        self.ws_buffer = {}
    
    def _get_rest_symbol(self, symbol: str) -> str:
        """Convert WS v2 symbol to REST symbol."""
        return self.REST_SYMBOL_MAP.get(symbol, symbol.replace("/", ""))
    
    async def fetch_candles(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int
    ) -> List[dict]:
        """Fetch OHLC via REST API."""
        if timeframe not in self.INTERVAL_MAP:
            return []
        
        interval = self.INTERVAL_MAP[timeframe]
        since = int(start_ms / 1000)
        rest_symbol = self._get_rest_symbol(symbol)
        
        url = f"{self.REST_URL}/0/public/OHLC"
        params = {
            "pair": rest_symbol,
            "interval": interval,
            "since": since,
        }
        
        try:
            async with self.session.get(url, params=params) as resp:
                if resp.status != 200:
                    return []
                
                data = await resp.json()
                
                errors = data.get("error", [])
                if errors:
                    self.logger.warning("REST error %s: %s", symbol, errors)
                    return []
                
                result = data.get("result", {})
                
                # Find OHLC data (key varies)
                ohlc_data = None
                for key, value in result.items():
                    if key != "last" and isinstance(value, list):
                        ohlc_data = value
                        break
                
                if not ohlc_data:
                    return []
                
                candles = []
                granularity_ms = interval * 60 * 1000
                
                for row in ohlc_data:
                    if len(row) < 8:
                        continue
                    
                    time_sec = int(row[0])
                    open_time_ms = normalize_timestamp_seconds(time_sec, timeframe)
                    
                    if open_time_ms > end_ms:
                        break
                    
                    candles.append({
                        "venue": self.VENUE,
                        "symbol": symbol,  # Keep WS v2 format
                        "timeframe": timeframe,
                        "open_time_ms": open_time_ms,
                        "close_time_ms": open_time_ms + granularity_ms - 1,
                        "open": float(row[1]),
                        "high": float(row[2]),
                        "low": float(row[3]),
                        "close": float(row[4]),
                        "volume": float(row[6]),
                        "quote_volume": None,
                        "vwap": float(row[5]) if row[5] else None,
                        "trades_count": int(row[7]) if len(row) > 7 else None,
                        "is_closed": True,
                        "source": "rest",
                        "ingest_time_ms": now_ms(),
                    })
                
                return candles
                
        except Exception as exc:
            self.logger.warning("REST exception %s: %s", symbol, exc)
            return []
    
    async def connect_ws(self) -> aiohttp.ClientWebSocketResponse:
        """Connect to Kraken WS v2."""
        ws = await self.session.ws_connect(self.WS_URL, heartbeat=30)
        return ws
    
    async def subscribe(self, ws: aiohttp.ClientWebSocketResponse, symbols: List[str], timeframes: List[str]):
        """Subscribe to OHLC channel."""
        intervals = [self.INTERVAL_MAP.get(tf, 1) for tf in timeframes]
        
        subscribe_msg = {
            "method": "subscribe",
            "params": {
                "channel": "ohlc",
                "symbol": symbols,
                "interval": intervals[0] if intervals else 1,
            }
        }
        
        await ws.send_str(json.dumps(subscribe_msg))
    
    def parse_ws_message(self, msg: dict) -> Optional[dict]:
        """Parse Kraken WS v2 OHLC message."""
        try:
            if not isinstance(msg, dict):
                return None
            
            channel = msg.get("channel", "")
            
            # Skip non-OHLC
            if channel in ("heartbeat", "status"):
                return None
            if "method" in msg:
                return None
            if channel != "ohlc":
                return None
            if msg.get("type") != "update":
                return None
            
            data_list = msg.get("data", [])
            if not data_list:
                return None
            
            candle_data = data_list[0]
            
            interval_begin = candle_data.get("interval_begin", "")
            symbol = candle_data.get("symbol", "")
            interval = candle_data.get("interval", 1)
            
            timeframe_map = {1: "1m", 5: "5m", 15: "15m", 30: "30m", 
                           60: "1h", 240: "4h", 1440: "1d"}
            timeframe = timeframe_map.get(interval, "1m")
            granularity_ms = interval * 60 * 1000
            
            open_time_ms = normalize_iso8601(interval_begin, timeframe)
            trades_count = candle_data.get("trades", candle_data.get("count", 0))
            
            current_candle = {
                "venue": self.VENUE,
                "symbol": symbol,
                "timeframe": timeframe,
                "open_time_ms": open_time_ms,
                "close_time_ms": open_time_ms + granularity_ms - 1,
                "open": float(candle_data.get("open", 0)),
                "high": float(candle_data.get("high", 0)),
                "low": float(candle_data.get("low", 0)),
                "close": float(candle_data.get("close", 0)),
                "volume": float(candle_data.get("volume", 0)),
                "quote_volume": None,
                "vwap": float(candle_data.get("vwap", 0)) if candle_data.get("vwap") else None,
                "trades_count": int(trades_count) if trades_count else None,
                "is_closed": True,
                "source": "websocket",
                "ingest_time_ms": now_ms(),
            }
            
            # Buffer for finality
            buffer_key = (symbol, timeframe)
            previous = self.ws_buffer.get(buffer_key)
            self.ws_buffer[buffer_key] = current_candle
            
            if previous and previous["open_time_ms"] != current_candle["open_time_ms"]:
                return previous
            
            return None
            
        except Exception:
            return None
