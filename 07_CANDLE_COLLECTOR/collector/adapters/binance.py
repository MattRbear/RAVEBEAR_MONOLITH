import json
import logging
from typing import Optional

import aiohttp

from .base import Candle, ExchangeAdapter
from ..utils.time import now_ms


class BinanceAdapter(ExchangeAdapter):
    def __init__(self, config, session: aiohttp.ClientSession):
        self.config = config
        self.session = session
        self.logger = logging.getLogger(__name__)

    async def fetch_klines(self, symbol, timeframe, start_ms, end_ms, limit):
        params = {
            "symbol": symbol,
            "interval": timeframe,
            "startTime": int(start_ms),
            "endTime": int(end_ms),
            "limit": int(limit),
        }
        url = f"{self.config.rest_url}/api/v3/klines"
        async with self.session.get(url, params=params) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"binance_rest_error status={resp.status} body={text}")
            data = await resp.json()

        candles = []
        for row in data:
            candles.append(
                {
                    "exchange": "binance",
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "open_time_ms": int(row[0]),
                    "close_time_ms": int(row[6]),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                    "quote_volume": float(row[7]),
                    "trades_count": int(row[8]),
                    "is_closed": True,
                    "source": "rest_backfill",
                    "ingest_time_ms": now_ms(),
                }
            )
        return candles

    async def connect_ws(self):
        return await self.session.ws_connect(self.config.ws_url, heartbeat=20)

    async def subscribe(self, ws, symbols, timeframes):
        streams = []
        for symbol in symbols:
            for timeframe in timeframes:
                streams.append(f"{symbol.lower()}@kline_{timeframe}")
        payload = {"method": "SUBSCRIBE", "params": streams, "id": 1}
        await ws.send_str(json.dumps(payload))

    def parse_ws_message(self, msg) -> Optional[Candle]:
        if "result" in msg:
            return None

        data = msg.get("data") if "stream" in msg else msg
        if data.get("e") != "kline":
            return None
        kline = data.get("k") or {}

        try:
            return {
                "exchange": "binance",
                "symbol": data.get("s") or kline.get("s"),
                "timeframe": kline["i"],
                "open_time_ms": int(kline["t"]),
                "close_time_ms": int(kline["T"]),
                "open": float(kline["o"]),
                "high": float(kline["h"]),
                "low": float(kline["l"]),
                "close": float(kline["c"]),
                "volume": float(kline["v"]),
                "quote_volume": float(kline.get("q", 0.0)),
                "trades_count": int(kline.get("n", 0)),
                "is_closed": bool(kline.get("x", False)),
                "source": "websocket",
                "ingest_time_ms": now_ms(),
            }
        except (KeyError, ValueError, TypeError) as exc:
            self.logger.error("event=ws_parse_error error=%s", exc)
            return None
