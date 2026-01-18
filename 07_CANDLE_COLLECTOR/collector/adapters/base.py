from abc import ABC, abstractmethod
from typing import Optional, TypedDict


class Candle(TypedDict, total=False):
    exchange: str
    symbol: str
    timeframe: str
    open_time_ms: int
    close_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: Optional[float]
    trades_count: Optional[int]
    is_closed: Optional[bool]
    source: str
    ingest_time_ms: int


class ExchangeAdapter(ABC):
    @abstractmethod
    async def fetch_klines(self, symbol, timeframe, start_ms, end_ms, limit):
        raise NotImplementedError

    @abstractmethod
    async def connect_ws(self):
        raise NotImplementedError

    @abstractmethod
    async def subscribe(self, ws, symbols, timeframes):
        raise NotImplementedError

    @abstractmethod
    def parse_ws_message(self, msg) -> Optional[Candle]:
        raise NotImplementedError
