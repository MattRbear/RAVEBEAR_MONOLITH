"""
Unified candle schema supporting multiple venues (Coinbase, Kraken, OKX).
Includes venue-specific fields with proper nullable handling.
"""
from dataclasses import dataclass, field
from typing import Optional
import pyarrow as pa


@dataclass
class Candle:
    """Unified candle data structure for all venues."""
    
    # Core fields (all venues)
    venue: str
    symbol: str
    timeframe: str
    open_time_ms: int
    close_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    
    # Extended fields (common)
    quote_volume: Optional[float] = None
    source: str = "unknown"
    ingest_time_ms: int = 0
    
    # Venue-specific fields
    vwap: Optional[float] = None              # Kraken only
    trades_count: Optional[int] = None        # Kraken only
    vol_ccy: Optional[float] = None           # OKX only (volume in contracts)
    vol_ccy_quote: Optional[float] = None     # OKX only (quote currency volume)
    is_closed: Optional[bool] = None          # OKX provides, infer for others
    
    def to_dict(self):
        """Convert to dictionary for Parquet writing."""
        return {
            "venue": self.venue,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "open_time_ms": self.open_time_ms,
            "close_time_ms": self.close_time_ms,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "quote_volume": self.quote_volume,
            "vwap": self.vwap,
            "trades_count": self.trades_count,
            "vol_ccy": self.vol_ccy,
            "vol_ccy_quote": self.vol_ccy_quote,
            "is_closed": self.is_closed,
            "source": self.source,
            "ingest_time_ms": self.ingest_time_ms,
        }


# PyArrow schema for Parquet files
CANDLE_SCHEMA = pa.schema([
    ("venue", pa.string()),
    ("symbol", pa.string()),
    ("timeframe", pa.string()),
    ("open_time_ms", pa.int64()),
    ("close_time_ms", pa.int64()),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.float64()),
    ("quote_volume", pa.float64()),  # Nullable
    ("vwap", pa.float64()),          # Nullable (Kraken only)
    ("trades_count", pa.int64()),    # Nullable (Kraken only)
    ("vol_ccy", pa.float64()),       # Nullable (OKX only)
    ("vol_ccy_quote", pa.float64()), # Nullable (OKX only)
    ("is_closed", pa.bool_()),       # Nullable
    ("source", pa.string()),
    ("ingest_time_ms", pa.int64()),
])
