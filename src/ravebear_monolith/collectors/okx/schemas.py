"""OKX trade schema with locked structure.

This schema is locked early to ensure consistent data flow.
"""

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, field_validator


class OKXTrade(BaseModel):
    """OKX trade event with locked schema.

    Attributes:
        inst_id: Instrument ID (e.g., "BTC-USDT").
        trade_id: Unique trade identifier.
        price: Trade price.
        size: Trade size/quantity.
        side: Trade side (buy or sell).
        ts_utc: ISO8601 UTC timestamp.
    """

    inst_id: str
    trade_id: str
    price: float
    size: float
    side: Literal["buy", "sell"]
    ts_utc: str

    @field_validator("price", "size", mode="before")
    @classmethod
    def coerce_to_float(cls, v: Any) -> float:
        """Coerce string numbers to float."""
        if isinstance(v, str):
            return float(v)
        return v

    @classmethod
    def from_raw(cls, raw: dict[str, Any]) -> "OKXTrade":
        """Create OKXTrade from raw API response.

        Handles OKX-specific field names and timestamp conversion.

        Args:
            raw: Raw trade data from OKX API/fixture.

        Returns:
            Validated OKXTrade instance.
        """
        # OKX uses "instId", "tradeId", "px", "sz", "side", "ts"
        inst_id = raw.get("instId", raw.get("inst_id", ""))
        trade_id = str(raw.get("tradeId", raw.get("trade_id", "")))
        price = raw.get("px", raw.get("price", 0))
        size = raw.get("sz", raw.get("size", 0))
        side = raw.get("side", "buy").lower()

        # Normalize timestamp to ISO8601 UTC
        ts_raw = raw.get("ts", raw.get("ts_utc", ""))
        ts_utc = cls._normalize_timestamp(ts_raw)

        return cls(
            inst_id=inst_id,
            trade_id=trade_id,
            price=float(price),
            size=float(size),
            side=side,  # type: ignore[arg-type]
            ts_utc=ts_utc,
        )

    @staticmethod
    def _normalize_timestamp(ts_raw: str | int) -> str:
        """Convert timestamp to ISO8601 UTC string.

        Args:
            ts_raw: Unix milliseconds (int/str) or ISO string.

        Returns:
            ISO8601 UTC timestamp string.
        """
        if isinstance(ts_raw, int) or (isinstance(ts_raw, str) and ts_raw.isdigit()):
            # Unix milliseconds
            ms = int(ts_raw)
            dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
            return dt.isoformat()
        elif isinstance(ts_raw, str) and ts_raw:
            # Already ISO format, ensure UTC indicator
            if not ts_raw.endswith("Z") and "+" not in ts_raw:
                return ts_raw + "+00:00"
            return ts_raw.replace("Z", "+00:00")
        else:
            # Fallback to current time
            return datetime.now(timezone.utc).isoformat()
