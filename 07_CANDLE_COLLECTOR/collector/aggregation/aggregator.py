"""
Timeframe aggregator - builds higher timeframes from 1m candles.
"""
from collections import defaultdict

from ..utils.time import timeframe_to_ms, floor_time


class TimeframeAggregator:
    """Aggregate 1m candles into higher timeframes."""
    
    def __init__(self, base_timeframe: str, target_timeframes: list):
        self.base_timeframe = base_timeframe
        self.target_timeframes = list(target_timeframes)
        self.state = defaultdict(dict)

    def update(self, candle: dict) -> list:
        """
        Update aggregator with a new candle.
        Returns list of completed higher-timeframe candles.
        """
        if candle.get("timeframe") != self.base_timeframe:
            return []
        if candle.get("is_closed") is False:
            return []

        outputs = []
        for timeframe in self.target_timeframes:
            duration = timeframe_to_ms(timeframe)
            bucket_start = floor_time(candle["open_time_ms"], duration)
            bucket_end = bucket_start + duration
            key = (candle.get("symbol"), timeframe)

            bucket = self.state.get(key)
            if not bucket or bucket["open_time_ms"] != bucket_start:
                # Start new bucket
                bucket = {
                    "venue": candle.get("venue"),  # Use "venue" not "exchange"
                    "symbol": candle.get("symbol"),
                    "timeframe": timeframe,
                    "open_time_ms": bucket_start,
                    "close_time_ms": bucket_end - 1,
                    "open": candle["open"],
                    "high": candle["high"],
                    "low": candle["low"],
                    "close": candle["close"],
                    "volume": candle.get("volume", 0),
                    "quote_volume": candle.get("quote_volume"),
                    "vwap": None,
                    "trades_count": candle.get("trades_count"),
                    "is_closed": False,
                    "source": "aggregated",
                }
                self.state[key] = bucket
            else:
                # Update existing bucket
                bucket["high"] = max(bucket["high"], candle["high"])
                bucket["low"] = min(bucket["low"], candle["low"])
                bucket["close"] = candle["close"]
                bucket["volume"] = bucket.get("volume", 0) + candle.get("volume", 0)
                
                if candle.get("quote_volume") is not None:
                    bucket["quote_volume"] = (bucket.get("quote_volume") or 0) + candle["quote_volume"]
                if candle.get("trades_count") is not None:
                    bucket["trades_count"] = (bucket.get("trades_count") or 0) + candle["trades_count"]

            # Check if bucket is complete
            if candle["close_time_ms"] >= bucket_end - 60001:  # Allow 1 minute tolerance
                bucket["is_closed"] = True
                outputs.append(bucket.copy())
                self.state.pop(key, None)

        return outputs
    
    def flush(self) -> list:
        """Flush all incomplete buckets."""
        outputs = []
        for key, bucket in list(self.state.items()):
            bucket["is_closed"] = True
            outputs.append(bucket)
        self.state.clear()
        return outputs
