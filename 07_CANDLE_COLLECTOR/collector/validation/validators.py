"""
Candle validator with deduplication and ordering.
Reports metrics to health monitor.
"""
import logging
from collections import defaultdict
from typing import Optional, Callable

from ..utils.time import ALLOWED_TIMEFRAMES, timeframe_to_ms


class CandleValidator:
    """Validate and deduplicate candles."""
    
    def __init__(self, out_of_order_window: int, on_dup_callback: Optional[Callable] = None):
        self.out_of_order_window = int(out_of_order_window)
        self.last_open = {}
        self.buffers = defaultdict(list)
        self.logger = logging.getLogger(__name__)
        
        # Metrics
        self.dup_count = defaultdict(int)
        self.on_dup_callback = on_dup_callback  # Health monitor callback

    def _validate_candle(self, candle: dict) -> tuple:
        """Validate candle data integrity."""
        if candle.get("timeframe") not in ALLOWED_TIMEFRAMES:
            return False, "unsupported_timeframe"

        duration = timeframe_to_ms(candle["timeframe"])
        open_time = int(candle["open_time_ms"])
        close_time = int(candle["close_time_ms"])
        
        if open_time % duration != 0:
            return False, "open_time_not_aligned"

        if close_time not in (open_time + duration, open_time + duration - 1):
            return False, "duration_mismatch"

        open_px = float(candle["open"])
        close_px = float(candle["close"])
        high_px = float(candle["high"])
        low_px = float(candle["low"])

        if high_px < max(open_px, close_px):
            return False, "high_lt_open_close"
        if low_px > min(open_px, close_px):
            return False, "low_gt_open_close"
        if high_px < low_px:
            return False, "high_lt_low"

        volume = float(candle.get("volume", 0))
        if volume < 0:
            return False, "volume_negative"

        if candle.get("quote_volume") is not None and float(candle["quote_volume"]) < 0:
            return False, "quote_volume_negative"

        trades = candle.get("trades_count")
        if trades is not None and int(trades) < 0:
            return False, "trades_negative"

        return True, None

    def add(self, candle: dict) -> list:
        """Add candle to validator, return batches ready for storage."""
        venue = candle.get("venue")
        key = (venue, candle.get("symbol"), candle.get("timeframe"))
        
        ok, reason = self._validate_candle(candle)
        if not ok:
            self.logger.warning(
                "Validation failed: %s %s/%s/%s",
                reason, key[0], key[1], key[2]
            )
            return []

        open_time = int(candle["open_time_ms"])
        
        # Check for out-of-order
        last = self.last_open.get(key)
        if last is not None and open_time <= last:
            return []

        # Check for duplicate in buffer
        if any(existing["open_time_ms"] == open_time for existing in self.buffers[key]):
            self.dup_count[venue] += 1
            # Notify health monitor
            if self.on_dup_callback:
                self.on_dup_callback(venue, 1)
            return []

        self.buffers[key].append(candle)
        self.buffers[key].sort(key=lambda c: c["open_time_ms"])

        emitted = []
        while len(self.buffers[key]) > self.out_of_order_window:
            next_candle = self.buffers[key].pop(0)
            self.last_open[key] = int(next_candle["open_time_ms"])
            emitted.append(next_candle)
        
        return [emitted] if emitted else []

    def flush_all(self) -> list:
        """Flush all pending candles."""
        batches = []
        for key, buffer in list(self.buffers.items()):
            buffer.sort(key=lambda c: c["open_time_ms"])
            emitted = []
            for candle in buffer:
                open_time = int(candle["open_time_ms"])
                last = self.last_open.get(key)
                if last is not None and open_time <= last:
                    continue
                self.last_open[key] = open_time
                emitted.append(candle)
            if emitted:
                batches.append(emitted)
            self.buffers[key] = []
        return batches
    
    def get_queue_depth(self, venue: str = None) -> int:
        """Get total pending candles in buffer."""
        if venue:
            return sum(len(buf) for key, buf in self.buffers.items() if key[0] == venue)
        return sum(len(buf) for buf in self.buffers.values())
    
    def get_queue_depths_by_venue(self) -> dict:
        """Get queue depths per venue."""
        depths = defaultdict(int)
        for key, buf in self.buffers.items():
            depths[key[0]] += len(buf)
        return dict(depths)
    
    def get_dup_stats(self) -> dict:
        """Get duplicate counts per venue."""
        return dict(self.dup_count)
