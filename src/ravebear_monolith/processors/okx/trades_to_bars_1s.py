"""OKX trades to 1-second bars stateful processor.

Aggregates trade events into OHLCV bars, flushing on bucket rollover.
"""

import json
import logging
from pathlib import Path

from ravebear_monolith.core.processor import ProcessorBase, ProcessResult
from ravebear_monolith.storage.bar_sink import Bar1s, BarSink
from ravebear_monolith.storage.event_reader import EventRow
from ravebear_monolith.util.logging import log_event

logger = logging.getLogger(__name__)


class BucketState:
    """In-memory state for a single 1-second bucket."""

    def __init__(self, symbol: str, ts_ms: int, first_price: float) -> None:
        self.symbol = symbol
        self.ts_ms = ts_ms  # Bucket start (floored to 1s)
        self.open = first_price
        self.high = first_price
        self.low = first_price
        self.close = first_price
        self.volume = 0.0
        self.trade_count = 0

    def add_trade(self, price: float, size: float) -> None:
        """Add a trade to the bucket."""
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += size
        self.trade_count += 1

    def to_bar(self) -> Bar1s:
        """Convert bucket state to Bar1s."""
        return Bar1s(
            symbol=self.symbol,
            ts_ms=self.ts_ms,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            trade_count=self.trade_count,
        )


class TradesToBars1sProcessor(ProcessorBase):
    """Stateful processor that aggregates trades into 1-second bars.

    Maintains in-memory bucket per symbol and flushes on rollover.

    Args:
        db_path: Path to SQLite database for BarSink.
        symbol_default: Default symbol if not in payload.
    """

    def __init__(
        self,
        db_path: Path | str,
        *,
        symbol_default: str | None = None,
    ) -> None:
        self._db_path = Path(db_path)
        self._symbol_default = symbol_default
        self._bar_sink: BarSink | None = None
        self._buckets: dict[str, BucketState] = {}  # symbol -> current bucket

    async def _ensure_sink(self) -> BarSink:
        """Lazily open bar sink."""
        if self._bar_sink is None:
            self._bar_sink = BarSink(self._db_path)
            await self._bar_sink.open()
        return self._bar_sink

    async def process(self, event: EventRow) -> ProcessResult:
        """Process a trade event, aggregating into 1s bars.

        Args:
            event: EventRow with OKXTrade-compatible payload.

        Returns:
            ProcessResult with ok=True on success, ok=False on parse error.
        """
        # Parse payload
        try:
            payload = json.loads(event.payload_json)
        except json.JSONDecodeError as e:
            return ProcessResult(ok=False, reason=f"Invalid JSON: {e}")

        # Extract required fields
        try:
            # Support both OKX raw format (instId, px, sz) and normalized
            symbol = payload.get("inst_id") or payload.get("instId")
            if not symbol:
                symbol = self._symbol_default
            if not symbol:
                return ProcessResult(ok=False, reason="Missing symbol in payload")

            # Price: px or price
            price_raw = payload.get("price") or payload.get("px")
            if price_raw is None:
                return ProcessResult(ok=False, reason="Missing price in payload")
            price = float(price_raw)

            # Size: sz or size
            size_raw = payload.get("size") or payload.get("sz")
            if size_raw is None:
                return ProcessResult(ok=False, reason="Missing size in payload")
            size = float(size_raw)

        except (ValueError, TypeError) as e:
            return ProcessResult(ok=False, reason=f"Invalid payload fields: {e}")

        # Compute bucket timestamp (floor to 1s)
        bucket_ts_ms = (event.ts_ms // 1000) * 1000

        # Get or create bucket
        sink = await self._ensure_sink()
        current_bucket = self._buckets.get(symbol)

        if current_bucket is None:
            # First trade for this symbol
            current_bucket = BucketState(symbol, bucket_ts_ms, price)
            self._buckets[symbol] = current_bucket
        elif current_bucket.ts_ms != bucket_ts_ms:
            # Bucket rollover - flush previous
            await sink.upsert_bar(current_bucket.to_bar())
            log_event(
                logger,
                logging.DEBUG,
                f"Flushed bar: {symbol} @ {current_bucket.ts_ms}",
                event="bar_flushed",
                symbol=symbol,
                ts_ms=current_bucket.ts_ms,
            )
            # Start new bucket
            current_bucket = BucketState(symbol, bucket_ts_ms, price)
            self._buckets[symbol] = current_bucket

        # Add trade to current bucket
        current_bucket.add_trade(price, size)

        return ProcessResult(ok=True)

    async def finalize(self) -> None:
        """Flush all remaining buckets and close sink.

        Must be called after replay completes.
        """
        if self._bar_sink:
            for symbol, bucket in self._buckets.items():
                if bucket.trade_count > 0:
                    await self._bar_sink.upsert_bar(bucket.to_bar())
                    log_event(
                        logger,
                        logging.DEBUG,
                        f"Finalized bar: {symbol} @ {bucket.ts_ms}",
                        event="bar_finalized",
                        symbol=symbol,
                        ts_ms=bucket.ts_ms,
                    )
            await self._bar_sink.close()
            self._bar_sink = None
        self._buckets.clear()
