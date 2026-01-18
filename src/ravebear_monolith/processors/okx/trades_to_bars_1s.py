"""OKX trades to 1-second bars stateful processor.

Aggregates trade events into OHLCV bars, flushing on bucket rollover.
Bars are deterministic regardless of input trade order.
"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path

from ravebear_monolith.core.processor import ProcessorBase, ProcessResult
from ravebear_monolith.storage.bar_sink import Bar1s, BarSink
from ravebear_monolith.storage.event_reader import EventRow
from ravebear_monolith.util.logging import log_event

logger = logging.getLogger(__name__)


@dataclass
class TradeRecord:
    """Single trade record for deterministic OHLC computation."""

    sort_key: tuple[int, str]  # (trade_ts_ms, event_id)
    price: float
    size: float


class BucketState:
    """In-memory state for a single 1-second bucket.

    Collects trades and computes deterministic OHLC based on sorted order.
    """

    def __init__(self, symbol: str, ts_ms: int) -> None:
        self.symbol = symbol
        self.ts_ms = ts_ms  # Bucket start (floored to 1s)
        self._trades: list[TradeRecord] = []

    def add_trade(self, trade: TradeRecord) -> None:
        """Add a trade to the bucket."""
        self._trades.append(trade)

    @property
    def trade_count(self) -> int:
        """Number of trades in bucket."""
        return len(self._trades)

    def to_bar(self) -> Bar1s:
        """Convert bucket state to Bar1s with deterministic OHLC.

        OPEN = price of trade with min(sort_key)
        CLOSE = price of trade with max(sort_key)
        HIGH = max(price)
        LOW = min(price)
        VOLUME = sum(size)
        """
        if not self._trades:
            raise ValueError("Cannot create bar from empty bucket")

        # Sort trades by (trade_ts_ms, event_id)
        sorted_trades = sorted(self._trades, key=lambda t: t.sort_key)

        open_price = sorted_trades[0].price
        close_price = sorted_trades[-1].price
        high_price = max(t.price for t in self._trades)
        low_price = min(t.price for t in self._trades)
        volume = sum(t.size for t in self._trades)

        return Bar1s(
            symbol=self.symbol,
            ts_ms=self.ts_ms,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=volume,
            trade_count=len(self._trades),
        )


class TradesToBars1sProcessor(ProcessorBase):
    """Stateful processor that aggregates trades into 1-second bars.

    Maintains in-memory bucket per symbol and flushes on rollover.
    Bars are deterministic regardless of trade arrival order.

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

            # Trade timestamp: use payload ts if present, else event.ts_ms
            trade_ts_ms = payload.get("trade_ts_ms")
            if trade_ts_ms is None:
                trade_ts_ms = payload.get("ts")
            if trade_ts_ms is not None:
                trade_ts_ms = int(trade_ts_ms)
            else:
                trade_ts_ms = event.ts_ms

        except (ValueError, TypeError) as e:
            return ProcessResult(ok=False, reason=f"Invalid payload fields: {e}")

        # Compute bucket timestamp (floor to 1s)
        bucket_ts_ms = (event.ts_ms // 1000) * 1000

        # Get or create bucket
        sink = await self._ensure_sink()
        current_bucket = self._buckets.get(symbol)

        if current_bucket is None:
            # First trade for this symbol
            current_bucket = BucketState(symbol, bucket_ts_ms)
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
            current_bucket = BucketState(symbol, bucket_ts_ms)
            self._buckets[symbol] = current_bucket

        # Create trade record with sort key for deterministic OHLC
        trade = TradeRecord(
            sort_key=(trade_ts_ms, event.id),
            price=price,
            size=size,
        )
        current_bucket.add_trade(trade)

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
