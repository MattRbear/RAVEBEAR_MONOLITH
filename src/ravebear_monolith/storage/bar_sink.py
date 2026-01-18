"""Bar sink for storing OHLCV bars in SQLite.

Uses aiosqlite with WAL mode and upsert semantics.
"""

import logging
from pathlib import Path

import aiosqlite
from pydantic import BaseModel

from ravebear_monolith.util.logging import log_event

logger = logging.getLogger(__name__)


class Bar1s(BaseModel, extra="forbid"):
    """1-second OHLCV bar."""

    symbol: str
    ts_ms: int  # Bucket start in ms (floored to 1s)
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int


class BarSink:
    """SQLite-backed sink for 1-second OHLCV bars.

    Uses WAL mode and upsert semantics for idempotent writes.

    Args:
        db_path: Path to SQLite database file.
    """

    def __init__(self, db_path: Path | str) -> None:
        self._db_path = Path(db_path)
        self._conn: aiosqlite.Connection | None = None

    async def open(self) -> None:
        """Open database connection and initialize schema."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        self._conn = await aiosqlite.connect(str(self._db_path))
        self._conn.row_factory = aiosqlite.Row

        try:
            # Enable WAL mode
            await self._conn.execute("PRAGMA journal_mode=WAL")
            await self._conn.execute("PRAGMA synchronous=NORMAL")

            # Create bars_1s table
            await self._conn.execute("""
                CREATE TABLE IF NOT EXISTS bars_1s (
                    symbol TEXT NOT NULL,
                    ts_ms INTEGER NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    trade_count INTEGER NOT NULL,
                    PRIMARY KEY (symbol, ts_ms)
                )
            """)

            # Index for time-based queries
            await self._conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_bars_1s_ts ON bars_1s (ts_ms)
            """)

            await self._conn.commit()

            log_event(
                logger,
                logging.INFO,
                f"Bar sink opened: {self._db_path}",
                event="bar_sink_opened",
                db_path=str(self._db_path),
            )
        except Exception:
            await self.close()
            raise

    async def close(self) -> None:
        """Close database connection."""
        if self._conn:
            await self._conn.close()
            self._conn = None
            log_event(logger, logging.INFO, "Bar sink closed", event="bar_sink_closed")

    async def __aenter__(self) -> "BarSink":
        """Async context manager entry."""
        await self.open()
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Async context manager exit."""
        await self.close()

    async def upsert_bar(self, bar: Bar1s) -> None:
        """Insert or update bar with merge semantics.

        - open: kept as first value (not overwritten)
        - high: max(existing, new)
        - low: min(existing, new)
        - close: updated to new value
        - volume: sum of existing + new
        - trade_count: sum of existing + new

        Args:
            bar: Bar1s to upsert.

        Raises:
            RuntimeError: If sink is not open.
        """
        if not self._conn:
            raise RuntimeError("Bar sink not open")

        await self._conn.execute(
            """
            INSERT INTO bars_1s (symbol, ts_ms, open, high, low, close, volume, trade_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, ts_ms) DO UPDATE SET
                high = MAX(high, excluded.high),
                low = MIN(low, excluded.low),
                close = excluded.close,
                volume = volume + excluded.volume,
                trade_count = trade_count + excluded.trade_count
            """,
            (
                bar.symbol,
                bar.ts_ms,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                bar.trade_count,
            ),
        )
        await self._conn.commit()

    async def get_bar(self, symbol: str, ts_ms: int) -> Bar1s | None:
        """Get bar by symbol and timestamp.

        Args:
            symbol: Trading symbol.
            ts_ms: Bucket start timestamp in ms.

        Returns:
            Bar1s if found, None otherwise.
        """
        if not self._conn:
            raise RuntimeError("Bar sink not open")

        async with self._conn.execute(
            "SELECT * FROM bars_1s WHERE symbol = ? AND ts_ms = ?",
            (symbol, ts_ms),
        ) as cursor:
            row = await cursor.fetchone()

        if row is None:
            return None

        return Bar1s(
            symbol=row["symbol"],
            ts_ms=row["ts_ms"],
            open=row["open"],
            high=row["high"],
            low=row["low"],
            close=row["close"],
            volume=row["volume"],
            trade_count=row["trade_count"],
        )

    async def count_bars(self, symbol: str | None = None) -> int:
        """Count bars, optionally filtered by symbol."""
        if not self._conn:
            raise RuntimeError("Bar sink not open")

        if symbol:
            async with self._conn.execute(
                "SELECT COUNT(*) FROM bars_1s WHERE symbol = ?", (symbol,)
            ) as cursor:
                row = await cursor.fetchone()
        else:
            async with self._conn.execute("SELECT COUNT(*) FROM bars_1s") as cursor:
                row = await cursor.fetchone()

        return row[0] if row else 0

    async def get_all_bars(self, symbol: str | None = None) -> list[Bar1s]:
        """Get all bars, optionally filtered by symbol."""
        if not self._conn:
            raise RuntimeError("Bar sink not open")

        if symbol:
            async with self._conn.execute(
                "SELECT * FROM bars_1s WHERE symbol = ? ORDER BY ts_ms",
                (symbol,),
            ) as cursor:
                rows = await cursor.fetchall()
        else:
            async with self._conn.execute("SELECT * FROM bars_1s ORDER BY symbol, ts_ms") as cursor:
                rows = await cursor.fetchall()

        return [
            Bar1s(
                symbol=row["symbol"],
                ts_ms=row["ts_ms"],
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=row["volume"],
                trade_count=row["trade_count"],
            )
            for row in rows
        ]
