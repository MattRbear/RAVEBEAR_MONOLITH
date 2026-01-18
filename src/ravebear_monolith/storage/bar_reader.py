"""Bar reader for read-only queries against stored 1s bars.

Uses aiosqlite in read-only mode with query-only pragma.
"""

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Annotated, Literal

import aiosqlite
from pydantic import BaseModel, Field


class BarQuerySpec(BaseModel, extra="forbid"):
    """Specification for bar queries.

    All filters are optional. Combine for AND logic.
    """

    symbol: str | None = None
    ts_min: int | None = None  # Unix milliseconds, inclusive
    ts_max: int | None = None  # Unix milliseconds, inclusive
    limit: Annotated[int, Field(ge=1, le=50_000)] = 1000
    order: Literal["asc", "desc"] = "asc"


class BarRow(BaseModel, extra="forbid"):
    """Row representation of a stored 1s bar."""

    symbol: str
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int


class BarReader:
    """Read-only bar reader for SQLite database.

    Opens connection in query-only mode to prevent mutations.

    Args:
        db_path: Path to SQLite database file.
    """

    def __init__(self, db_path: Path | str) -> None:
        self._db_path = Path(db_path)
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        """Open database connection in read-only mode."""
        self._conn = await aiosqlite.connect(str(self._db_path))
        self._conn.row_factory = aiosqlite.Row

        try:
            # Set read-only pragmas
            await self._conn.execute("PRAGMA journal_mode=WAL")
            await self._conn.execute("PRAGMA query_only=ON")
            await self._conn.execute("PRAGMA busy_timeout=5000")
        except Exception:
            await self.close()
            raise

    async def close(self) -> None:
        """Close database connection."""
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def __aenter__(self) -> "BarReader":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Async context manager exit."""
        await self.close()

    async def query(self, spec: BarQuerySpec) -> list[BarRow]:
        """Execute query and return all matching rows.

        Args:
            spec: Query specification with filters.

        Returns:
            List of matching BarRow objects.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._conn:
            raise RuntimeError("BarReader not connected")

        sql, params = self._build_query(spec)
        async with self._conn.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
            return [self._row_to_bar(row) for row in rows]

    async def iter_query(
        self,
        spec: BarQuerySpec,
        *,
        chunk_size: Annotated[int, Field(ge=1, le=10_000)] = 1000,
    ) -> AsyncIterator[BarRow]:
        """Stream query results in chunks.

        Does not load all rows into memory.

        Args:
            spec: Query specification with filters.
            chunk_size: Number of rows to fetch per batch.

        Yields:
            BarRow for each matching row.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._conn:
            raise RuntimeError("BarReader not connected")

        sql, params = self._build_query(spec)
        async with self._conn.execute(sql, params) as cursor:
            while True:
                rows = await cursor.fetchmany(chunk_size)
                if not rows:
                    break
                for row in rows:
                    yield self._row_to_bar(row)

    def _build_query(self, spec: BarQuerySpec) -> tuple[str, list]:
        """Build parameterized SQL query from spec.

        Args:
            spec: Query specification.

        Returns:
            Tuple of (SQL string, parameter list).
        """
        conditions = []
        params: list = []

        if spec.symbol is not None:
            conditions.append("symbol = ?")
            params.append(spec.symbol)

        if spec.ts_min is not None:
            conditions.append("ts_ms >= ?")
            params.append(spec.ts_min)

        if spec.ts_max is not None:
            conditions.append("ts_ms <= ?")
            params.append(spec.ts_max)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        order_dir = "ASC" if spec.order == "asc" else "DESC"

        # Secondary ORDER BY symbol for determinism
        sql = f"""
            SELECT symbol, ts_ms, open, high, low, close, volume, trade_count
            FROM bars_1s
            WHERE {where_clause}
            ORDER BY ts_ms {order_dir}, symbol {order_dir}
            LIMIT ?
        """
        params.append(spec.limit)

        return sql, params

    @staticmethod
    def _row_to_bar(row: aiosqlite.Row) -> BarRow:
        """Convert database row to BarRow."""
        return BarRow(
            symbol=row["symbol"],
            ts_ms=row["ts_ms"],
            open=row["open"],
            high=row["high"],
            low=row["low"],
            close=row["close"],
            volume=row["volume"],
            trade_count=row["trade_count"],
        )
