"""Event reader for read-only queries against stored events.

Uses aiosqlite in read-only mode with query-only pragma.
"""

import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Annotated, Literal

import aiosqlite
from pydantic import BaseModel, Field


class QuerySpec(BaseModel, extra="forbid"):
    """Specification for event queries.

    All filters are optional. Combine for AND logic.
    """

    source: str | None = None
    event_type: str | None = None
    ts_min: int | None = None  # Unix milliseconds
    ts_max: int | None = None  # Unix milliseconds
    limit: Annotated[int, Field(ge=1, le=50_000)] = 1000
    order: Literal["asc", "desc"] = "asc"


class EventRow(BaseModel, extra="forbid"):
    """Row representation of stored event."""

    id: str
    source: str
    event_type: str
    ts_ms: int  # Mapped from 'ts' column
    payload_json: str
    content_hash: str


def payload_as_dict(row: EventRow) -> dict:
    """Decode event payload to dictionary.

    Args:
        row: EventRow with payload_json field.

    Returns:
        Decoded payload dictionary.

    Raises:
        ValueError: On invalid JSON.
    """
    try:
        return json.loads(row.payload_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in payload: {e}") from e


class EventReader:
    """Read-only event reader for SQLite database.

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

        # Set read-only pragmas
        await self._conn.execute("PRAGMA query_only=ON")
        await self._conn.execute("PRAGMA busy_timeout=5000")

    async def close(self) -> None:
        """Close database connection."""
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def query(self, spec: QuerySpec) -> list[EventRow]:
        """Execute query and return all matching rows.

        Args:
            spec: Query specification with filters.

        Returns:
            List of matching EventRow objects.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._conn:
            raise RuntimeError("EventReader not connected")

        sql, params = self._build_query(spec)
        cursor = await self._conn.execute(sql, params)
        rows = await cursor.fetchall()

        return [self._row_to_event(row) for row in rows]

    async def iter_query(
        self,
        spec: QuerySpec,
        *,
        chunk_size: Annotated[int, Field(ge=1, le=10_000)] = 1000,
    ) -> AsyncIterator[EventRow]:
        """Stream query results in chunks.

        Does not load all rows into memory.

        Args:
            spec: Query specification with filters.
            chunk_size: Number of rows to fetch per batch.

        Yields:
            EventRow for each matching row.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._conn:
            raise RuntimeError("EventReader not connected")

        sql, params = self._build_query(spec)
        cursor = await self._conn.execute(sql, params)

        while True:
            rows = await cursor.fetchmany(chunk_size)
            if not rows:
                break
            for row in rows:
                yield self._row_to_event(row)

    def _build_query(self, spec: QuerySpec) -> tuple[str, list]:
        """Build parameterized SQL query from spec.

        Args:
            spec: Query specification.

        Returns:
            Tuple of (SQL string, parameter list).
        """
        conditions = []
        params: list = []

        if spec.source is not None:
            conditions.append("source = ?")
            params.append(spec.source)

        if spec.event_type is not None:
            conditions.append("type = ?")
            params.append(spec.event_type)

        if spec.ts_min is not None:
            conditions.append("ts >= ?")
            params.append(spec.ts_min)

        if spec.ts_max is not None:
            conditions.append("ts <= ?")
            params.append(spec.ts_max)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        order_dir = "ASC" if spec.order == "asc" else "DESC"

        sql = f"""
            SELECT id, ts, source, type, payload_json, content_hash
            FROM events
            WHERE {where_clause}
            ORDER BY ts {order_dir}
            LIMIT ?
        """
        params.append(spec.limit)

        return sql, params

    @staticmethod
    def _row_to_event(row: aiosqlite.Row) -> EventRow:
        """Convert database row to EventRow.

        Maps 'ts' column to 'ts_ms' and 'type' to 'event_type'.
        """
        return EventRow(
            id=row["id"],
            source=row["source"],
            event_type=row["type"],  # Map 'type' column to 'event_type' field
            ts_ms=row["ts"],  # Map 'ts' column to 'ts_ms' field
            payload_json=row["payload_json"],
            content_hash=row["content_hash"],
        )
