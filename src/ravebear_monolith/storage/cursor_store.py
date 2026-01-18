"""Cursor store for replay cursor persistence.

Manages replay cursors that allow processors to resume from where they left off.
"""

from datetime import datetime, timezone
from pathlib import Path

import aiosqlite
from pydantic import BaseModel


class ReplayCursor(BaseModel, extra="forbid"):
    """Replay cursor representing last processed event position."""

    name: str
    last_ts_ms: int
    last_event_id: str
    updated_ts_ms: int


class CursorStore:
    """Store for managing replay cursors in SQLite.

    Provides atomic get/upsert operations for cursor persistence.

    Args:
        db_path: Path to SQLite database file.
    """

    def __init__(self, db_path: Path | str) -> None:
        self._db_path = Path(db_path)
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        """Open database connection."""
        self._conn = await aiosqlite.connect(str(self._db_path))
        self._conn.row_factory = aiosqlite.Row

        try:
            # Ensure table exists (same schema as EventSink creates)
            await self._conn.execute("""
                CREATE TABLE IF NOT EXISTS replay_cursors (
                    name TEXT PRIMARY KEY,
                    last_ts_ms INTEGER NOT NULL,
                    last_event_id TEXT NOT NULL,
                    updated_ts_ms INTEGER NOT NULL
                )
            """)
            await self._conn.commit()
        except Exception:
            await self.close()
            raise

    async def close(self) -> None:
        """Close database connection."""
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def __aenter__(self) -> "CursorStore":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Async context manager exit."""
        await self.close()

    async def get(self, name: str) -> ReplayCursor | None:
        """Get cursor by name.

        Args:
            name: Cursor name.

        Returns:
            ReplayCursor if found, None otherwise.

        Raises:
            RuntimeError: If not connected.
            ValueError: If name is empty or whitespace.
        """
        if not self._conn:
            raise RuntimeError("CursorStore not connected")

        self._validate_name(name)

        async with self._conn.execute(
            """
            SELECT name, last_ts_ms, last_event_id, updated_ts_ms
            FROM replay_cursors WHERE name = ?
            """,
            (name,),
        ) as cursor:
            row = await cursor.fetchone()

        if row is None:
            return None

        return ReplayCursor(
            name=row["name"],
            last_ts_ms=row["last_ts_ms"],
            last_event_id=row["last_event_id"],
            updated_ts_ms=row["updated_ts_ms"],
        )

    async def upsert(self, name: str, last_ts_ms: int, last_event_id: str) -> None:
        """Insert or update cursor atomically.

        Args:
            name: Cursor name.
            last_ts_ms: Timestamp of last processed event (unix ms).
            last_event_id: ID of last processed event.

        Raises:
            RuntimeError: If not connected.
            ValueError: If name is empty/whitespace or last_ts_ms < 0.
        """
        if not self._conn:
            raise RuntimeError("CursorStore not connected")

        self._validate_name(name)

        if last_ts_ms < 0:
            raise ValueError("last_ts_ms must be >= 0")

        updated_ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        await self._conn.execute(
            """
            INSERT INTO replay_cursors (name, last_ts_ms, last_event_id, updated_ts_ms)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                last_ts_ms = excluded.last_ts_ms,
                last_event_id = excluded.last_event_id,
                updated_ts_ms = excluded.updated_ts_ms
            """,
            (name, last_ts_ms, last_event_id, updated_ts_ms),
        )
        await self._conn.commit()

    @staticmethod
    def _validate_name(name: str) -> None:
        """Validate cursor name is non-empty."""
        if not name or not name.strip():
            raise ValueError("Cursor name cannot be empty or whitespace")
