"""Event sink for persisting collector events to SQLite.

Uses aiosqlite with WAL mode for async access and hash-based deduplication.
"""

import hashlib
import json
import logging
from pathlib import Path
from typing import Any

import aiosqlite

from ravebear_monolith.collectors.base import CollectorEvent
from ravebear_monolith.util.logging import log_event

logger = logging.getLogger(__name__)


class EventSink:
    """SQLite-backed event sink with WAL mode and deduplication.

    Uses id = sha256(source:ts:content_hash) for idempotent writes.
    INSERT OR IGNORE ensures safe deduplication.

    Args:
        db_path: Path to SQLite database file.
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._conn: aiosqlite.Connection | None = None

    async def open(self) -> None:
        """Open database connection and initialize schema."""
        # Ensure parent directory exists
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        self._conn = await aiosqlite.connect(str(self._db_path))
        self._conn.row_factory = aiosqlite.Row

        try:
            # Enable WAL mode for better concurrency
            await self._conn.execute("PRAGMA journal_mode=WAL")
            await self._conn.execute("PRAGMA synchronous=NORMAL")

            # Create events table (append-only)
            await self._conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    ts INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL
                )
            """)

            # Create replay_cursors table for restart-safe processing
            await self._conn.execute("""
                CREATE TABLE IF NOT EXISTS replay_cursors (
                    name TEXT PRIMARY KEY,
                    last_ts_ms INTEGER NOT NULL,
                    last_event_id TEXT NOT NULL,
                    updated_ts_ms INTEGER NOT NULL
                )
            """)

            # Create indexes for common queries
            await self._conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_source_type
                ON events (source, type)
            """)
            await self._conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_ts ON events (ts)
            """)

            await self._conn.commit()

            log_event(
                logger,
                logging.INFO,
                f"Event sink opened: {self._db_path}",
                event="event_sink_opened",
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
            log_event(
                logger,
                logging.INFO,
                "Event sink closed",
                event="event_sink_closed",
            )

    async def __aenter__(self) -> "EventSink":
        """Async context manager entry."""
        await self.open()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()

    async def write(self, event: CollectorEvent) -> None:
        """Write event to database, ignoring duplicates.

        Args:
            event: CollectorEvent to persist.

        Raises:
            RuntimeError: If sink is not open.
            aiosqlite.Error: On database errors (fatal).
        """
        if not self._conn:
            raise RuntimeError("Event sink not open")

        # Parse timestamp to integer (Unix ms or extract from ISO)
        ts = self._parse_timestamp(event.ts_utc)

        # Compute content hash and event ID
        payload_json = json.dumps(event.payload, sort_keys=True)
        content_hash = self._compute_content_hash(payload_json)
        event_id = self._compute_event_id(event.source, ts, content_hash)

        # INSERT OR IGNORE for safe deduplication
        await self._conn.execute(
            """
            INSERT OR IGNORE INTO events (id, ts, source, type, payload_json, content_hash)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (event_id, ts, event.source, event.event_type, payload_json, content_hash),
        )
        await self._conn.commit()

    async def read_all(self) -> list[dict[str, Any]]:
        """Read all events from database.

        Returns:
            List of event dictionaries.
        """
        if not self._conn:
            raise RuntimeError("Event sink not open")

        async with self._conn.execute(
            "SELECT id, ts, source, type, payload_json FROM events ORDER BY ts"
        ) as cursor:
            rows = await cursor.fetchall()

        return [
            {
                "id": row["id"],
                "ts": row["ts"],
                "source": row["source"],
                "type": row["type"],
                "payload": json.loads(row["payload_json"]),
            }
            for row in rows
        ]

    async def count(self) -> int:
        """Count total events in database."""
        if not self._conn:
            raise RuntimeError("Event sink not open")

        async with self._conn.execute("SELECT COUNT(*) FROM events") as cursor:
            row = await cursor.fetchone()
        return row[0] if row else 0

    @staticmethod
    def _compute_content_hash(payload_json: str) -> str:
        """Compute hash of payload content."""
        return hashlib.sha256(payload_json.encode()).hexdigest()

    @staticmethod
    def _compute_event_id(source: str, ts: int, content_hash: str) -> str:
        """Compute unique event ID for deduplication.

        id = sha256(f"{source}:{ts}:{content_hash}")
        """
        content = f"{source}:{ts}:{content_hash}"
        return hashlib.sha256(content.encode()).hexdigest()

    @staticmethod
    def _parse_timestamp(ts_utc: str) -> int:
        """Parse ISO8601 timestamp to Unix milliseconds."""
        from datetime import datetime

        # Handle various ISO formats
        ts_clean = ts_utc.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(ts_clean)
            return int(dt.timestamp() * 1000)
        except ValueError:
            # Fallback: return current time
            return int(datetime.now().timestamp() * 1000)
