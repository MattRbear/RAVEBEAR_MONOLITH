"""Base collector contract for data collection.

Provides:
- CollectorEvent: Standard event structure
- CollectorBase: Abstract base class for all collectors
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel


class CollectorEvent(BaseModel):
    """Standard event structure from collectors.

    Attributes:
        source: Collector name/identifier.
        event_type: Type of event (e.g., "trade", "orderbook", "ticker").
        ts_utc: ISO8601 UTC timestamp.
        payload: Event-specific data.
    """

    source: str
    event_type: str
    ts_utc: str
    payload: dict[str, Any]

    @classmethod
    def create(cls, source: str, event_type: str, payload: dict[str, Any]) -> "CollectorEvent":
        """Create event with current UTC timestamp."""
        return cls(
            source=source,
            event_type=event_type,
            ts_utc=datetime.now(timezone.utc).isoformat(),
            payload=payload,
        )


class CollectorBase(ABC):
    """Abstract base class for data collectors.

    Collectors must implement start/stop/next_event methods.
    All methods must be cancellable and fail-closed.
    """

    def __init__(self, name: str) -> None:
        """Initialize collector.

        Args:
            name: Unique identifier for this collector.
        """
        self._name = name
        self._running = False

    @property
    def name(self) -> str:
        """Collector identifier."""
        return self._name

    @property
    def is_running(self) -> bool:
        """Whether collector is currently running."""
        return self._running

    @abstractmethod
    async def start(self) -> None:
        """Start the collector.

        Must be idempotent and cancellable.
        Raises on failure (fail-closed).
        """
        ...

    @abstractmethod
    async def stop(self) -> None:
        """Stop the collector gracefully.

        Must be idempotent and cancellable.
        Should not raise on already-stopped collector.
        """
        ...

    @abstractmethod
    async def next_event(self) -> CollectorEvent | None:
        """Get the next event from this collector.

        Returns:
            CollectorEvent if available, None if no event ready.

        Must be cancellable.
        May raise on transient errors (retry-eligible).
        """
        ...
