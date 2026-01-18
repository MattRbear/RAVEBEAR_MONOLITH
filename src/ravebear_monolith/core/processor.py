"""Processor contract for event processing pipeline.

Defines the interface that all event processors must implement.
"""

from abc import ABC, abstractmethod

from pydantic import BaseModel

from ravebear_monolith.storage.event_reader import EventRow


class ProcessResult(BaseModel, extra="forbid"):
    """Result of processing an event.

    Attributes:
        ok: Whether processing succeeded.
        reason: Explanation for failures or notes.
    """

    ok: bool
    reason: str | None = None


class ProcessorBase(ABC):
    """Abstract base class for event processors.

    All processors must implement the process method.
    """

    @abstractmethod
    async def process(self, event: EventRow) -> ProcessResult:
        """Process a single event.

        Args:
            event: EventRow to process.

        Returns:
            ProcessResult indicating success or failure.
        """
        ...


class NoopProcessor(ProcessorBase):
    """No-op processor that always succeeds.

    Useful for testing and as a placeholder.
    """

    async def process(self, event: EventRow) -> ProcessResult:
        """Process event by doing nothing.

        Args:
            event: EventRow (ignored).

        Returns:
            ProcessResult with ok=True.
        """
        return ProcessResult(ok=True)
