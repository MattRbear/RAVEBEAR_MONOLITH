"""Tests for ProcessorRouter with failure policies."""


import pytest

from ravebear_monolith.core.processor import ProcessorBase, ProcessResult
from ravebear_monolith.core.processor_router import (
    FailurePolicy,
    ProcessorRouter,
)
from ravebear_monolith.storage.event_reader import EventRow


def make_event_row(event_id: str = "test123", ts_ms: int = 1705536000000) -> EventRow:
    """Create a test EventRow."""
    return EventRow(
        id=event_id,
        source="test",
        event_type="trade",
        ts_ms=ts_ms,
        payload_json='{"price": 42000}',
        content_hash="abc123",
    )


class OkProcessor(ProcessorBase):
    """Processor that always succeeds."""

    async def process(self, event: EventRow) -> ProcessResult:
        return ProcessResult(ok=True)


class FailProcessor(ProcessorBase):
    """Processor that always fails."""

    async def process(self, event: EventRow) -> ProcessResult:
        return ProcessResult(ok=False, reason="Always fails")


class ExceptionProcessor(ProcessorBase):
    """Processor that always raises."""

    async def process(self, event: EventRow) -> ProcessResult:
        raise RuntimeError("Always raises")


class TrackingProcessor(ProcessorBase):
    """Processor that tracks calls."""

    def __init__(self) -> None:
        self.call_count = 0

    async def process(self, event: EventRow) -> ProcessResult:
        self.call_count += 1
        return ProcessResult(ok=True)


class TestProcessorRouter:
    """Tests for ProcessorRouter."""

    @pytest.mark.asyncio
    async def test_deterministic_order_sorted_by_name(self) -> None:
        """Processors are called in sorted name order."""
        call_order: list[str] = []

        class OrderTracker(ProcessorBase):
            def __init__(self, name: str) -> None:
                self._name = name

            async def process(self, event: EventRow) -> ProcessResult:
                call_order.append(self._name)
                return ProcessResult(ok=True)

        router = ProcessorRouter(
            {
                "zebra": OrderTracker("zebra"),
                "apple": OrderTracker("apple"),
                "mango": OrderTracker("mango"),
            },
            policy=FailurePolicy.FAIL_CLOSED,
        )

        event = make_event_row()
        await router.process(event)

        assert call_order == ["apple", "mango", "zebra"]

    @pytest.mark.asyncio
    async def test_fail_closed_runs_all_processors(self) -> None:
        """FAIL_CLOSED runs all processors (for visibility) and reports outcomes."""
        tracker_a = TrackingProcessor()
        tracker_b = TrackingProcessor()

        router = ProcessorRouter(
            {"a": tracker_a, "b": FailProcessor(), "c": tracker_b},
            policy=FailurePolicy.FAIL_CLOSED,
        )

        event = make_event_row()
        result = await router.process(event)

        assert result.ok is False
        # All processors should have been called
        assert tracker_a.call_count == 1
        assert tracker_b.call_count == 1

    @pytest.mark.asyncio
    async def test_best_effort_runs_all_processors(self) -> None:
        """BEST_EFFORT runs all processors and reports all outcomes."""
        tracker_a = TrackingProcessor()
        tracker_c = TrackingProcessor()

        router = ProcessorRouter(
            {"a": tracker_a, "b": FailProcessor(), "c": tracker_c},
            policy=FailurePolicy.BEST_EFFORT,
        )

        event = make_event_row()
        result = await router.process(event)

        assert result.ok is False
        assert tracker_a.call_count == 1
        assert tracker_c.call_count == 1

    @pytest.mark.asyncio
    async def test_all_ok_returns_ok_true(self) -> None:
        """Result is ok=True when all processors succeed."""
        router = ProcessorRouter(
            {"a": OkProcessor(), "b": OkProcessor()},
            policy=FailurePolicy.FAIL_CLOSED,
        )

        event = make_event_row()
        result = await router.process(event)

        assert result.ok is True

    @pytest.mark.asyncio
    async def test_exception_captured_as_failure(self) -> None:
        """Exceptions are captured as ok=False with reason."""
        router = ProcessorRouter(
            {"a": OkProcessor(), "b": ExceptionProcessor()},
            policy=FailurePolicy.BEST_EFFORT,
        )

        event = make_event_row()
        result = await router.process(event)

        assert result.ok is False
        # Reason should contain JSON with outcomes
        import json

        router_result = json.loads(result.reason)
        assert router_result["ok"] is False
        outcomes = router_result["outcomes"]
        b_outcome = next(o for o in outcomes if o["name"] == "b")
        assert b_outcome["ok"] is False
        assert "Exception" in b_outcome["reason"]

    @pytest.mark.asyncio
    async def test_outcome_contains_all_processors(self) -> None:
        """RouterResult contains outcomes for all processors."""
        router = ProcessorRouter(
            {"a": OkProcessor(), "b": FailProcessor(), "c": OkProcessor()},
            policy=FailurePolicy.BEST_EFFORT,
        )

        event = make_event_row()
        result = await router.process(event)

        import json

        router_result = json.loads(result.reason)
        names = [o["name"] for o in router_result["outcomes"]]
        assert sorted(names) == ["a", "b", "c"]
