"""Processor router for multi-processor fan-out with failure policies.

Routes events to multiple processors in deterministic order.
"""

import json
from enum import Enum

from pydantic import BaseModel

from ravebear_monolith.core.processor import ProcessorBase, ProcessResult
from ravebear_monolith.storage.event_reader import EventRow


class FailurePolicy(Enum):
    """Policy for handling processor failures."""

    FAIL_CLOSED = "fail_closed"  # Halt on failure, trigger kill switch
    BEST_EFFORT = "best_effort"  # Run all, report outcomes, exit 3 on failure


class ProcessorOutcome(BaseModel, extra="forbid"):
    """Outcome of a single processor's execution."""

    name: str
    ok: bool
    reason: str | None = None


class RouterResult(BaseModel, extra="forbid"):
    """Result of router processing all processors."""

    ok: bool
    outcomes: list[ProcessorOutcome]


class ProcessorRouter(ProcessorBase):
    """Router that fans out events to multiple processors.

    Processors are run in deterministic (sorted by name) order.

    Args:
        processors: Dict mapping names to processor instances.
        policy: Failure policy (FAIL_CLOSED or BEST_EFFORT).
    """

    def __init__(
        self,
        processors: dict[str, ProcessorBase],
        *,
        policy: FailurePolicy = FailurePolicy.FAIL_CLOSED,
    ) -> None:
        self._processors = processors
        self._policy = policy

    @property
    def policy(self) -> FailurePolicy:
        """Get the failure policy."""
        return self._policy

    async def process(self, event: EventRow) -> ProcessResult:
        """Process event through all processors.

        Args:
            event: EventRow to process.

        Returns:
            ProcessResult with ok=all_succeeded and reason containing RouterResult JSON.
        """
        outcomes: list[ProcessorOutcome] = []
        all_ok = True

        # Deterministic order: sorted by name
        for name in sorted(self._processors.keys()):
            processor = self._processors[name]

            try:
                result = await processor.process(event)
                outcome = ProcessorOutcome(
                    name=name,
                    ok=result.ok,
                    reason=result.reason,
                )
            except Exception as e:
                outcome = ProcessorOutcome(
                    name=name,
                    ok=False,
                    reason=f"Exception: {e!r}",
                )

            outcomes.append(outcome)

            if not outcome.ok:
                all_ok = False

                # FAIL_CLOSED: could short-circuit, but we run all for visibility
                # Both policies run all processors, just differ in behavior after

        router_result = RouterResult(ok=all_ok, outcomes=outcomes)

        return ProcessResult(
            ok=router_result.ok,
            reason=json.dumps(router_result.model_dump()),
        )

    async def finalize(self) -> RouterResult:
        """Finalize all child processors that have finalize() method.

        Runs in deterministic (sorted by name) order.
        - FAIL_CLOSED: stop on first finalize failure
        - BEST_EFFORT: run all, ok=True only if all succeeded

        Returns:
            RouterResult with outcomes for each processor finalized.
        """
        outcomes: list[ProcessorOutcome] = []
        all_ok = True

        for name in sorted(self._processors.keys()):
            processor = self._processors[name]

            if not (hasattr(processor, "finalize") and callable(processor.finalize)):
                continue

            try:
                await processor.finalize()
                outcome = ProcessorOutcome(name=name, ok=True)
            except Exception as e:
                outcome = ProcessorOutcome(
                    name=name,
                    ok=False,
                    reason=f"Finalize exception: {e!r}",
                )

            outcomes.append(outcome)

            if not outcome.ok:
                all_ok = False
                if self._policy == FailurePolicy.FAIL_CLOSED:
                    # Stop on first failure
                    break

        return RouterResult(ok=all_ok, outcomes=outcomes)
