"""Tests for orchestrator lifecycle."""

import asyncio
import json

import pytest

from ravebear_monolith.foundation.config import AppConfig
from ravebear_monolith.foundation.orchestrator import run


class TestOrchestratorRun:
    """Tests for the orchestrator run function."""

    @pytest.mark.asyncio
    async def test_run_completes_with_max_beats(self) -> None:
        """Orchestrator exits cleanly when max_beats is reached."""
        config = AppConfig(heartbeat_interval_s=1)
        result = await run(config, max_beats=2)
        assert result == 0

    @pytest.mark.asyncio
    async def test_run_handles_cancellation(self) -> None:
        """Orchestrator handles cancellation gracefully."""
        config = AppConfig(heartbeat_interval_s=1)

        async def cancel_after_delay() -> int:
            task = asyncio.create_task(run(config))
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                return await task
            except asyncio.CancelledError:
                return 0

        result = await asyncio.wait_for(cancel_after_delay(), timeout=5.0)
        assert result == 0

    @pytest.mark.asyncio
    async def test_run_does_not_hang(self) -> None:
        """Orchestrator with max_beats does not hang."""
        config = AppConfig(heartbeat_interval_s=1)

        async def bounded_run() -> int:
            return await run(config, max_beats=1)

        # Should complete within 2 seconds
        result = await asyncio.wait_for(bounded_run(), timeout=2.0)
        assert result == 0

    @pytest.mark.asyncio
    async def test_run_logs_structured_json(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Orchestrator logs structured JSON with events."""
        config = AppConfig(app_name="TestApp", heartbeat_interval_s=1, log_level="DEBUG")
        await run(config, max_beats=1)
        captured = capsys.readouterr()

        # Parse JSON lines from output
        lines = [line for line in captured.out.strip().split("\n") if line]
        assert len(lines) >= 1

        # Check for orchestrator_start event
        events = [json.loads(line)["event"] for line in lines]
        assert "orchestrator_start" in events
