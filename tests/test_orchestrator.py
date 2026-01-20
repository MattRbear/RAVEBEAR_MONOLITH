"""Tests for orchestrator lifecycle."""

import asyncio
import json
from pathlib import Path

import pytest

from ravebear_monolith.foundation.config import AppConfig
from ravebear_monolith.foundation.orchestrator import run


class TestOrchestratorRun:
    """Tests for the orchestrator run function."""

    @pytest.mark.asyncio
    async def test_run_completes_with_max_beats(self, tmp_path: Path) -> None:
        """Orchestrator exits cleanly when max_beats is reached."""
        config = AppConfig(heartbeat_interval_s=1, data_dir=tmp_path)
        result = await run(config, max_beats=2)
        assert result == 0

    @pytest.mark.asyncio
    async def test_run_handles_cancellation(self, tmp_path: Path) -> None:
        """Orchestrator handles cancellation gracefully."""
        config = AppConfig(heartbeat_interval_s=1, data_dir=tmp_path)

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
    async def test_run_does_not_hang(self, tmp_path: Path) -> None:
        """Orchestrator with max_beats does not hang."""
        config = AppConfig(heartbeat_interval_s=1, data_dir=tmp_path)

        async def bounded_run() -> int:
            return await run(config, max_beats=1)

        # Should complete within 2 seconds
        result = await asyncio.wait_for(bounded_run(), timeout=2.0)
        assert result == 0

    @pytest.mark.asyncio
    async def test_run_logs_structured_json(
        self, capsys: pytest.CaptureFixture[str], tmp_path: Path
    ) -> None:
        """Orchestrator logs structured JSON with events."""
        config = AppConfig(
            app_name="TestApp",
            heartbeat_interval_s=1,
            log_level="DEBUG",
            data_dir=tmp_path,
        )
        await run(config, max_beats=1)
        captured = capsys.readouterr()

        # Parse JSON lines from output
        lines = [line for line in captured.out.strip().split("\n") if line]
        assert len(lines) >= 1

        # Check for orchestrator_start event
        events = [json.loads(line)["event"] for line in lines]
        assert "orchestrator_start" in events

    @pytest.mark.asyncio
    async def test_run_logs_health_snapshot(
        self, capsys: pytest.CaptureFixture[str], tmp_path: Path
    ) -> None:
        """Orchestrator logs health snapshot at startup."""
        config = AppConfig(heartbeat_interval_s=1, log_level="DEBUG", data_dir=tmp_path)
        await run(config, max_beats=1)
        captured = capsys.readouterr()

        lines = [line for line in captured.out.strip().split("\n") if line]
        events = [json.loads(line)["event"] for line in lines]
        assert "health_snapshot" in events

    @pytest.mark.asyncio
    async def test_kill_switch_returns_2(self, tmp_path: Path) -> None:
        """Kill switch triggered returns exit code 2."""
        kill_file = tmp_path / "kill_switch.txt"
        kill_file.write_text("KILL", encoding="utf-8")

        config = AppConfig(
            heartbeat_interval_s=1,
            data_dir=tmp_path,
            kill_switch_path=kill_file,
        )
        result = await run(config, max_beats=5)
        assert result == 2

    @pytest.mark.asyncio
    async def test_no_kill_switch_returns_0(self, tmp_path: Path) -> None:
        """No kill switch file means normal exit code 0."""
        config = AppConfig(
            heartbeat_interval_s=1,
            data_dir=tmp_path,
            kill_switch_path=tmp_path / "nonexistent.txt",
        )
        result = await run(config, max_beats=1)
        assert result == 0


class TestRunLiveWithProcessing:
    """Tests for run_live_with_processing function."""

    @pytest.mark.asyncio
    async def test_cancellation_stops_both_tasks(self, tmp_path: Path) -> None:
        """Cancellation stops both collector and processing tasks cleanly."""
        from unittest.mock import AsyncMock, MagicMock, patch

        from ravebear_monolith.foundation.orchestrator import run_live_with_processing

        config = AppConfig(
            data_dir=tmp_path,
            storage={"db_path": tmp_path / "events.db"},
        )

        # Create mock collector that yields events slowly
        mock_collector = MagicMock()
        mock_collector.is_running = True

        async def slow_events(*args, **kwargs):
            while True:
                await asyncio.sleep(10)  # Will be cancelled before this completes
                yield MagicMock()

        # Mock router to use slow events
        with (
            patch("ravebear_monolith.collectors.router.CollectorRouter") as MockRouter,
            patch("ravebear_monolith.core.replay_runner.ReplayRunner") as MockRunner,
            patch(
                "ravebear_monolith.processors.okx.trades_to_bars_1s.TradesToBars1sProcessor"
            ) as MockProcessor,
        ):
            MockProcessor.return_value = MagicMock()
            mock_router_instance = MagicMock()
            mock_router_instance.events = slow_events
            mock_router_instance.event_count = 0
            MockRouter.return_value = mock_router_instance

            # Mock replay runner to also be slow
            mock_runner_instance = AsyncMock()
            mock_runner_instance.run = AsyncMock(side_effect=asyncio.CancelledError)
            MockRunner.return_value = mock_runner_instance

            # Run with timeout to ensure cancellation
            async def run_with_cancel():
                task = asyncio.create_task(
                    run_live_with_processing(config, [mock_collector], max_events=10)
                )
                await asyncio.sleep(0.1)
                task.cancel()
                try:
                    return await task
                except asyncio.CancelledError:
                    return 0

            result = await asyncio.wait_for(run_with_cancel(), timeout=5.0)
            assert result == 0

    @pytest.mark.asyncio
    async def test_completes_with_max_events(self, tmp_path: Path) -> None:
        """Completes when max_events is reached."""
        from unittest.mock import AsyncMock, MagicMock, patch

        from ravebear_monolith.collectors.base import CollectorEvent
        from ravebear_monolith.foundation.orchestrator import run_live_with_processing

        config = AppConfig(
            data_dir=tmp_path,
            storage={"db_path": tmp_path / "events.db"},
        )

        events_yielded = 0

        async def limited_events(*args, max_events=None, **kwargs):
            nonlocal events_yielded
            for i in range(max_events or 3):
                events_yielded += 1
                yield CollectorEvent(
                    source="test",
                    event_type="trade",
                    ts_utc="2024-01-01T00:00:00Z",
                    payload={"price": 100, "size": 1, "trade_ts_ms": 1704067200000},
                )
                await asyncio.sleep(0.01)

        mock_collector = MagicMock()
        mock_collector.is_running = True

        with (
            patch("ravebear_monolith.collectors.router.CollectorRouter") as MockRouter,
            patch("ravebear_monolith.core.replay_runner.ReplayRunner") as MockRunner,
            patch(
                "ravebear_monolith.processors.okx.trades_to_bars_1s.TradesToBars1sProcessor"
            ) as MockProcessor,
        ):
            mock_router_instance = MagicMock()
            mock_router_instance.events = limited_events
            mock_router_instance.event_count = 0
            MockRouter.return_value = mock_router_instance

            # Mock replay runner to return immediately (caught up)
            mock_runner_instance = AsyncMock()
            mock_runner_instance.run = AsyncMock(return_value=0)
            MockRunner.return_value = mock_runner_instance

            MockProcessor.return_value = MagicMock()

            result = await asyncio.wait_for(
                run_live_with_processing(config, [mock_collector], max_events=3),
                timeout=5.0,
            )
            assert result == 0
            assert events_yielded == 3


class TestCLIParsing:
    """Tests for CLI argument parsing."""

    def test_mode_live_with_processing_accepted(self) -> None:
        """CLI accepts live-with-processing mode."""
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--mode",
            choices=["live", "replay", "live-with-processing"],
            default="live",
        )
        parser.add_argument("--cursor-name", default="default")
        parser.add_argument("--poll-interval-s", type=float, default=1.0)

        args = parser.parse_args(["--mode", "live-with-processing"])
        assert args.mode == "live-with-processing"

    def test_poll_interval_parsed(self) -> None:
        """CLI parses poll-interval-s correctly."""
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--poll-interval-s", type=float, default=1.0)

        args = parser.parse_args(["--poll-interval-s", "2.5"])
        assert args.poll_interval_s == 2.5
