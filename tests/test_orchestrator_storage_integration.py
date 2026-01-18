"""Tests for orchestrator + EventSink integration."""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from ravebear_monolith.collectors.okx.dry_run import OKXTradesDryRunCollector
from ravebear_monolith.foundation.config import AppConfig
from ravebear_monolith.foundation.orchestrator import run_with_collectors
from ravebear_monolith.storage.event_sink import EventSink

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestOrchestratorStorageIntegration:
    """Integration tests for orchestrator with EventSink."""

    @pytest.mark.asyncio
    async def test_events_written_to_sqlite(self, tmp_path: Path) -> None:
        """Events from collector are persisted to SQLite."""
        db_path = tmp_path / "events.db"
        config = AppConfig(
            heartbeat_interval_s=1,
            data_dir=tmp_path,
            storage={"db_path": db_path},
            kill_switch_path=tmp_path / "kill.txt",
        )

        collector = OKXTradesDryRunCollector(FIXTURES_DIR / "okx_trades.json")

        result = await run_with_collectors(config, [collector], max_events=5)

        assert result == 0

        # Verify events in database
        sink = EventSink(db_path)
        await sink.open()
        try:
            count = await sink.count()
            assert count == 5
        finally:
            await sink.close()

    @pytest.mark.asyncio
    async def test_all_events_written(self, tmp_path: Path) -> None:
        """All events from fixture are written to database."""
        db_path = tmp_path / "events.db"
        config = AppConfig(
            heartbeat_interval_s=1,
            data_dir=tmp_path,
            storage={"db_path": db_path},
            kill_switch_path=tmp_path / "kill.txt",
        )

        collector = OKXTradesDryRunCollector(FIXTURES_DIR / "okx_trades.json")

        result = await run_with_collectors(config, [collector], max_events=100)

        assert result == 0

        # Verify all 10 events from fixture
        sink = EventSink(db_path)
        await sink.open()
        try:
            count = await sink.count()
            assert count == 10
        finally:
            await sink.close()

    @pytest.mark.asyncio
    async def test_kill_switch_triggered_on_write_failure(self, tmp_path: Path) -> None:
        """Kill switch triggered when EventSink.write fails."""
        db_path = tmp_path / "events.db"
        kill_path = tmp_path / "kill.txt"
        config = AppConfig(
            heartbeat_interval_s=1,
            data_dir=tmp_path,
            storage={"db_path": db_path},
            kill_switch_path=kill_path,
        )

        collector = OKXTradesDryRunCollector(FIXTURES_DIR / "okx_trades.json")

        # Patch sink.write to raise
        with patch.object(
            EventSink, "write", new_callable=AsyncMock, side_effect=RuntimeError("disk full")
        ):
            result = await run_with_collectors(config, [collector], max_events=5)

        assert result == 2

        # Verify kill switch was written
        assert kill_path.exists()
        content = kill_path.read_text(encoding="utf-8")
        assert "KILL" in content

    @pytest.mark.asyncio
    async def test_nonzero_exit_on_sink_failure(self, tmp_path: Path) -> None:
        """Orchestrator returns non-zero exit code on sink failure."""
        db_path = tmp_path / "events.db"
        config = AppConfig(
            heartbeat_interval_s=1,
            data_dir=tmp_path,
            storage={"db_path": db_path},
            kill_switch_path=tmp_path / "kill.txt",
        )

        collector = OKXTradesDryRunCollector(FIXTURES_DIR / "okx_trades.json")

        with patch.object(
            EventSink, "write", new_callable=AsyncMock, side_effect=Exception("write error")
        ):
            result = await run_with_collectors(config, [collector], max_events=1)

        assert result == 2

    @pytest.mark.asyncio
    async def test_sink_closed_on_completion(self, tmp_path: Path) -> None:
        """EventSink is closed after processing completes."""
        db_path = tmp_path / "events.db"
        config = AppConfig(
            heartbeat_interval_s=1,
            data_dir=tmp_path,
            storage={"db_path": db_path},
            kill_switch_path=tmp_path / "kill.txt",
        )

        collector = OKXTradesDryRunCollector(FIXTURES_DIR / "okx_trades.json")

        result = await run_with_collectors(config, [collector], max_events=1)
        assert result == 0

        # Opening new sink should work (previous was closed)
        sink = EventSink(db_path)
        await sink.open()
        try:
            count = await sink.count()
            assert count == 1
        finally:
            await sink.close()
