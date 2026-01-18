"""Test runtime boot lifecycle."""

from pathlib import Path

import pytest

from ravebear_monolith.runtime.main import _run_lifecycle


class TestRuntimeBoot:
    """Tests for runtime boot sequence."""

    @pytest.mark.asyncio
    async def test_run_exits_cleanly(self, tmp_path: Path) -> None:
        """run() exits cleanly with exit code 0."""
        # Create minimal valid config
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(
            "app_name: TestMonolith\nheartbeat_interval_s: 1\n",
            encoding="utf-8",
        )

        exit_code = await _run_lifecycle(config_path=config_file, max_beats=1)

        assert exit_code == 0

    @pytest.mark.asyncio
    async def test_run_no_exceptions(self, tmp_path: Path) -> None:
        """run() completes without raising exceptions."""
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text("", encoding="utf-8")  # Empty = use defaults

        # Should not raise
        exit_code = await _run_lifecycle(config_path=config_file, max_beats=1)
        assert exit_code == 0
