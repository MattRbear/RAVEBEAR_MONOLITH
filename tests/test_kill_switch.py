"""Tests for kill switch functionality."""

from pathlib import Path

from ravebear_monolith.util.kill_switch import KillSwitch


class TestKillSwitch:
    """Tests for KillSwitch class."""

    def test_missing_file_returns_false(self, tmp_path: Path) -> None:
        """Missing kill switch file does not trigger halt."""
        ks = KillSwitch(tmp_path / "nonexistent.txt")
        assert ks.should_halt() is False
        assert ks.reason() == ""

    def test_file_with_kill_triggers_halt(self, tmp_path: Path) -> None:
        """File containing KILL triggers halt."""
        kill_file = tmp_path / "kill_switch.txt"
        kill_file.write_text("KILL", encoding="utf-8")

        ks = KillSwitch(kill_file)
        assert ks.should_halt() is True
        assert "KILL" in ks.reason()

    def test_kill_case_insensitive(self, tmp_path: Path) -> None:
        """KILL detection is case insensitive."""
        kill_file = tmp_path / "kill_switch.txt"
        kill_file.write_text("kill", encoding="utf-8")

        ks = KillSwitch(kill_file)
        assert ks.should_halt() is True

    def test_kill_with_whitespace(self, tmp_path: Path) -> None:
        """KILL with surrounding whitespace still triggers."""
        kill_file = tmp_path / "kill_switch.txt"
        kill_file.write_text("  KILL  \n", encoding="utf-8")

        ks = KillSwitch(kill_file)
        assert ks.should_halt() is True

    def test_other_content_does_not_trigger(self, tmp_path: Path) -> None:
        """File with other content does not trigger halt."""
        kill_file = tmp_path / "kill_switch.txt"
        kill_file.write_text("STOP", encoding="utf-8")

        ks = KillSwitch(kill_file)
        assert ks.should_halt() is False
        assert ks.reason() == ""

    def test_empty_file_does_not_trigger(self, tmp_path: Path) -> None:
        """Empty file does not trigger halt."""
        kill_file = tmp_path / "kill_switch.txt"
        kill_file.write_text("", encoding="utf-8")

        ks = KillSwitch(kill_file)
        assert ks.should_halt() is False

    def test_reason_includes_path(self, tmp_path: Path) -> None:
        """Reason includes the file path."""
        kill_file = tmp_path / "my_kill_switch.txt"
        kill_file.write_text("KILL", encoding="utf-8")

        ks = KillSwitch(kill_file)
        ks.should_halt()
        assert str(kill_file) in ks.reason()

    def test_reason_empty_when_no_halt(self, tmp_path: Path) -> None:
        """Reason is empty when halt is not triggered."""
        kill_file = tmp_path / "kill_switch.txt"
        kill_file.write_text("OK", encoding="utf-8")

        ks = KillSwitch(kill_file)
        ks.should_halt()
        assert ks.reason() == ""
