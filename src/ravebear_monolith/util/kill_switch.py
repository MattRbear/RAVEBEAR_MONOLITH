"""Kill switch for emergency halt.

Provides fail-closed mechanism to stop all operations
when a kill switch file contains "KILL".
"""

from pathlib import Path


class KillSwitch:
    """Kill switch that halts operations when triggered.

    The kill switch is triggered when the file exists and contains "KILL".
    Missing file means no halt (fail-open for normal operation).

    Args:
        path: Path to the kill switch file.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._cached_reason: str = ""

    def should_halt(self) -> bool:
        """Check if operations should halt.

        Returns:
            True if kill switch is active, False otherwise.
        """
        if not self._path.exists():
            self._cached_reason = ""
            return False

        try:
            content = self._path.read_text(encoding="utf-8").strip().upper()
            if content == "KILL":
                self._cached_reason = f"Kill switch active: {self._path} contains 'KILL'"
                return True
            self._cached_reason = ""
            return False
        except Exception as e:
            # Fail-closed on read error: treat as kill
            self._cached_reason = f"Kill switch read error: {self._path}: {e}"
            return True

    def reason(self) -> str:
        """Get the reason for halt.

        Returns:
            Reason string if halt is active, empty string otherwise.
        """
        return self._cached_reason
