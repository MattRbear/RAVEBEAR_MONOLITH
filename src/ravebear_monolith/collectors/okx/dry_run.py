"""OKX trades dry-run collector using fixtures.

This collector reads from a fixture file for testing and development.
No network calls are made.
"""

import json
from pathlib import Path

from ravebear_monolith.collectors.base import CollectorBase, CollectorEvent
from ravebear_monolith.collectors.okx.schemas import OKXTrade


class OKXTradesDryRunCollector(CollectorBase):
    """Dry-run OKX trades collector using fixtures.

    Reads trades from a JSON fixture file and emits them as CollectorEvents.
    Useful for testing the data pipeline without network calls.

    Args:
        fixture_path: Path to JSON fixture file.
    """

    def __init__(self, fixture_path: Path) -> None:
        super().__init__("okx_trades_dry")
        self._fixture_path = fixture_path
        self._trades: list[dict] = []
        self._index = 0

    async def start(self) -> None:
        """Load fixture file and prepare for iteration."""
        self._running = True
        self._index = 0

        if self._fixture_path.exists():
            with open(self._fixture_path, encoding="utf-8") as f:
                data = json.load(f)
                # Handle both array and {"trades": [...]} formats
                if isinstance(data, list):
                    self._trades = data
                elif isinstance(data, dict) and "trades" in data:
                    self._trades = data["trades"]
                else:
                    self._trades = []
        else:
            self._trades = []

    async def stop(self) -> None:
        """Stop the collector."""
        self._running = False
        self._trades = []
        self._index = 0

    async def next_event(self) -> CollectorEvent | None:
        """Get next trade event from fixture.

        Returns:
            CollectorEvent with trade data, or None if exhausted.
        """
        if not self._running:
            return None

        if self._index >= len(self._trades):
            return None

        raw_trade = self._trades[self._index]
        self._index += 1

        # Parse and validate
        trade = OKXTrade.from_raw(raw_trade)

        return CollectorEvent(
            source="okx",
            event_type="trade",
            ts_utc=trade.ts_utc,
            payload=trade.model_dump(),
        )
