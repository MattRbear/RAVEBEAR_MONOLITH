from dataclasses import dataclass
from typing import Optional, Literal
import numpy as np

@dataclass
class Position:
    """
    Represents a single asset position (Long/Short) in Perpetual Futures.
    Tracks entry price, size, margin, and handles liquidation/funding logic.
    """
    symbol: str
    side: Literal["LONG", "SHORT"]
    entry_price: float
    size: float  # Quantity of the asset (e.g., 0.1 BTC)
    leverage: float
    entry_time: object = None # Adding entry time tracking
    maintenance_margin_rate: float = 0.005  # 0.5% default
    liquidation_price: float = 0.0
    initial_margin: float = 0.0
    
    def __post_init__(self):
        self.initial_margin = (self.entry_price * self.size) / self.leverage
        self.update_liquidation_price()

    @property
    def value(self) -> float:
        """Notional value of the position."""
        return self.entry_price * self.size

    def update_liquidation_price(self):
        """
        Calculates the price at which Equity < Maintenance Margin.
        Using precise Isolated Margin formula.
        """
        if self.side == "LONG":
            # Liq Price = Entry * (1 - 1/Lev + MMR)
            self.liquidation_price = self.entry_price * (1 - (1/self.leverage) + self.maintenance_margin_rate)
        else:
            # Liq Price = Entry * (1 + 1/Lev - MMR)
            self.liquidation_price = self.entry_price * (1 + (1/self.leverage) - self.maintenance_margin_rate)

    def check_liquidation(self, current_price: float) -> bool:
        """Returns True if the position is liquidated at current_price."""
        if self.side == "LONG":
            return current_price <= self.liquidation_price
        else:
            return current_price >= self.liquidation_price

    def calculate_pnl(self, current_price: float) -> float:
        """Calculates Unrealized PnL."""
        if self.side == "LONG":
            return (current_price - self.entry_price) * self.size
        else:
            return (self.entry_price - current_price) * self.size

    def apply_funding(self, current_mark_price: float, funding_rate: float) -> float:
        """
        Calculates funding based on Current Mark Value (not Entry).
        
        fee = notional_value * funding_rate
        """
        # Funding is based on Notional Value = Price * Size
        notional_value = current_mark_price * self.size
        fee = notional_value * funding_rate
        
        # Longs pay positive rate, Shorts receive positive rate
        if self.side == "LONG":
            return fee
        else:
            return -fee
