"""
CVD Calculator Core
Based on proven CVD calculation logic.
Handles timeframe bucketing and cumulative delta properly.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

# Timeframe configuration
TIMEFRAME_SECONDS: Dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


def floor_window_start(ts: datetime, tf_seconds: int) -> datetime:
    """
    Floor timestamp to the start of its window.
    e.g., 14:23:45 with 1h -> 14:00:00
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    
    epoch = int(ts.timestamp())
    floored = epoch - (epoch % tf_seconds)
    return datetime.fromtimestamp(floored, tz=timezone.utc)


@dataclass
class Trade:
    """Single trade record."""
    timestamp: datetime
    price: Decimal
    size: Decimal
    side: str  # 'buy' or 'sell'
    trade_id: Optional[str] = None
    
    @property
    def size_usd(self) -> Decimal:
        return self.price * self.size
    
    @property
    def delta(self) -> Decimal:
        """CVD delta: positive for buys, negative for sells."""
        if self.side == 'buy':
            return self.size_usd
        else:
            return -self.size_usd


@dataclass
class CVDWindow:
    """CVD data for a single timeframe window."""
    window_start: datetime
    timeframe: str
    cvd_open: Decimal  # CVD at window open
    cvd_close: Decimal  # CVD at window close (current)
    cvd_high: Decimal  # Max CVD during window
    cvd_low: Decimal   # Min CVD during window
    buy_volume: Decimal
    sell_volume: Decimal
    trade_count: int
    
    @property
    def cvd_delta(self) -> Decimal:
        """Net CVD change during this window."""
        return self.cvd_close - self.cvd_open
    
    @property
    def net_volume(self) -> Decimal:
        return self.buy_volume - self.sell_volume


class CVDCalculator:
    """
    Cumulative Volume Delta Calculator.
    
    CVD = Running sum of (buy_volume - sell_volume) in USD terms.
    
    When CVD rises: More aggressive buying (market buys hitting asks)
    When CVD falls: More aggressive selling (market sells hitting bids)
    
    Divergence signals:
    - Price up + CVD down = weak rally, potential reversal
    - Price down + CVD up = weak selloff, potential bounce
    """
    
    def __init__(self, timeframes: List[str] = None):
        self.timeframes = timeframes or ["1m", "5m", "15m", "1h", "4h"]
        
        # Validate timeframes
        for tf in self.timeframes:
            if tf not in TIMEFRAME_SECONDS:
                raise ValueError(f"Unknown timeframe: {tf}")
        
        # Global CVD state
        self.cvd_total = Decimal("0")
        self.total_buy_volume = Decimal("0")
        self.total_sell_volume = Decimal("0")
        self.trade_count = 0
        
        # Per-timeframe window tracking
        self.current_windows: Dict[str, CVDWindow] = {}
        self.completed_windows: Dict[str, List[CVDWindow]] = {tf: [] for tf in self.timeframes}
        
        # Last trade info
        self.last_trade: Optional[Trade] = None
        self.last_price = Decimal("0")
    
    def process_trade(self, trade: Trade) -> Dict[str, CVDWindow]:
        """
        Process a single trade and update CVD state.
        Returns dict of updated windows.
        """
        # Update global CVD
        delta = trade.delta
        self.cvd_total += delta
        self.trade_count += 1
        self.last_trade = trade
        self.last_price = trade.price
        
        if trade.side == 'buy':
            self.total_buy_volume += trade.size_usd
        else:
            self.total_sell_volume += trade.size_usd
        
        # Update each timeframe window
        updated = {}
        
        for tf in self.timeframes:
            tf_secs = TIMEFRAME_SECONDS[tf]
            window_start = floor_window_start(trade.timestamp, tf_secs)
            
            # Check if we need a new window
            if tf not in self.current_windows or self.current_windows[tf].window_start != window_start:
                # Archive old window if exists
                if tf in self.current_windows:
                    self.completed_windows[tf].append(self.current_windows[tf])
                    # Keep only last 100 windows per timeframe
                    if len(self.completed_windows[tf]) > 100:
                        self.completed_windows[tf] = self.completed_windows[tf][-100:]
                
                # Create new window
                self.current_windows[tf] = CVDWindow(
                    window_start=window_start,
                    timeframe=tf,
                    cvd_open=self.cvd_total - delta,  # CVD before this trade
                    cvd_close=self.cvd_total,
                    cvd_high=self.cvd_total,
                    cvd_low=self.cvd_total,
                    buy_volume=trade.size_usd if trade.side == 'buy' else Decimal("0"),
                    sell_volume=trade.size_usd if trade.side == 'sell' else Decimal("0"),
                    trade_count=1,
                )
            else:
                # Update existing window
                window = self.current_windows[tf]
                window.cvd_close = self.cvd_total
                window.cvd_high = max(window.cvd_high, self.cvd_total)
                window.cvd_low = min(window.cvd_low, self.cvd_total)
                window.trade_count += 1
                
                if trade.side == 'buy':
                    window.buy_volume += trade.size_usd
                else:
                    window.sell_volume += trade.size_usd
            
            updated[tf] = self.current_windows[tf]
        
        return updated
    
    def process_trades(self, trades: List[Trade]) -> None:
        """Process multiple trades in order."""
        for trade in sorted(trades, key=lambda t: (t.timestamp, t.trade_id or '')):
            self.process_trade(trade)
    
    def get_window(self, timeframe: str) -> Optional[CVDWindow]:
        """Get current window for timeframe."""
        return self.current_windows.get(timeframe)
    
    def get_recent_windows(self, timeframe: str, n: int = 10) -> List[CVDWindow]:
        """Get last N completed windows for timeframe."""
        completed = self.completed_windows.get(timeframe, [])
        current = self.current_windows.get(timeframe)
        
        windows = completed[-n:] if len(completed) >= n else completed.copy()
        if current and len(windows) < n:
            windows.append(current)
        
        return windows
    
    def get_cvd_summary(self) -> Dict:
        """Get summary of CVD state."""
        return {
            'cvd_total': self.cvd_total,
            'total_buy_volume': self.total_buy_volume,
            'total_sell_volume': self.total_sell_volume,
            'net_volume': self.total_buy_volume - self.total_sell_volume,
            'trade_count': self.trade_count,
            'last_price': self.last_price,
            'windows': {
                tf: {
                    'cvd_delta': w.cvd_delta if w else Decimal("0"),
                    'buy_vol': w.buy_volume if w else Decimal("0"),
                    'sell_vol': w.sell_volume if w else Decimal("0"),
                    'trades': w.trade_count if w else 0,
                }
                for tf, w in self.current_windows.items()
            }
        }
    
    def detect_divergence(self, price_change: Decimal) -> Optional[str]:
        """
        Detect CVD/price divergence.
        
        Returns:
        - 'bullish_div': Price down but CVD up (buyers absorbing)
        - 'bearish_div': Price up but CVD down (sellers distributing)
        - None: No divergence
        """
        if not self.current_windows:
            return None
        
        # Use 5m window for divergence
        window = self.current_windows.get('5m')
        if not window:
            return None
        
        cvd_change = window.cvd_delta
        
        # Thresholds (adjust based on your needs)
        price_threshold = Decimal("10")  # $10 move
        cvd_threshold = Decimal("10000")  # $10k CVD
        
        if abs(price_change) < price_threshold or abs(cvd_change) < cvd_threshold:
            return None
        
        if price_change > 0 and cvd_change < 0:
            return 'bearish_div'
        elif price_change < 0 and cvd_change > 0:
            return 'bullish_div'
        
        return None


# Utility functions for the collector
def create_trade(price: float, size: float, side: str, timestamp: datetime, trade_id: str = None) -> Trade:
    """Helper to create Trade object."""
    return Trade(
        timestamp=timestamp,
        price=Decimal(str(price)),
        size=Decimal(str(size)),
        side=side,
        trade_id=trade_id,
    )
