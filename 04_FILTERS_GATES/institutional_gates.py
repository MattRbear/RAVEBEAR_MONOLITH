"""
MICROSTRUCTURE ENGINE - Institutional Gates
============================================
Advanced signal gating for MM-grade execution.
"""

import time
from typing import Optional, List, Deque, Dict, Tuple
from collections import deque
from dataclasses import dataclass, field
from data_types import OrderBookLevel, Side
import numpy as np


@dataclass
class LadderSnapshot:
    """Snapshot of top N levels for stability tracking."""
    timestamp: float
    bid_levels: List[Tuple[float, float]]  # [(price, qty), ...]
    ask_levels: List[Tuple[float, float]]
    
    
@dataclass
class TradeAccumulator:
    """Track trades for CVD calculation."""
    timestamp: float
    price: float
    volume: float
    side: Side  # Aggressor side


class LadderStabilityTracker:
    """
    Gate 1: Bid-Ask Ladder Stability
    
    Filters spoofing by requiring top levels to show consistent
    accumulation for minimum duration before signal fires.
    """
    
    def __init__(
        self,
        depth: int = 3,
        min_stable_ms: int = 500,
        max_variance_pct: float = 0.20  # Max 20% variance in quantities
    ):
        self.depth = depth
        self.min_stable_ms = min_stable_ms
        self.max_variance_pct = max_variance_pct
        self.history: Deque[LadderSnapshot] = deque(maxlen=50)
    
    def update(self, bids: List[OrderBookLevel], asks: List[OrderBookLevel]):
        """Record current ladder state."""
        snapshot = LadderSnapshot(
            timestamp=time.time() * 1000,  # ms
            bid_levels=[(b.price, b.quantity) for b in bids[:self.depth]],
            ask_levels=[(a.price, a.quantity) for a in asks[:self.depth]]
        )
        self.history.append(snapshot)
    
    def check_stability(self, side: Side) -> Tuple[bool, str]:
        """
        Check if ladder shows stable accumulation.
        Returns (is_stable, reason).
        """
        if len(self.history) < 3:
            return False, "Insufficient history"
        
        now = time.time() * 1000
        min_time = now - self.min_stable_ms
        
        # Get relevant snapshots
        recent = [s for s in self.history if s.timestamp >= min_time]
        if len(recent) < 2:
            return False, f"Need {self.min_stable_ms}ms of data"
        
        # Check the side we're trading
        if side == Side.BUY:
            # For BUY signal, check bid ladder stability
            quantities = []
            for snap in recent:
                total_qty = sum(qty for _, qty in snap.bid_levels)
                quantities.append(total_qty)
        else:
            # For SELL signal, check ask ladder stability
            quantities = []
            for snap in recent:
                total_qty = sum(qty for _, qty in snap.ask_levels)
                quantities.append(total_qty)
        
        if not quantities:
            return False, "No quantity data"
        
        # Check variance
        mean_qty = np.mean(quantities)
        if mean_qty == 0:
            return False, "Zero mean quantity"
        
        variance = np.std(quantities) / mean_qty
        
        if variance > self.max_variance_pct:
            return False, f"Ladder unstable: {variance*100:.1f}% variance"
        
        # Check for accumulation (not depletion)
        if quantities[-1] < quantities[0] * 0.8:
            return False, "Ladder depleting, not accumulating"
        
        return True, f"Stable for {self.min_stable_ms}ms"


class VWAPTracker:
    """
    Gate 2: VWAP Confluence
    
    Only fire signals when price is near VWAP (mean territory).
    Avoids trading at distribution edges where liquidity thins.
    """
    
    def __init__(self, window_seconds: int = 300, max_distance_bps: float = 15.0):
        self.window_seconds = window_seconds
        self.max_distance_bps = max_distance_bps
        self.trades: Deque[Tuple[float, float, float]] = deque()  # (timestamp, price, volume)
        self.cumulative_pv: float = 0.0  # price * volume
        self.cumulative_v: float = 0.0   # volume
    
    def update(self, price: float, volume: float):
        """Add trade to VWAP calculation."""
        now = time.time()
        self.trades.append((now, price, volume))
        self.cumulative_pv += price * volume
        self.cumulative_v += volume
        
        # Prune old trades
        cutoff = now - self.window_seconds
        while self.trades and self.trades[0][0] < cutoff:
            old_ts, old_price, old_vol = self.trades.popleft()
            self.cumulative_pv -= old_price * old_vol
            self.cumulative_v -= old_vol
    
    def get_vwap(self) -> float:
        """Calculate current VWAP."""
        if self.cumulative_v == 0:
            return 0.0
        return self.cumulative_pv / self.cumulative_v
    
    def check_confluence(self, current_price: float) -> Tuple[bool, str, float]:
        """
        Check if price is within acceptable distance from VWAP.
        Returns (is_valid, reason, distance_bps).
        """
        vwap = self.get_vwap()
        if vwap == 0:
            return True, "VWAP not established", 0.0  # Allow if no data
        
        distance_bps = abs(current_price - vwap) / vwap * 10000
        
        if distance_bps > self.max_distance_bps:
            return False, f"Price {distance_bps:.1f}bps from VWAP (max {self.max_distance_bps})", distance_bps
        
        return True, f"Within {distance_bps:.1f}bps of VWAP", distance_bps


class MicroCVDTracker:
    """
    Gate 3: Micro Cumulative Volume Delta
    
    Require CVD direction to match OBI direction.
    Confirms that actual flow supports the order book imbalance.
    """
    
    def __init__(self, window_seconds: int = 60, agreement_threshold: float = 0.6):
        self.window_seconds = window_seconds
        self.agreement_threshold = agreement_threshold
        self.trades: Deque[TradeAccumulator] = deque()
    
    def update(self, price: float, volume: float, side: Side):
        """Add trade to CVD calculation."""
        now = time.time()
        self.trades.append(TradeAccumulator(
            timestamp=now,
            price=price,
            volume=volume,
            side=side
        ))
        
        # Prune old trades
        cutoff = now - self.window_seconds
        while self.trades and self.trades[0].timestamp < cutoff:
            self.trades.popleft()
    
    def get_cvd(self) -> float:
        """
        Calculate CVD.
        Positive = net buying (more buy aggressors)
        Negative = net selling (more sell aggressors)
        """
        buy_vol = sum(t.volume for t in self.trades if t.side == Side.BUY)
        sell_vol = sum(t.volume for t in self.trades if t.side == Side.SELL)
        return buy_vol - sell_vol
    
    def get_cvd_direction(self) -> Side:
        """Get CVD direction as Side."""
        cvd = self.get_cvd()
        if cvd > 0:
            return Side.BUY
        elif cvd < 0:
            return Side.SELL
        return Side.HOLD
    
    def check_agreement(self, obi_side: Side) -> Tuple[bool, str]:
        """
        Check if CVD agrees with OBI direction.
        """
        cvd = self.get_cvd()
        cvd_side = self.get_cvd_direction()
        
        if cvd_side == Side.HOLD:
            return True, "CVD neutral"  # Allow if no clear direction
        
        if cvd_side == obi_side:
            return True, f"CVD confirms {obi_side.value} (CVD={cvd:+.4f})"
        
        return False, f"CVD contradicts: OBI={obi_side.value} but CVD={cvd:+.4f}"


class WallDetector:
    """
    Gate 4: Liquidity Wall Detection
    
    Decline signals if a wall within 2-5 bps is countering the imbalance.
    Prevents suicide longs into ask-walls or shorts into bid-walls.
    """
    
    def __init__(
        self,
        wall_threshold_mult: float = 4.0,  # Wall = 4x average size
        danger_zone_bps: float = 5.0       # Check within 5 bps
    ):
        self.wall_threshold_mult = wall_threshold_mult
        self.danger_zone_bps = danger_zone_bps
    
    def detect_walls(
        self, 
        bids: List[OrderBookLevel], 
        asks: List[OrderBookLevel],
        current_price: float
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Detect walls within danger zone.
        Returns (bid_wall_price, ask_wall_price) or None if no wall.
        """
        if not bids or not asks:
            return None, None
        
        # Calculate average level size
        all_sizes = [b.quantity for b in bids[:10]] + [a.quantity for a in asks[:10]]
        if not all_sizes:
            return None, None
        
        avg_size = np.mean(all_sizes)
        wall_threshold = avg_size * self.wall_threshold_mult
        
        # Price range to check
        danger_range = current_price * (self.danger_zone_bps / 10000)
        
        bid_wall = None
        ask_wall = None
        
        # Check for bid wall (support)
        for bid in bids[:10]:
            if bid.quantity >= wall_threshold:
                if abs(current_price - bid.price) <= danger_range:
                    bid_wall = bid.price
                    break
        
        # Check for ask wall (resistance)
        for ask in asks[:10]:
            if ask.quantity >= wall_threshold:
                if abs(ask.price - current_price) <= danger_range:
                    ask_wall = ask.price
                    break
        
        return bid_wall, ask_wall
    
    def check_wall_conflict(
        self,
        side: Side,
        bids: List[OrderBookLevel],
        asks: List[OrderBookLevel],
        current_price: float
    ) -> Tuple[bool, str]:
        """
        Check if a wall conflicts with the signal direction.
        Returns (is_safe, reason).
        """
        bid_wall, ask_wall = self.detect_walls(bids, asks, current_price)
        
        if side == Side.BUY and ask_wall:
            distance_bps = (ask_wall - current_price) / current_price * 10000
            return False, f"Ask wall at ${ask_wall:.2f} ({distance_bps:.1f}bps away)"
        
        if side == Side.SELL and bid_wall:
            distance_bps = (current_price - bid_wall) / current_price * 10000
            return False, f"Bid wall at ${bid_wall:.2f} ({distance_bps:.1f}bps away)"
        
        return True, "No conflicting walls"


class SpreadGuard:
    """
    Gate 5: Spread Constraint
    
    Block signals when spread is too wide.
    Wide spreads = predatory volatility traps.
    """
    
    def __init__(self, max_spread_bps: float = 3.0):
        self.max_spread_bps = max_spread_bps
    
    def check_spread(self, spread_bps: float) -> Tuple[bool, str]:
        """
        Check if spread is acceptable.
        Returns (is_valid, reason).
        """
        if spread_bps > self.max_spread_bps:
            return False, f"Spread {spread_bps:.2f}bps > max {self.max_spread_bps}bps"
        
        return True, f"Spread OK: {spread_bps:.2f}bps"


@dataclass
class GateResult:
    """Result of gate check."""
    passed: bool
    gate_name: str
    reason: str
    

class InstitutionalGatekeeper:
    """
    Master gatekeeper combining all 5 institutional-grade filters.
    
    Gates:
    1. Ladder Stability - filters spoofing
    2. VWAP Confluence - trade in mean territory
    3. Micro-CVD Agreement - flow confirmation
    4. Wall Detection - avoid liquidity traps
    5. Spread Guard - avoid wide spread traps
    """
    
    def __init__(
        self,
        # Ladder stability params
        ladder_depth: int = 3,
        ladder_stable_ms: int = 500,
        ladder_variance_pct: float = 0.20,
        
        # VWAP params
        vwap_window_seconds: int = 300,
        vwap_max_distance_bps: float = 15.0,
        
        # CVD params
        cvd_window_seconds: int = 60,
        
        # Wall detection params
        wall_threshold_mult: float = 4.0,
        wall_danger_zone_bps: float = 5.0,
        
        # Spread params
        max_spread_bps: float = 3.0,
        
        # Gate enables (for testing)
        enable_ladder: bool = True,
        enable_vwap: bool = True,
        enable_cvd: bool = True,
        enable_walls: bool = True,
        enable_spread: bool = True
    ):
        self.ladder_tracker = LadderStabilityTracker(
            depth=ladder_depth,
            min_stable_ms=ladder_stable_ms,
            max_variance_pct=ladder_variance_pct
        )
        
        self.vwap_tracker = VWAPTracker(
            window_seconds=vwap_window_seconds,
            max_distance_bps=vwap_max_distance_bps
        )
        
        self.cvd_tracker = MicroCVDTracker(
            window_seconds=cvd_window_seconds
        )
        
        self.wall_detector = WallDetector(
            wall_threshold_mult=wall_threshold_mult,
            danger_zone_bps=wall_danger_zone_bps
        )
        
        self.spread_guard = SpreadGuard(max_spread_bps=max_spread_bps)
        
        # Gate enables
        self.enable_ladder = enable_ladder
        self.enable_vwap = enable_vwap
        self.enable_cvd = enable_cvd
        self.enable_walls = enable_walls
        self.enable_spread = enable_spread
        
        # Stats
        self.checks_total = 0
        self.checks_passed = 0
        self.blocks_by_gate: Dict[str, int] = {
            "ladder": 0,
            "vwap": 0,
            "cvd": 0,
            "wall": 0,
            "spread": 0
        }

    def update(
        self,
        bids: List[OrderBookLevel],
        asks: List[OrderBookLevel],
        last_trade_price: float,
        last_trade_volume: float,
        last_trade_side: Optional[Side] = None
    ):
        """Update all trackers with new market data."""
        # Ladder stability
        self.ladder_tracker.update(bids, asks)
        
        # VWAP
        if last_trade_price > 0 and last_trade_volume > 0:
            self.vwap_tracker.update(last_trade_price, last_trade_volume)
        
        # CVD
        if last_trade_side and last_trade_price > 0:
            self.cvd_tracker.update(last_trade_price, last_trade_volume, last_trade_side)
    
    def check_all_gates(
        self,
        side: Side,
        bids: List[OrderBookLevel],
        asks: List[OrderBookLevel],
        current_price: float,
        spread_bps: float
    ) -> Tuple[bool, List[GateResult]]:
        """
        Run all institutional gates.
        Returns (all_passed, list of results).
        """
        self.checks_total += 1
        results = []
        all_passed = True
        
        # Gate 1: Ladder Stability
        if self.enable_ladder:
            passed, reason = self.ladder_tracker.check_stability(side)
            results.append(GateResult(passed, "LADDER", reason))
            if not passed:
                all_passed = False
                self.blocks_by_gate["ladder"] += 1
        
        # Gate 2: VWAP Confluence
        if self.enable_vwap:
            passed, reason, _ = self.vwap_tracker.check_confluence(current_price)
            results.append(GateResult(passed, "VWAP", reason))
            if not passed:
                all_passed = False
                self.blocks_by_gate["vwap"] += 1
        
        # Gate 3: CVD Agreement
        if self.enable_cvd:
            passed, reason = self.cvd_tracker.check_agreement(side)
            results.append(GateResult(passed, "CVD", reason))
            if not passed:
                all_passed = False
                self.blocks_by_gate["cvd"] += 1
        
        # Gate 4: Wall Detection
        if self.enable_walls:
            passed, reason = self.wall_detector.check_wall_conflict(
                side, bids, asks, current_price
            )
            results.append(GateResult(passed, "WALL", reason))
            if not passed:
                all_passed = False
                self.blocks_by_gate["wall"] += 1
        
        # Gate 5: Spread Guard
        if self.enable_spread:
            passed, reason = self.spread_guard.check_spread(spread_bps)
            results.append(GateResult(passed, "SPREAD", reason))
            if not passed:
                all_passed = False
                self.blocks_by_gate["spread"] += 1
        
        if all_passed:
            self.checks_passed += 1
        
        return all_passed, results
    
    def get_stats(self) -> Dict:
        """Get gatekeeper statistics."""
        pass_rate = (self.checks_passed / self.checks_total * 100) if self.checks_total > 0 else 0
        return {
            "total_checks": self.checks_total,
            "passed": self.checks_passed,
            "pass_rate": f"{pass_rate:.1f}%",
            "blocks_by_gate": self.blocks_by_gate,
            "vwap": self.vwap_tracker.get_vwap(),
            "cvd": self.cvd_tracker.get_cvd()
        }
    
    def format_results(self, results: List[GateResult]) -> str:
        """Format gate results for logging."""
        lines = []
        for r in results:
            status = "✓" if r.passed else "✗"
            lines.append(f"  [{status}] {r.gate_name}: {r.reason}")
        return "\n".join(lines)
