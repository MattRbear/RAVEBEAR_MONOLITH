"""
MICROSTRUCTURE ENGINE - Alpha Module
=====================================
Microstructure math: OBI, VWMP, spread analysis, whale detection.
"""

from typing import List, Optional, Tuple
from data_types import OrderBookLevel, MicrostructureMetrics, Side
import numpy as np


class MicrostructureAlpha:
    """
    Calculates features based on market microstructure.
    This is where the edge lives.
    """
    
    def __init__(self, depth: int = 10, whale_threshold_mult: float = 5.0):
        self.depth = depth
        self.whale_threshold_mult = whale_threshold_mult
    
    def calculate_all(
        self, 
        bids: List[OrderBookLevel], 
        asks: List[OrderBookLevel],
        prev_imbalance: float = 0.0
    ) -> MicrostructureMetrics:
        """Calculate all microstructure metrics in one pass."""
        
        obi = self.get_order_book_imbalance(bids, asks)
        vwmp = self.get_weighted_mid_price(bids, asks)
        spread = self.get_spread_bps(bids, asks)
        
        bid_depth = sum(b.quantity for b in bids[:self.depth])
        ask_depth = sum(a.quantity for a in asks[:self.depth])
        bid_notional = sum(b.notional for b in bids[:self.depth])
        ask_notional = sum(a.notional for a in asks[:self.depth])
        
        vol_at_touch = (bids[0].quantity if bids else 0) + (asks[0].quantity if asks else 0)
        
        # Whale detection
        whale_detected, whale_side, whale_size = self.detect_large_order(bids, asks)
        
        # Imbalance gradient (momentum)
        gradient = obi - prev_imbalance
        
        return MicrostructureMetrics(
            order_book_imbalance=obi,
            weighted_mid_price=vwmp,
            spread_bps=spread,
            bid_depth_total=bid_depth,
            ask_depth_total=ask_depth,
            bid_depth_notional=bid_notional,
            ask_depth_notional=ask_notional,
            imbalance_gradient=gradient,
            volume_at_touch=vol_at_touch,
            large_order_detected=whale_detected,
            large_order_side=whale_side,
            large_order_size=whale_size
        )

    def get_order_book_imbalance(
        self, 
        bids: List[OrderBookLevel], 
        asks: List[OrderBookLevel]
    ) -> float:
        """
        Order Book Imbalance (OBI)
        Returns value between -1 (heavy sell pressure) and 1 (heavy buy pressure).
        
        Formula: (BidVol - AskVol) / (BidVol + AskVol)
        
        This is the PRIMARY alpha signal. When OBI > 0.3, expect upward pressure.
        When OBI < -0.3, expect downward pressure.
        """
        bid_vol = sum(b.quantity for b in bids[:self.depth])
        ask_vol = sum(a.quantity for a in asks[:self.depth])
        
        total = bid_vol + ask_vol
        if total == 0:
            return 0.0
        
        return (bid_vol - ask_vol) / total
    
    def get_weighted_mid_price(
        self, 
        bids: List[OrderBookLevel], 
        asks: List[OrderBookLevel]
    ) -> float:
        """
        Volume Weighted Mid Price (VWMP)
        
        Standard Mid = (BestBid + BestAsk) / 2
        VWMP accounts for WHERE the liquidity actually sits.
        
        If bids are heavy, the "true" price is pulled UP toward asks.
        If asks are heavy, the "true" price is pulled DOWN toward bids.
        
        This predicts where price WANTS to go based on liquidity distribution.
        """
        if not bids or not asks:
            return 0.0
        
        total_bid_vol = sum(b.quantity for b in bids[:self.depth])
        total_ask_vol = sum(a.quantity for a in asks[:self.depth])
        total = total_bid_vol + total_ask_vol
        
        if total == 0:
            return (bids[0].price + asks[0].price) / 2
        
        best_bid = bids[0].price
        best_ask = asks[0].price
        
        # Imbalance ratio used to skew mid price
        # Heavy bids = price pressure UP (toward asks)
        imbalance = total_bid_vol / total
        
        return (best_ask * imbalance) + (best_bid * (1 - imbalance))
    
    def get_spread_bps(
        self, 
        bids: List[OrderBookLevel], 
        asks: List[OrderBookLevel]
    ) -> float:
        """
        Spread in basis points.
        Tight spread = liquid market, signals more reliable.
        Wide spread = illiquid, be careful.
        """
        if not bids or not asks:
            return float('inf')
        
        best_bid = bids[0].price
        best_ask = asks[0].price
        spread = best_ask - best_bid
        
        return (spread / best_ask) * 10000

    def detect_large_order(
        self, 
        bids: List[OrderBookLevel], 
        asks: List[OrderBookLevel]
    ) -> Tuple[bool, Optional[Side], float]:
        """
        Detect whale orders that are significantly larger than average.
        
        A whale order at the top of book = strong directional signal.
        Market makers often put large orders as "walls" to manipulate.
        """
        if not bids or not asks:
            return False, None, 0.0
        
        # Calculate average size across top N levels
        all_sizes = [b.quantity for b in bids[:self.depth]] + [a.quantity for a in asks[:self.depth]]
        if not all_sizes:
            return False, None, 0.0
        
        avg_size = np.mean(all_sizes)
        threshold = avg_size * self.whale_threshold_mult
        
        # Check top 3 bid levels
        for level in bids[:3]:
            if level.quantity > threshold:
                return True, Side.BUY, level.quantity
        
        # Check top 3 ask levels
        for level in asks[:3]:
            if level.quantity > threshold:
                return True, Side.SELL, level.quantity
        
        return False, None, 0.0
    
    def get_depth_ratio(
        self, 
        bids: List[OrderBookLevel], 
        asks: List[OrderBookLevel],
        levels: int = 5
    ) -> float:
        """
        Ratio of bid depth to ask depth at specified levels.
        
        > 1 = more support (buyers)
        < 1 = more resistance (sellers)
        """
        bid_depth = sum(b.quantity for b in bids[:levels])
        ask_depth = sum(a.quantity for a in asks[:levels])
        
        if ask_depth == 0:
            return float('inf')
        
        return bid_depth / ask_depth
    
    def get_notional_imbalance(
        self, 
        bids: List[OrderBookLevel], 
        asks: List[OrderBookLevel]
    ) -> float:
        """
        Imbalance weighted by dollar value, not just quantity.
        More accurate for cross-asset comparison.
        """
        bid_notional = sum(b.notional for b in bids[:self.depth])
        ask_notional = sum(a.notional for a in asks[:self.depth])
        
        total = bid_notional + ask_notional
        if total == 0:
            return 0.0
        
        return (bid_notional - ask_notional) / total
    
    def get_price_impact_estimate(
        self, 
        bids: List[OrderBookLevel], 
        asks: List[OrderBookLevel],
        order_size: float,
        side: Side
    ) -> float:
        """
        Estimate price impact of a market order of given size.
        Used for execution planning.
        """
        if side == Side.BUY:
            levels = asks
        else:
            levels = bids
        
        if not levels:
            return 0.0
        
        remaining = order_size
        total_cost = 0.0
        
        for level in levels:
            if remaining <= 0:
                break
            
            fill_qty = min(remaining, level.quantity)
            total_cost += fill_qty * level.price
            remaining -= fill_qty
        
        if order_size == remaining:
            return float('inf')  # Not enough liquidity
        
        avg_fill = total_cost / (order_size - remaining)
        best_price = levels[0].price
        
        # Impact in basis points
        return abs(avg_fill - best_price) / best_price * 10000
