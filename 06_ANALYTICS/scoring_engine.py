import pandas as pd
import numpy as np
from dataclasses import dataclass, field

@dataclass
class WickFeatures:
    # Geometry
    wick_size_pct: float = 0.0
    body_size_pct: float = 0.0
    wick_to_body_ratio: float = 0.0
    rejection_velocity: float = 0.0 # Proxy: size/time
    
    # Order Flow (Proxies)
    imbalance_trap_score: float = 0.0 # Proxy: RelVol
    
    # Liquidity (Proxies)
    l5_depth_bid: float = 0.0
    l5_depth_ask: float = 0.0
    depth_imbalance: float = 0.0
    
    # Derivatives
    oi_change_pct: float = 0.0
    
    # VWAP/Trend
    vwap_mean_reversion_score: float = 0.0 # Proxy: deviation from MA
    
    # Signals
    fresh_sd_zone_flag: bool = True
    delta_divergence_flag: bool = False
    absorption_flag: bool = False

@dataclass
class WickEvent:
    symbol: str
    wick_side: str # "upper" or "lower"
    price: float
    features: WickFeatures

class HardenedWickScorer:
    """
    Ported scoring logic from ALPHA system, adapted for Hardened 1m candles.
    """
    
    def extract_features(self, candle: pd.Series, history: pd.DataFrame) -> WickFeatures:
        """Calculate features from OHLCV data."""
        f = WickFeatures()
        
        # Geometry
        open_ = candle['open']
        close = candle['close']
        high = candle['high']
        low = candle['low']
        
        range_len = high - low
        body_len = abs(close - open_)
        
        if range_len == 0:
            return f
            
        f.body_size_pct = (body_len / open_) * 100
        
        # Determine side and size
        if high - max(open_, close) > min(open_, close) - low:
            # Upper wick dominance
            wick_len = high - max(open_, close)
        else:
            wick_len = min(open_, close) - low
            
        f.wick_size_pct = (wick_len / open_) * 100
        f.wick_to_body_ratio = wick_len / max(body_len, 0.00000001)
        
        # Velocity Proxy (Points per minute)
        # 1m candle = 60 seconds
        f.rejection_velocity = wick_len / 60.0 
        
        # Trap/Sweep Proxy (Relative Volume)
        vol = candle['volume']
        # Calculate avg volume of last 20 candles
        if len(history) > 20:
            avg_vol = history['volume'].iloc[-20:].mean()
            rel_vol = vol / max(avg_vol, 1)
            # Map RelVol 1.0 -> 50 score, 3.0 -> 100 score
            f.imbalance_trap_score = min(100, rel_vol * 33)
        else:
            f.imbalance_trap_score = 50.0
            
        # VWAP/Trend Proxy
        # Distance from 20 SMA
        if len(history) > 20:
            sma = history['close'].iloc[-20:].mean()
            dist_pct = abs(close - sma) / sma
            # 2% deviation = 100 score
            f.vwap_mean_reversion_score = min(100, (dist_pct / 0.02) * 100)
        
        # Liquidity Proxies (Neutral)
        f.l5_depth_bid = 50.0
        f.l5_depth_ask = 50.0
        
        return f

    def score_wick(self, wick: WickEvent) -> dict:
        """Compute Wick Magnet Score (0-100)"""
        f = wick.features
        scores = {}
        
        # 1. Virgin Status (15 pts) - Always fresh in this factory
        scores['virgin_status'] = 15
        
        # 2. Distance (20 pts) - Currently at price
        scores['distance'] = 20
        
        # 3. Approach Velocity (20 pts) - Scaled 0-20
        # Normalizing: 50 points/min = max score?
        # BTC Price ~90k. 50 pts is small.
        # Let's say 0.1% move in 1 min is fast.
        # 90k * 0.001 = 90 points.
        velocity_score = min(20, (f.rejection_velocity / 2.0) * 20) 
        scores['approach_velocity'] = velocity_score
        
        # 4. Sweep Probability (15 pts) - Based on Volume
        scores['sweep_probability'] = (f.imbalance_trap_score / 100) * 15
        
        # 5. Liquidity Density (10 pts) - Neutral
        scores['liquidity_density'] = 5
        
        # 6. VWAP Alignment (10 pts)
        if f.vwap_mean_reversion_score > 70:
            scores['vwap_alignment'] = 10
        elif f.vwap_mean_reversion_score > 40:
            scores['vwap_alignment'] = 5
        else:
            scores['vwap_alignment'] = 0
            
        # 7. OI Conviction (5 pts) - Neutral
        scores['oi_conviction'] = 0
        
        # 8. Age (5 pts) - New
        scores['age_maturity'] = 1
        
        total_score = sum(scores.values())
        
        return {
            "magnet_score": round(total_score, 2),
            "breakdown": scores,
            "confidence": self._compute_confidence(f, total_score)
        }

    def _compute_confidence(self, f: WickFeatures, score: float) -> float:
        conf = 50.0
        if f.imbalance_trap_score > 80: conf += 20 # High volume
        if score > 70: conf += 20
        return min(100.0, conf)
