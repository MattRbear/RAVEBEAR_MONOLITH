# file: detectors/wick_detector.py
from utils.aggregation import Candle
from typing import List, Dict, Literal

def detect_wick_events(candle: Candle, wick_min_ratio: float = 1.5) -> List[Dict]:
    """
    Analyze a candle and detect if it forms an upper or lower wick of significance.
    Returns a list of dicts describing the events (wick side, etc).
    """
    events = []
    
    range_ = candle.high - candle.low
    if range_ == 0:
        return events

    body_top = max(candle.open, candle.close)
    body_bottom = min(candle.open, candle.close)
    body_size = max(body_top - body_bottom, 0.00000001) # Avoid div/0
    
    upper_wick_size = candle.high - body_top
    lower_wick_size = body_bottom - candle.low
    
    # Check Upper Wick
    if (upper_wick_size / body_size) >= wick_min_ratio:
        events.append({
            "side": "upper",
            "wick_high": candle.high,
            "wick_low": body_top,
            "candle": candle
        })

    # Check Lower Wick
    if (lower_wick_size / body_size) >= wick_min_ratio:
        events.append({
            "side": "lower",
            "wick_high": body_bottom,
            "wick_low": candle.low,
            "candle": candle
        })
    
    return events
