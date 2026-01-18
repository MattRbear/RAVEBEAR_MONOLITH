"""
RaveBear Candle Pattern Detector
Detects candle patterns at level touches for entry filtering.
Logs: 3 candles before, touch candle, 3 candles after
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict

try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("pip install pandas numpy")
    exit(1)

CANDLE_PATH = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\1m_Candles\BTC_USDT_SWAP_1m.parquet")
PATTERN_LOG_PATH = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\pattern_touches.json")


# ============================================================================
# CANDLE CLASSIFICATION
# ============================================================================

def classify_candle(o: float, h: float, l: float, c: float) -> Dict:
    """Classify a single candle's characteristics."""
    body = abs(c - o)
    total_range = h - l
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l
    
    if total_range == 0:
        return {'type': 'DOJI', 'body_pct': 0, 'upper_wick_pct': 0, 'lower_wick_pct': 0}
    
    body_pct = (body / total_range) * 100
    upper_wick_pct = (upper_wick / total_range) * 100
    lower_wick_pct = (lower_wick / total_range) * 100
    
    is_bullish = c > o
    
    # Pattern detection
    pattern = 'NORMAL'
    
    # Doji (tiny body)
    if body_pct < 10:
        if upper_wick_pct > 40 and lower_wick_pct > 40:
            pattern = 'DOJI_STAR'
        elif upper_wick_pct > 60:
            pattern = 'GRAVESTONE_DOJI'
        elif lower_wick_pct > 60:
            pattern = 'DRAGONFLY_DOJI'
        else:
            pattern = 'DOJI'
    
    # Hammer / Hanging Man (small body, long lower wick)
    elif lower_wick_pct > 60 and upper_wick_pct < 15:
        pattern = 'HAMMER' if is_bullish else 'HANGING_MAN'
    
    # Inverted Hammer / Shooting Star (small body, long upper wick)
    elif upper_wick_pct > 60 and lower_wick_pct < 15:
        pattern = 'INVERTED_HAMMER' if is_bullish else 'SHOOTING_STAR'
    
    # Marubozu (full body, no wicks)
    elif body_pct > 90:
        pattern = 'MARUBOZU_BULL' if is_bullish else 'MARUBOZU_BEAR'
    
    # Strong candle (large body)
    elif body_pct > 70:
        pattern = 'STRONG_BULL' if is_bullish else 'STRONG_BEAR'
    
    return {
        'type': pattern,
        'is_bullish': is_bullish,
        'body_pct': round(body_pct, 1),
        'upper_wick_pct': round(upper_wick_pct, 1),
        'lower_wick_pct': round(lower_wick_pct, 1),
        'body_size': round(body, 2),
        'range': round(total_range, 2),
    }


def detect_engulfing(prev_candle: Dict, curr_candle: Dict, prev_o: float, prev_c: float, curr_o: float, curr_c: float) -> Optional[str]:
    """Detect engulfing pattern between two candles."""
    prev_bull = prev_c > prev_o
    curr_bull = curr_c > curr_o
    
    # Bullish engulfing: prev bearish, curr bullish, curr body engulfs prev
    if not prev_bull and curr_bull:
        if curr_o <= prev_c and curr_c >= prev_o:
            return 'BULLISH_ENGULFING'
    
    # Bearish engulfing: prev bullish, curr bearish, curr body engulfs prev
    if prev_bull and not curr_bull:
        if curr_o >= prev_c and curr_c <= prev_o:
            return 'BEARISH_ENGULFING'
    
    return None


def calculate_momentum(candles: List[Dict]) -> Dict:
    """Calculate momentum from a series of candles."""
    if not candles:
        return {'direction': 'NEUTRAL', 'strength': 0, 'avg_body': 0}
    
    total_move = 0
    total_body = 0
    bull_count = 0
    
    for c in candles:
        if c.get('is_bullish'):
            total_move += c.get('body_size', 0)
            bull_count += 1
        else:
            total_move -= c.get('body_size', 0)
        total_body += c.get('body_size', 0)
    
    avg_body = total_body / len(candles) if candles else 0
    
    if bull_count > len(candles) * 0.6:
        direction = 'BULLISH'
    elif bull_count < len(candles) * 0.4:
        direction = 'BEARISH'
    else:
        direction = 'MIXED'
    
    # Strength: how consistent is the momentum
    strength = abs(total_move) / (total_body + 0.01) * 100
    
    return {
        'direction': direction,
        'strength': round(min(100, strength), 1),
        'avg_body': round(avg_body, 2),
        'net_move': round(total_move, 2),
        'bull_ratio': round(bull_count / len(candles) * 100, 1) if candles else 0,
    }


# ============================================================================
# VOLUME ANALYSIS
# ============================================================================

def analyze_volume_cluster(volumes: List[float], avg_volume: float) -> Dict:
    """Analyze volume characteristics around a level touch."""
    if not volumes or avg_volume == 0:
        return {'cluster_type': 'UNKNOWN', 'volume_ratio': 0}
    
    touch_vol = volumes[len(volumes) // 2] if volumes else 0
    vol_ratio = touch_vol / avg_volume
    
    max_vol = max(volumes)
    max_idx = volumes.index(max_vol)
    
    # Volume spike location
    if max_idx < len(volumes) // 3:
        spike_location = 'PRE_TOUCH'
    elif max_idx > len(volumes) * 2 // 3:
        spike_location = 'POST_TOUCH'
    else:
        spike_location = 'AT_TOUCH'
    
    # Cluster classification
    if vol_ratio > 3:
        cluster_type = 'INSTITUTIONAL'  # Big boys
    elif vol_ratio > 2:
        cluster_type = 'SIGNIFICANT'
    elif vol_ratio > 1.5:
        cluster_type = 'ELEVATED'
    elif vol_ratio < 0.5:
        cluster_type = 'THIN'  # Low liquidity, dangerous
    else:
        cluster_type = 'NORMAL'
    
    return {
        'cluster_type': cluster_type,
        'volume_ratio': round(vol_ratio, 2),
        'spike_location': spike_location,
        'max_volume': round(max_vol, 2),
        'touch_volume': round(touch_vol, 2),
        'avg_cluster_volume': round(sum(volumes) / len(volumes), 2),
    }


# ============================================================================
# PATTERN TOUCH LOGGER
# ============================================================================

@dataclass
class PatternTouch:
    """Complete pattern data for a level touch."""
    timestamp: int
    datetime_str: str
    level_price: float
    level_type: str  # WICK_UP, WICK_DN, POOR_HI, POOR_LO
    touch_price: float
    
    # Pre-touch analysis (3 candles before)
    pre_momentum: Dict
    pre_candles: List[Dict]
    
    # Touch candle
    touch_candle: Dict
    engulfing_pattern: Optional[str]
    
    # Post-touch analysis (3 candles after)
    post_momentum: Dict
    post_candles: List[Dict]
    
    # Volume analysis
    volume_cluster: Dict
    
    # Outcome (filled later)
    outcome_5m: Optional[Dict] = None
    outcome_15m: Optional[Dict] = None
    outcome_30m: Optional[Dict] = None


class PatternTouchLogger:
    """Logs pattern data at level touches."""
    
    def __init__(self):
        self.touches: List[PatternTouch] = []
        self.df = None
        self.avg_volume = 0
        self.load_candles()
        self.load_existing()
    
    def load_candles(self):
        """Load candle data."""
        if CANDLE_PATH.exists():
            self.df = pd.read_parquet(CANDLE_PATH).sort_values('timestamp').reset_index(drop=True)
            # Calculate average volume (last 1000 candles)
            if len(self.df) > 1000:
                self.avg_volume = self.df.tail(1000)['volume'].mean()
            else:
                self.avg_volume = self.df['volume'].mean()
    
    def load_existing(self):
        """Load existing touch patterns."""
        if PATTERN_LOG_PATH.exists():
            try:
                with open(PATTERN_LOG_PATH) as f:
                    data = json.load(f)
                    # Don't convert back to dataclass, just keep as dict list
                    self.touches = data
            except:
                self.touches = []
    
    def save(self):
        """Save touches to file."""
        with open(PATTERN_LOG_PATH, 'w') as f:
            json.dump(self.touches, f, indent=2, default=str)
    
    def find_candle_index(self, timestamp: int) -> Optional[int]:
        """Find candle index closest to timestamp."""
        if self.df is None:
            return None
        
        # Find closest timestamp
        idx = (self.df['timestamp'] - timestamp).abs().idxmin()
        return idx
    
    def analyze_touch(self, level_price: float, level_type: str, touch_timestamp: int) -> Optional[Dict]:
        """Analyze a level touch and return pattern data."""
        if self.df is None:
            return None
        
        # Find touch candle index
        touch_idx = self.find_candle_index(touch_timestamp)
        if touch_idx is None or touch_idx < 3 or touch_idx >= len(self.df) - 3:
            return None
        
        # Get candle data
        pre_candles_raw = []
        post_candles_raw = []
        volumes = []
        
        # Pre-touch (3 candles before)
        for i in range(touch_idx - 3, touch_idx):
            row = self.df.iloc[i]
            candle_info = classify_candle(row['open'], row['high'], row['low'], row['close'])
            candle_info['timestamp'] = int(row['timestamp'])
            candle_info['volume'] = float(row['volume'])
            pre_candles_raw.append(candle_info)
            volumes.append(row['volume'])
        
        # Touch candle
        touch_row = self.df.iloc[touch_idx]
        touch_candle = classify_candle(touch_row['open'], touch_row['high'], touch_row['low'], touch_row['close'])
        touch_candle['timestamp'] = int(touch_row['timestamp'])
        touch_candle['volume'] = float(touch_row['volume'])
        volumes.append(touch_row['volume'])
        
        # Post-touch (3 candles after)
        for i in range(touch_idx + 1, min(touch_idx + 4, len(self.df))):
            row = self.df.iloc[i]
            candle_info = classify_candle(row['open'], row['high'], row['low'], row['close'])
            candle_info['timestamp'] = int(row['timestamp'])
            candle_info['volume'] = float(row['volume'])
            post_candles_raw.append(candle_info)
            volumes.append(row['volume'])
        
        # Detect engulfing
        prev_row = self.df.iloc[touch_idx - 1]
        engulfing = detect_engulfing(
            pre_candles_raw[-1], touch_candle,
            prev_row['open'], prev_row['close'],
            touch_row['open'], touch_row['close']
        )
        
        # Calculate momentum
        pre_momentum = calculate_momentum(pre_candles_raw)
        post_momentum = calculate_momentum(post_candles_raw)
        
        # Volume cluster
        volume_cluster = analyze_volume_cluster(volumes, self.avg_volume)
        
        # Calculate outcomes
        outcomes = {}
        for mins, label in [(5, '5m'), (15, '15m'), (30, '30m')]:
            end_idx = touch_idx + mins
            if end_idx < len(self.df):
                end_row = self.df.iloc[end_idx]
                touch_close = touch_row['close']
                end_close = end_row['close']
                change = end_close - touch_close
                change_pct = (change / touch_close) * 100
                
                # For support (DN levels): bounce = price went UP
                # For resistance (UP levels): bounce = price went DOWN
                if level_type in ('WICK_DN', 'POOR_LO'):
                    bounced = change > 0
                else:
                    bounced = change < 0
                
                outcomes[label] = {
                    'change': round(change, 2),
                    'change_pct': round(change_pct, 4),
                    'bounced': bounced,
                    'end_price': round(end_close, 2),
                }
        
        # Build touch record
        touch_data = {
            'timestamp': int(touch_timestamp),
            'datetime_str': datetime.fromtimestamp(touch_timestamp/1000, tz=timezone.utc).isoformat(),
            'level_price': round(level_price, 2),
            'level_type': level_type,
            'touch_price': round(touch_row['close'], 2),
            'pre_momentum': pre_momentum,
            'pre_candles': pre_candles_raw,
            'touch_candle': touch_candle,
            'engulfing_pattern': engulfing,
            'post_momentum': post_momentum,
            'post_candles': post_candles_raw,
            'volume_cluster': volume_cluster,
            'outcome_5m': outcomes.get('5m'),
            'outcome_15m': outcomes.get('15m'),
            'outcome_30m': outcomes.get('30m'),
        }
        
        self.touches.append(touch_data)
        self.save()
        
        return touch_data
    
    def get_pattern_stats(self) -> Dict:
        """Calculate statistics on pattern performance."""
        if not self.touches:
            return {}
        
        stats = {
            'total_touches': len(self.touches),
            'by_pattern': {},
            'by_volume_cluster': {},
            'by_momentum': {},
            'by_engulfing': {},
        }
        
        # Group by touch candle pattern
        pattern_outcomes = {}
        for t in self.touches:
            pattern = t.get('touch_candle', {}).get('type', 'UNKNOWN')
            if pattern not in pattern_outcomes:
                pattern_outcomes[pattern] = {'count': 0, 'bounces_15m': 0}
            pattern_outcomes[pattern]['count'] += 1
            if t.get('outcome_15m', {}).get('bounced'):
                pattern_outcomes[pattern]['bounces_15m'] += 1
        
        for pattern, data in pattern_outcomes.items():
            if data['count'] >= 3:
                stats['by_pattern'][pattern] = {
                    'count': data['count'],
                    'bounce_rate': round(data['bounces_15m'] / data['count'] * 100, 1),
                }
        
        # Group by volume cluster
        vol_outcomes = {}
        for t in self.touches:
            cluster = t.get('volume_cluster', {}).get('cluster_type', 'UNKNOWN')
            if cluster not in vol_outcomes:
                vol_outcomes[cluster] = {'count': 0, 'bounces_15m': 0}
            vol_outcomes[cluster]['count'] += 1
            if t.get('outcome_15m', {}).get('bounced'):
                vol_outcomes[cluster]['bounces_15m'] += 1
        
        for cluster, data in vol_outcomes.items():
            if data['count'] >= 3:
                stats['by_volume_cluster'][cluster] = {
                    'count': data['count'],
                    'bounce_rate': round(data['bounces_15m'] / data['count'] * 100, 1),
                }
        
        # Group by pre-touch momentum
        mom_outcomes = {}
        for t in self.touches:
            mom = t.get('pre_momentum', {}).get('direction', 'UNKNOWN')
            if mom not in mom_outcomes:
                mom_outcomes[mom] = {'count': 0, 'bounces_15m': 0}
            mom_outcomes[mom]['count'] += 1
            if t.get('outcome_15m', {}).get('bounced'):
                mom_outcomes[mom]['bounces_15m'] += 1
        
        for mom, data in mom_outcomes.items():
            if data['count'] >= 3:
                stats['by_momentum'][mom] = {
                    'count': data['count'],
                    'bounce_rate': round(data['bounces_15m'] / data['count'] * 100, 1),
                }
        
        # Engulfing patterns
        eng_outcomes = {'with_engulfing': {'count': 0, 'bounces': 0}, 'no_engulfing': {'count': 0, 'bounces': 0}}
        for t in self.touches:
            key = 'with_engulfing' if t.get('engulfing_pattern') else 'no_engulfing'
            eng_outcomes[key]['count'] += 1
            if t.get('outcome_15m', {}).get('bounced'):
                eng_outcomes[key]['bounces'] += 1
        
        for key, data in eng_outcomes.items():
            if data['count'] >= 3:
                stats['by_engulfing'][key] = {
                    'count': data['count'],
                    'bounce_rate': round(data['bounces'] / data['count'] * 100, 1),
                }
        
        return stats


def print_pattern_stats(stats: Dict):
    """Print pattern statistics."""
    print("\n" + "=" * 70)
    print("  PATTERN TOUCH ANALYSIS")
    print("=" * 70)
    
    print(f"\nTotal Touches Logged: {stats.get('total_touches', 0)}")
    
    print("\n📊 BOUNCE RATE BY TOUCH CANDLE PATTERN:")
    print("-" * 50)
    for pattern, data in sorted(stats.get('by_pattern', {}).items(), key=lambda x: x[1]['bounce_rate'], reverse=True):
        bar = "█" * int(data['bounce_rate'] / 5)
        print(f"  {pattern:<20} {data['count']:>4} touches  {data['bounce_rate']:>5.1f}% {bar}")
    
    print("\n💰 BOUNCE RATE BY VOLUME CLUSTER:")
    print("-" * 50)
    for cluster, data in sorted(stats.get('by_volume_cluster', {}).items(), key=lambda x: x[1]['bounce_rate'], reverse=True):
        bar = "█" * int(data['bounce_rate'] / 5)
        emoji = {"INSTITUTIONAL": "🐋", "SIGNIFICANT": "📈", "ELEVATED": "📊", "NORMAL": "➖", "THIN": "⚠️"}.get(cluster, "")
        print(f"  {emoji} {cluster:<15} {data['count']:>4} touches  {data['bounce_rate']:>5.1f}% {bar}")
    
    print("\n📈 BOUNCE RATE BY PRE-TOUCH MOMENTUM:")
    print("-" * 50)
    for mom, data in sorted(stats.get('by_momentum', {}).items(), key=lambda x: x[1]['bounce_rate'], reverse=True):
        bar = "█" * int(data['bounce_rate'] / 5)
        print(f"  {mom:<15} {data['count']:>4} touches  {data['bounce_rate']:>5.1f}% {bar}")
    
    print("\n🔄 ENGULFING PATTERN IMPACT:")
    print("-" * 50)
    for key, data in stats.get('by_engulfing', {}).items():
        bar = "█" * int(data['bounce_rate'] / 5)
        print(f"  {key:<20} {data['count']:>4} touches  {data['bounce_rate']:>5.1f}% {bar}")


def main():
    """Run pattern analysis on existing data."""
    logger = PatternTouchLogger()
    
    print("=" * 70)
    print("  RAVEBEAR PATTERN TOUCH ANALYZER")
    print("=" * 70)
    
    if not logger.touches:
        print("\nNo touches logged yet.")
        print("Touches will be logged automatically by the dashboard.")
        print(f"\nData will be saved to: {PATTERN_LOG_PATH}")
        return
    
    stats = logger.get_pattern_stats()
    print_pattern_stats(stats)
    
    print("\n" + "=" * 70)
    print("  KEY INSIGHTS")
    print("=" * 70)
    
    # Find best patterns
    best_pattern = max(stats.get('by_pattern', {}).items(), key=lambda x: x[1]['bounce_rate'], default=(None, None))
    best_volume = max(stats.get('by_volume_cluster', {}).items(), key=lambda x: x[1]['bounce_rate'], default=(None, None))
    
    if best_pattern[0]:
        print(f"\n🎯 BEST TOUCH CANDLE: {best_pattern[0]} ({best_pattern[1]['bounce_rate']}% bounce rate)")
    
    if best_volume[0]:
        print(f"🐋 BEST VOLUME CLUSTER: {best_volume[0]} ({best_volume[1]['bounce_rate']}% bounce rate)")
    
    print("\n💡 TRADING FILTER:")
    print("   Only enter when:")
    if best_pattern[0]:
        print(f"   ✓ Touch candle is {best_pattern[0]}")
    if best_volume[0]:
        print(f"   ✓ Volume cluster is {best_volume[0]}")


if __name__ == '__main__':
    main()
