"""
Level Factory v2
Creates poor high/low objects with quality scoring.
Lifecycle: ACTIVE -> TOUCHED -> CROSSED -> RETIRED
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import numpy as np

DATA_PATH = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\1m_Candles\BTC_USDT_SWAP_1m.parquet")
OUTPUT_PATH = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\Objects\levels.json")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)


def load_levels():
    if OUTPUT_PATH.exists():
        with open(OUTPUT_PATH) as f:
            return json.load(f)
    return {'levels': [], 'retired': []}


def save_levels(data):
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(data, f, indent=2)


def calc_quality_score(row, df, idx, level_type):
    """
    Quality score 0-100 based on:
    - Tightness (how clean the rejection)
    - Taps (how many times price tested nearby)
    - Symmetry (wick vs body ratio)
    - Volume (relative to average)
    """
    score = 50  # Base score
    
    o, h, l, c = row['open'], row['high'], row['low'], row['close']
    body = abs(c - o)
    body_top = max(o, c)
    body_bot = min(o, c)
    
    if level_type == 'HI':
        wick = h - body_top
        level_price = h
    else:
        wick = body_bot - l
        level_price = l
    
    # Tightness: smaller wick relative to body = cleaner rejection
    if body > 0:
        wick_ratio = wick / body
        if wick_ratio <= 0.1:
            score += 20  # Very clean
        elif wick_ratio <= 0.2:
            score += 10
        elif wick_ratio <= 0.3:
            score += 5
    
    # Volume: higher than average = more significant
    vol_avg = df['volume'].mean()
    if row['volume'] > vol_avg * 2:
        score += 15
    elif row['volume'] > vol_avg * 1.5:
        score += 10
    elif row['volume'] > vol_avg:
        score += 5
    
    # Nearby taps: count candles that came close (within $50) in last 100 candles
    lookback = df.iloc[max(0, idx-100):idx]
    if level_type == 'HI':
        taps = len(lookback[lookback['high'] >= level_price - 50])
    else:
        taps = len(lookback[lookback['low'] <= level_price + 50])
    
    if taps >= 3:
        score += 15  # Multiple tests = stronger level
    elif taps >= 2:
        score += 10
    elif taps >= 1:
        score += 5
    
    return min(100, max(0, score))


def find_poor_levels(df, lookback=3):
    """Find poor highs and lows with quality scoring."""
    
    n = len(df)
    levels = []
    
    highs = df['high'].values
    lows = df['low'].values
    opens = df['open'].values
    closes = df['close'].values
    ts = df['timestamp'].values
    
    # Future max close / min close for "uncleared" check
    run_max_c = np.maximum.accumulate(closes[::-1])[::-1]
    run_min_c = np.minimum.accumulate(closes[::-1])[::-1]
    
    for i in range(lookback, n - lookback - 1):
        o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        body = abs(c - o)
        body_top = max(o, c)
        body_bot = min(o, c)
        upper_wick = h - body_top
        lower_wick = body_bot - l
        
        if body == 0:
            continue
        
        # Poor High: swing high + small upper wick + never closed above
        is_swing_high = all(highs[j] < h for j in range(i - lookback, i + lookback + 1) if j != i)
        if is_swing_high and (upper_wick / body) <= 0.3 and run_max_c[i + 1] < h:
            quality = calc_quality_score(df.iloc[i], df, i, 'HI')
            levels.append({
                'id': f"PH_{int(ts[i])}",
                'type': 'POOR_HIGH',
                'price': float(h),
                'ts_created': int(ts[i]),
                'datetime': datetime.fromtimestamp(ts[i]/1000, tz=timezone.utc).isoformat(),
                'quality_score': quality,
                'wick_ratio': round(upper_wick / body, 3),
                'volume': float(df.iloc[i]['volume']),
                'state': 'ACTIVE',
                'ts_touched': None,
                'ts_retired': None,
            })
        
        # Poor Low: swing low + small lower wick + never closed below
        is_swing_low = all(lows[j] > l for j in range(i - lookback, i + lookback + 1) if j != i)
        if is_swing_low and (lower_wick / body) <= 0.3 and run_min_c[i + 1] > l:
            quality = calc_quality_score(df.iloc[i], df, i, 'LO')
            levels.append({
                'id': f"PL_{int(ts[i])}",
                'type': 'POOR_LOW',
                'price': float(l),
                'ts_created': int(ts[i]),
                'datetime': datetime.fromtimestamp(ts[i]/1000, tz=timezone.utc).isoformat(),
                'quality_score': quality,
                'wick_ratio': round(lower_wick / body, 3),
                'volume': float(df.iloc[i]['volume']),
                'state': 'ACTIVE',
                'ts_touched': None,
                'ts_retired': None,
            })
    
    return levels


def main():
    print("Loading candles...")
    df = pd.read_parquet(DATA_PATH).sort_values('timestamp').reset_index(drop=True)
    print(f"Loaded {len(df)} candles")
    
    print("Finding poor levels with quality scoring...")
    levels = find_poor_levels(df)
    
    # Sort by quality
    levels = sorted(levels, key=lambda x: x['quality_score'], reverse=True)
    
    data = {'levels': levels, 'retired': []}
    save_levels(data)
    
    current_price = df.iloc[-1]['close']
    
    print(f"\n{'='*70}")
    print(f"  LEVEL FACTORY - POOR HIGHS/LOWS")
    print(f"  Current Price: ${current_price:,.2f}")
    print(f"{'='*70}")
    print(f"  Total levels found: {len(levels)}")
    
    poor_hi = [l for l in levels if l['type'] == 'POOR_HIGH']
    poor_lo = [l for l in levels if l['type'] == 'POOR_LOW']
    
    print(f"  Poor Highs: {len(poor_hi)}")
    print(f"  Poor Lows:  {len(poor_lo)}")
    
    # Quality distribution
    high_q = len([l for l in levels if l['quality_score'] >= 70])
    med_q = len([l for l in levels if 50 <= l['quality_score'] < 70])
    low_q = len([l for l in levels if l['quality_score'] < 50])
    
    print(f"\n  QUALITY DISTRIBUTION:")
    print(f"    High (70+):  {high_q}")
    print(f"    Medium:      {med_q}")
    print(f"    Low (<50):   {low_q}")
    
    # Top 10 by quality
    print(f"\n  TOP 10 QUALITY LEVELS:")
    print(f"  {'TYPE':<12} {'PRICE':>12} {'QUALITY':>8} {'DIST':>10}")
    print(f"  {'-'*45}")
    
    for lvl in levels[:10]:
        dist = lvl['price'] - current_price
        direction = "above" if dist > 0 else "below"
        print(f"  {lvl['type']:<12} ${lvl['price']:>10,.2f} {lvl['quality_score']:>7} ${abs(dist):>8,.0f} {direction}")
    
    print(f"\n  Saved to: {OUTPUT_PATH}")


if __name__ == '__main__':
    main()
