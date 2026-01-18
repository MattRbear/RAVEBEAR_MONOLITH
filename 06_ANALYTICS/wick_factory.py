"""
Wick Factory v2
Creates wick objects with freshness/quality ranking.
Lifecycle: UNTOUCHED -> TOUCHED -> SWEPT -> RETIRED
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import numpy as np

DATA_PATH = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\1m_Candles\BTC_USDT_SWAP_1m.parquet")
OUTPUT_PATH = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\Objects\wicks.json")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)


def load_wicks():
    if OUTPUT_PATH.exists():
        with open(OUTPUT_PATH) as f:
            return json.load(f)
    return {'wicks': [], 'retired': []}


def save_wicks(data):
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(data, f, indent=2)


def calc_freshness_score(ts_created, current_ts, distance_from_price, current_price):
    """
    Freshness score 0-100 based on:
    - Age (newer = fresher)
    - Distance (closer = more relevant)
    """
    # Age factor: decay over 24 hours
    age_minutes = (current_ts - ts_created) / 60000
    age_factor = max(0, 100 - (age_minutes / 14.4))  # 0 at 24hr
    
    # Distance factor: closer = higher score
    dist_pct = abs(distance_from_price) / current_price * 100
    if dist_pct <= 0.1:
        dist_factor = 100
    elif dist_pct <= 0.25:
        dist_factor = 80
    elif dist_pct <= 0.5:
        dist_factor = 60
    elif dist_pct <= 1.0:
        dist_factor = 40
    else:
        dist_factor = 20
    
    return round((age_factor * 0.4 + dist_factor * 0.6), 1)


def calc_quality_score(wick_len, body_len, volume, avg_volume):
    """
    Quality score 0-100 based on:
    - Wick length (bigger = more significant)
    - Wick/body ratio (higher = cleaner rejection)
    - Volume (higher = more conviction)
    """
    score = 30  # Base
    
    # Wick length
    if wick_len >= 100:
        score += 25
    elif wick_len >= 50:
        score += 20
    elif wick_len >= 30:
        score += 15
    elif wick_len >= 20:
        score += 10
    
    # Wick/body ratio
    if body_len > 0:
        ratio = wick_len / body_len
        if ratio >= 2:
            score += 25
        elif ratio >= 1:
            score += 15
        elif ratio >= 0.5:
            score += 10
    else:
        score += 20  # Doji with wick
    
    # Volume
    if volume > avg_volume * 2:
        score += 20
    elif volume > avg_volume * 1.5:
        score += 15
    elif volume > avg_volume:
        score += 10
    
    return min(100, score)


def find_untouched_wicks(df, min_wick=20.0):
    """Find untouched wicks with quality and freshness scoring."""
    
    n = len(df)
    wicks = []
    
    highs = df['high'].values
    lows = df['low'].values
    opens = df['open'].values
    closes = df['close'].values
    volumes = df['volume'].values
    ts = df['timestamp'].values
    
    current_price = closes[-1]
    current_ts = ts[-1]
    avg_volume = volumes.mean()
    
    # Running max/min from future
    run_max_h = np.maximum.accumulate(highs[::-1])[::-1]
    run_min_l = np.minimum.accumulate(lows[::-1])[::-1]
    
    for i in range(n - 1):
        o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        body_top = max(o, c)
        body_bot = min(o, c)
        body = abs(c - o)
        upper_wick = h - body_top
        lower_wick = body_bot - l
        
        # Upper wick: untouched if future max high never reached
        if upper_wick >= min_wick and run_max_h[i + 1] < h:
            distance = h - current_price
            quality = calc_quality_score(upper_wick, body, volumes[i], avg_volume)
            freshness = calc_freshness_score(ts[i], current_ts, distance, current_price)
            
            wicks.append({
                'id': f"WU_{int(ts[i])}",
                'type': 'UPPER_WICK',
                'price': float(h),
                'wick_len': float(upper_wick),
                'body_len': float(body),
                'ts_created': int(ts[i]),
                'datetime': datetime.fromtimestamp(ts[i]/1000, tz=timezone.utc).isoformat(),
                'quality_score': quality,
                'freshness_score': freshness,
                'combined_score': round((quality * 0.6 + freshness * 0.4), 1),
                'volume': float(volumes[i]),
                'state': 'UNTOUCHED',
                'distance': float(distance),
            })
        
        # Lower wick: untouched if future min low never reached
        if lower_wick >= min_wick and run_min_l[i + 1] > l:
            distance = l - current_price
            quality = calc_quality_score(lower_wick, body, volumes[i], avg_volume)
            freshness = calc_freshness_score(ts[i], current_ts, distance, current_price)
            
            wicks.append({
                'id': f"WL_{int(ts[i])}",
                'type': 'LOWER_WICK',
                'price': float(l),
                'wick_len': float(lower_wick),
                'body_len': float(body),
                'ts_created': int(ts[i]),
                'datetime': datetime.fromtimestamp(ts[i]/1000, tz=timezone.utc).isoformat(),
                'quality_score': quality,
                'freshness_score': freshness,
                'combined_score': round((quality * 0.6 + freshness * 0.4), 1),
                'volume': float(volumes[i]),
                'state': 'UNTOUCHED',
                'distance': float(distance),
            })
    
    return wicks


def main():
    print("Loading candles...")
    df = pd.read_parquet(DATA_PATH).sort_values('timestamp').reset_index(drop=True)
    print(f"Loaded {len(df)} candles")
    
    print("Finding untouched wicks with scoring...")
    wicks = find_untouched_wicks(df, min_wick=20.0)
    
    # Sort by combined score
    wicks = sorted(wicks, key=lambda x: x['combined_score'], reverse=True)
    
    data = {'wicks': wicks, 'retired': []}
    save_wicks(data)
    
    current_price = df.iloc[-1]['close']
    
    print(f"\n{'='*70}")
    print(f"  WICK FACTORY - UNTOUCHED WICKS")
    print(f"  Current Price: ${current_price:,.2f}")
    print(f"{'='*70}")
    print(f"  Total wicks found: {len(wicks)}")
    
    upper = [w for w in wicks if w['type'] == 'UPPER_WICK']
    lower = [w for w in wicks if w['type'] == 'LOWER_WICK']
    
    print(f"  Upper wicks: {len(upper)}")
    print(f"  Lower wicks: {len(lower)}")
    
    # Score distribution
    high_s = len([w for w in wicks if w['combined_score'] >= 70])
    med_s = len([w for w in wicks if 50 <= w['combined_score'] < 70])
    low_s = len([w for w in wicks if w['combined_score'] < 50])
    
    print(f"\n  SCORE DISTRIBUTION:")
    print(f"    High (70+):  {high_s}")
    print(f"    Medium:      {med_s}")
    print(f"    Low (<50):   {low_s}")
    
    # Top 10 by combined score
    print(f"\n  TOP 10 HIGHEST PRIORITY WICKS:")
    print(f"  {'TYPE':<12} {'PRICE':>12} {'SCORE':>7} {'QUAL':>6} {'FRESH':>6} {'DIST':>10}")
    print(f"  {'-'*60}")
    
    for w in wicks[:10]:
        dist = w['price'] - current_price
        direction = "^" if dist > 0 else "v"
        print(f"  {w['type']:<12} ${w['price']:>10,.2f} {w['combined_score']:>6.1f} {w['quality_score']:>6} {w['freshness_score']:>6.1f} {direction}${abs(dist):>8,.0f}")
    
    print(f"\n  Saved to: {OUTPUT_PATH}")


if __name__ == '__main__':
    main()
