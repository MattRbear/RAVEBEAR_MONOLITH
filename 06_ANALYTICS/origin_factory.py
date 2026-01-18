"""
Origin Zone Factory
Detects displacement/imbalance zones from big moves.
These are the "origin" candles where price launched from.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import numpy as np

DATA_PATH = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\1m_Candles\BTC_USDT_SWAP_1m.parquet")
OUTPUT_PATH = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\Objects\origins.json")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)


def save_origins(data):
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(data, f, indent=2)


def find_origin_zones(df, min_displacement_pct=0.25):
    """Find displacement zones from large candles."""
    
    n = len(df)
    origins = []
    
    highs = df['high'].values
    lows = df['low'].values
    opens = df['open'].values
    closes = df['close'].values
    volumes = df['volume'].values
    ts = df['timestamp'].values
    
    current_price = closes[-1]
    avg_range = (highs - lows).mean()
    avg_volume = volumes.mean()
    
    for i in range(n - 1):
        o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        candle_range = h - l
        body = abs(c - o)
        displacement_pct = (candle_range / o) * 100
        
        if displacement_pct < min_displacement_pct:
            continue
        
        direction = 'BULL' if c > o else 'BEAR'
        
        # Define the zone
        if direction == 'BULL':
            zone_high = max(o, c)
            zone_low = min(o, c)
        else:
            zone_high = max(o, c)
            zone_low = min(o, c)
        
        # Check if zone was revisited and held/failed
        state = 'ACTIVE'
        for j in range(i + 1, n):
            if direction == 'BULL':
                if lows[j] <= zone_high:  # Came back to zone
                    if closes[j] < zone_low:  # Failed
                        state = 'FAILED'
                    else:
                        state = 'HELD'
                    break
            else:
                if highs[j] >= zone_low:  # Came back to zone
                    if closes[j] > zone_high:  # Failed
                        state = 'FAILED'
                    else:
                        state = 'HELD'
                    break
        
        # Magnitude score
        magnitude = min(100, displacement_pct * 100)
        
        # Volume score
        vol_ratio = volumes[i] / avg_volume
        vol_score = min(100, vol_ratio * 30)
        
        origins.append({
            'id': f"OZ_{int(ts[i])}",
            'type': 'ORIGIN_ZONE',
            'direction': direction,
            'zone_high': float(zone_high),
            'zone_low': float(zone_low),
            'zone_mid': float((zone_high + zone_low) / 2),
            'displacement_pct': round(displacement_pct, 3),
            'ts_created': int(ts[i]),
            'datetime': datetime.fromtimestamp(ts[i]/1000, tz=timezone.utc).isoformat(),
            'magnitude_score': round(magnitude, 1),
            'volume_score': round(vol_score, 1),
            'combined_score': round((magnitude * 0.6 + vol_score * 0.4), 1),
            'state': state,
            'distance': float((zone_high + zone_low) / 2 - current_price),
        })
    
    return origins


def main():
    print("Loading candles...")
    df = pd.read_parquet(DATA_PATH).sort_values('timestamp').reset_index(drop=True)
    print(f"Loaded {len(df)} candles")
    
    print("Finding origin/displacement zones...")
    origins = find_origin_zones(df, min_displacement_pct=0.25)
    
    origins = sorted(origins, key=lambda x: x['combined_score'], reverse=True)
    
    data = {'origins': origins}
    save_origins(data)
    
    current_price = df.iloc[-1]['close']
    
    print(f"\n{'='*70}")
    print(f"  ORIGIN ZONE FACTORY")
    print(f"  Current Price: ${current_price:,.2f}")
    print(f"{'='*70}")
    print(f"  Total zones found: {len(origins)}")
    
    active = [o for o in origins if o['state'] == 'ACTIVE']
    held = [o for o in origins if o['state'] == 'HELD']
    failed = [o for o in origins if o['state'] == 'FAILED']
    
    print(f"  Active: {len(active)}")
    print(f"  Held:   {len(held)}")
    print(f"  Failed: {len(failed)}")
    
    if active:
        print(f"\n  ACTIVE ORIGIN ZONES:")
        print(f"  {'DIR':<5} {'ZONE HIGH':>12} {'ZONE LOW':>12} {'DISP%':>7} {'SCORE':>7}")
        print(f"  {'-'*50}")
        
        for o in active[:10]:
            print(f"  {o['direction']:<5} ${o['zone_high']:>10,.2f} ${o['zone_low']:>10,.2f} {o['displacement_pct']:>6.2f}% {o['combined_score']:>6.1f}")
    
    print(f"\n  Saved to: {OUTPUT_PATH}")


if __name__ == '__main__':
    main()
