"""
Box Factory
Detects compression/consolidation zones as tradeable objects.
Lifecycle: FORMING -> CONFIRMED -> BROKEN_UP/DOWN -> RETIRED
"""

import json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import numpy as np

DATA_PATH = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\1m_Candles\BTC_USDT_SWAP_1m.parquet")
OUTPUT_PATH = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\Objects\boxes.json")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)


def save_boxes(data):
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(data, f, indent=2)


def find_boxes(df, min_candles=10, max_range_pct=0.3):
    """Find consolidation boxes."""
    
    n = len(df)
    boxes = []
    
    highs = df['high'].values
    lows = df['low'].values
    closes = df['close'].values
    ts = df['timestamp'].values
    
    current_price = closes[-1]
    
    i = 0
    while i < n - min_candles:
        # Start a potential box
        box_start = i
        box_high = highs[i]
        box_low = lows[i]
        
        # Extend box while range stays tight
        j = i + 1
        while j < n:
            test_high = max(box_high, highs[j])
            test_low = min(box_low, lows[j])
            test_range_pct = (test_high - test_low) / test_low * 100
            
            if test_range_pct > max_range_pct:
                break
            
            box_high = test_high
            box_low = test_low
            j += 1
        
        box_len = j - i
        
        if box_len >= min_candles:
            box_range = box_high - box_low
            box_mid = (box_high + box_low) / 2
            
            # Check if box is broken
            state = 'ACTIVE'
            broken_ts = None
            broken_dir = None
            
            if j < n:
                for k in range(j, n):
                    if closes[k] > box_high:
                        state = 'BROKEN_UP'
                        broken_ts = int(ts[k])
                        broken_dir = 'UP'
                        break
                    elif closes[k] < box_low:
                        state = 'BROKEN_DOWN'
                        broken_ts = int(ts[k])
                        broken_dir = 'DOWN'
                        break
            
            # Tightness score: tighter = higher
            tightness = max(0, 100 - (box_range / current_price * 1000))
            
            # Duration score: longer consolidation = stronger
            duration_score = min(100, box_len * 2)
            
            boxes.append({
                'id': f"BOX_{int(ts[box_start])}",
                'high': float(box_high),
                'low': float(box_low),
                'mid': float(box_mid),
                'range': float(box_range),
                'range_pct': round((box_range / box_low) * 100, 3),
                'duration': box_len,
                'ts_start': int(ts[box_start]),
                'ts_end': int(ts[j - 1]),
                'datetime_start': datetime.fromtimestamp(ts[box_start]/1000, tz=timezone.utc).isoformat(),
                'tightness_score': round(tightness, 1),
                'duration_score': duration_score,
                'combined_score': round((tightness * 0.5 + duration_score * 0.5), 1),
                'state': state,
                'broken_dir': broken_dir,
                'broken_ts': broken_ts,
            })
            
            i = j  # Skip past this box
        else:
            i += 1
    
    return boxes


def main():
    print("Loading candles...")
    df = pd.read_parquet(DATA_PATH).sort_values('timestamp').reset_index(drop=True)
    print(f"Loaded {len(df)} candles")
    
    print("Finding consolidation boxes...")
    boxes = find_boxes(df, min_candles=10, max_range_pct=0.3)
    
    # Sort by combined score
    boxes = sorted(boxes, key=lambda x: x['combined_score'], reverse=True)
    
    data = {'boxes': boxes}
    save_boxes(data)
    
    current_price = df.iloc[-1]['close']
    
    print(f"\n{'='*70}")
    print(f"  BOX FACTORY - CONSOLIDATION ZONES")
    print(f"  Current Price: ${current_price:,.2f}")
    print(f"{'='*70}")
    print(f"  Total boxes found: {len(boxes)}")
    
    active = [b for b in boxes if b['state'] == 'ACTIVE']
    broken_up = [b for b in boxes if b['state'] == 'BROKEN_UP']
    broken_down = [b for b in boxes if b['state'] == 'BROKEN_DOWN']
    
    print(f"  Active:     {len(active)}")
    print(f"  Broken Up:  {len(broken_up)}")
    print(f"  Broken Down:{len(broken_down)}")
    
    # Show active boxes near price
    if active:
        print(f"\n  ACTIVE BOXES (sorted by score):")
        print(f"  {'HIGH':>12} {'LOW':>12} {'RANGE':>8} {'DUR':>5} {'SCORE':>7}")
        print(f"  {'-'*50}")
        
        for b in active[:10]:
            print(f"  ${b['high']:>10,.2f} ${b['low']:>10,.2f} ${b['range']:>6,.0f} {b['duration']:>5} {b['combined_score']:>6.1f}")
    
    print(f"\n  Saved to: {OUTPUT_PATH}")


if __name__ == '__main__':
    main()
