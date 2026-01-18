"""
Pressure Map
One-glance summary: where are the objects? Open air or minefield?
"""

import json
from pathlib import Path
import pandas as pd

DATA_PATH = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\1m_Candles\BTC_USDT_SWAP_1m.parquet")
OBJECTS_DIR = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\Objects")


def get_current_price():
    df = pd.read_parquet(DATA_PATH)
    return df.iloc[-1]['close']


def load_all_prices():
    """Load all active object prices."""
    prices = []
    
    # Wicks
    p = OBJECTS_DIR / "wicks.json"
    if p.exists():
        with open(p) as f:
            for w in json.load(f).get('wicks', []):
                prices.append({'price': w['price'], 'type': 'WICK', 'score': w.get('combined_score', 50)})
    
    # Levels
    p = OBJECTS_DIR / "levels.json"
    if p.exists():
        with open(p) as f:
            for l in json.load(f).get('levels', []):
                prices.append({'price': l['price'], 'type': 'LEVEL', 'score': l.get('quality_score', 50)})
    
    # Boxes
    p = OBJECTS_DIR / "boxes.json"
    if p.exists():
        with open(p) as f:
            for b in json.load(f).get('boxes', []):
                if b['state'] == 'ACTIVE':
                    prices.append({'price': b['high'], 'type': 'BOX', 'score': b.get('combined_score', 50)})
                    prices.append({'price': b['low'], 'type': 'BOX', 'score': b.get('combined_score', 50)})
    
    # Origins
    p = OBJECTS_DIR / "origins.json"
    if p.exists():
        with open(p) as f:
            for o in json.load(f).get('origins', []):
                if o['state'] == 'ACTIVE':
                    prices.append({'price': o['zone_mid'], 'type': 'ORIGIN', 'score': o.get('combined_score', 50)})
    
    return prices


def calc_pressure(objects, current_price, atr=100):
    """Calculate pressure above and below current price."""
    
    above = [o for o in objects if o['price'] > current_price]
    below = [o for o in objects if o['price'] < current_price]
    
    # Count within ATR bands
    above_025 = [o for o in above if o['price'] <= current_price + atr * 0.25]
    above_050 = [o for o in above if o['price'] <= current_price + atr * 0.5]
    above_100 = [o for o in above if o['price'] <= current_price + atr]
    
    below_025 = [o for o in below if o['price'] >= current_price - atr * 0.25]
    below_050 = [o for o in below if o['price'] >= current_price - atr * 0.5]
    below_100 = [o for o in below if o['price'] >= current_price - atr]
    
    # Nearest objects
    above_sorted = sorted(above, key=lambda x: x['price'])
    below_sorted = sorted(below, key=lambda x: x['price'], reverse=True)
    
    nearest_above = above_sorted[:3] if above_sorted else []
    nearest_below = below_sorted[:3] if below_sorted else []
    
    return {
        'above': {
            'total': len(above),
            'within_025_atr': len(above_025),
            'within_050_atr': len(above_050),
            'within_100_atr': len(above_100),
            'nearest_3': [{'price': o['price'], 'type': o['type'], 'dist': o['price'] - current_price} for o in nearest_above],
        },
        'below': {
            'total': len(below),
            'within_025_atr': len(below_025),
            'within_050_atr': len(below_050),
            'within_100_atr': len(below_100),
            'nearest_3': [{'price': o['price'], 'type': o['type'], 'dist': current_price - o['price']} for o in nearest_below],
        }
    }


def main():
    print("Loading data...")
    current_price = get_current_price()
    objects = load_all_prices()
    
    if not objects:
        print("No objects found. Run the factory bots first.")
        return
    
    # Use median range as rough ATR
    df = pd.read_parquet(DATA_PATH)
    atr = (df['high'] - df['low']).median()
    
    pressure = calc_pressure(objects, current_price, atr)
    
    print(f"\n{'='*70}")
    print(f"  PRESSURE MAP")
    print(f"  Current: ${current_price:,.2f}  |  ATR: ${atr:.2f}")
    print(f"{'='*70}")
    
    print(f"\n  TOTAL OBJECTS: {len(objects)}")
    print(f"    Above price: {pressure['above']['total']}")
    print(f"    Below price: {pressure['below']['total']}")
    
    # Density assessment
    above_dense = pressure['above']['within_050_atr']
    below_dense = pressure['below']['within_050_atr']
    
    print(f"\n  DENSITY (within 0.5 ATR):")
    print(f"    Above: {above_dense} objects")
    print(f"    Below: {below_dense} objects")
    
    if above_dense > 5:
        print(f"    ^ RESISTANCE MINEFIELD")
    elif above_dense == 0:
        print(f"    ^ OPEN AIR ABOVE")
    
    if below_dense > 5:
        print(f"    v SUPPORT MINEFIELD")
    elif below_dense == 0:
        print(f"    v OPEN AIR BELOW")
    
    print(f"\n  NEAREST ABOVE:")
    for obj in pressure['above']['nearest_3']:
        print(f"    ${obj['price']:>10,.2f}  ({obj['type']:<6})  +${obj['dist']:>6,.0f}")
    if not pressure['above']['nearest_3']:
        print(f"    (none)")
    
    print(f"\n  NEAREST BELOW:")
    for obj in pressure['below']['nearest_3']:
        print(f"    ${obj['price']:>10,.2f}  ({obj['type']:<6})  -${obj['dist']:>6,.0f}")
    if not pressure['below']['nearest_3']:
        print(f"    (none)")
    
    # Bias
    if above_dense > below_dense + 2:
        bias = "SELL PRESSURE (more resistance)"
    elif below_dense > above_dense + 2:
        bias = "BUY PRESSURE (more support)"
    else:
        bias = "NEUTRAL"
    
    print(f"\n  BIAS: {bias}")
    print(f"{'='*70}")


if __name__ == '__main__':
    main()
