"""
Derivatives Correlation Tracker
Logs market state snapshots for correlation analysis.
Combines: Coinalyze (OI, funding, L/S, liqs) + Candle levels + CVD
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path
import sys

# Add paths for imports
sys.path.insert(0, str(Path(r"C:\Users\M.R Bear\Documents\Coin_anal")))
sys.path.insert(0, str(Path(r"C:\Users\M.R Bear\Documents\Candle_collector")))

from coinalyze_client import CoinalyzeClient

DATA_DIR = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\Correlations")
DATA_DIR.mkdir(parents=True, exist_ok=True)

SNAPSHOT_PATH = DATA_DIR / "market_snapshots.json"
EVENTS_PATH = DATA_DIR / "derivative_events.json"


def load_json(path):
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return []


def save_json(path, data):
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)


def get_btc_price():
    """Get current BTC price from candle data."""
    candle_path = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\1m_Candles\BTC_USDT_SWAP_1m.parquet")
    try:
        import pandas as pd
        df = pd.read_parquet(candle_path)
        return float(df.iloc[-1]['close'])
    except:
        return None


def get_cvd_state():
    """Get CVD state if available."""
    cvd_path = Path(r"C:\Users\M.R Bear\Documents\CVD DASH\data\cvd_state_v2.json")
    try:
        with open(cvd_path) as f:
            data = json.load(f)
            return {
                'cvd_total': float(data.get('cvd_total', 0)),
                'buy_volume': float(data.get('buy_volume', 0)),
                'sell_volume': float(data.get('sell_volume', 0)),
            }
    except:
        return None


def get_level_counts():
    """Get counts of active levels."""
    objects_dir = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\Objects")
    counts = {'wicks': 0, 'levels': 0, 'boxes': 0, 'stacks': 0}
    
    try:
        wicks_path = objects_dir / "wicks.json"
        if wicks_path.exists():
            with open(wicks_path) as f:
                counts['wicks'] = len(json.load(f).get('wicks', []))
        
        levels_path = objects_dir / "levels.json"
        if levels_path.exists():
            with open(levels_path) as f:
                counts['levels'] = len(json.load(f).get('levels', []))
        
        stacks_path = objects_dir / "stacks.json"
        if stacks_path.exists():
            with open(stacks_path) as f:
                counts['stacks'] = len(json.load(f).get('stacks', []))
    except:
        pass
    
    return counts


class DerivativesTracker:
    def __init__(self):
        self.client = CoinalyzeClient()
        self.snapshots = load_json(SNAPSHOT_PATH)
        self.events = load_json(EVENTS_PATH)
        self.last_snapshot = None
    
    def take_snapshot(self):
        """Take a full market state snapshot."""
        print("Taking market snapshot...")
        
        # Coinalyze data
        oi_data = self.client.get_open_interest()
        funding_data = self.client.get_funding_rate()
        pred_funding = self.client.get_predicted_funding()
        
        # Get latest L/S ratio
        ls_history = self.client.get_long_short_ratio_history(interval="1hour", hours=1)
        ls_latest = ls_history[-1] if ls_history else {}
        
        # Get recent liquidations
        liq_history = self.client.get_liquidation_history(interval="1hour", hours=4)
        recent_liq_long = sum(l.get('l', 0) for l in liq_history[-4:]) if liq_history else 0
        recent_liq_short = sum(l.get('s', 0) for l in liq_history[-4:]) if liq_history else 0
        
        # Local data
        btc_price = get_btc_price()
        cvd_state = get_cvd_state()
        level_counts = get_level_counts()
        
        snapshot = {
            'timestamp': int(time.time() * 1000),
            'datetime': datetime.now(timezone.utc).isoformat(),
            
            # Price
            'btc_price': btc_price,
            
            # Open Interest
            'oi_value': oi_data[0].get('value', 0) if oi_data else 0,
            
            # Funding
            'funding_rate': funding_data[0].get('value', 0) if funding_data else 0,
            'predicted_funding': pred_funding[0].get('value', 0) if pred_funding else 0,
            
            # Long/Short Ratio
            'long_pct': ls_latest.get('l', 50) if isinstance(ls_latest, dict) else 50,
            'short_pct': ls_latest.get('s', 50) if isinstance(ls_latest, dict) else 50,
            'ls_ratio': ls_latest.get('r', 1) if isinstance(ls_latest, dict) else 1,
            
            # Liquidations (4h)
            'liq_long_4h': recent_liq_long,
            'liq_short_4h': recent_liq_short,
            'liq_total_4h': recent_liq_long + recent_liq_short,
            
            # CVD (if available)
            'cvd_total': cvd_state.get('cvd_total') if cvd_state else None,
            'cvd_buy_vol': cvd_state.get('buy_volume') if cvd_state else None,
            'cvd_sell_vol': cvd_state.get('sell_volume') if cvd_state else None,
            
            # Level counts
            'active_wicks': level_counts['wicks'],
            'active_levels': level_counts['levels'],
            'active_stacks': level_counts['stacks'],
        }
        
        # Detect regime
        snapshot['regime'] = self.classify_regime(snapshot)
        
        # Detect events
        events = self.detect_events(snapshot)
        if events:
            for event in events:
                self.events.append(event)
            save_json(EVENTS_PATH, self.events[-1000:])  # Keep last 1000 events
        
        self.snapshots.append(snapshot)
        save_json(SNAPSHOT_PATH, self.snapshots[-500:])  # Keep last 500 snapshots
        
        self.last_snapshot = snapshot
        return snapshot
    
    def classify_regime(self, snapshot):
        """Classify current market regime."""
        funding = snapshot.get('funding_rate', 0)
        long_pct = snapshot.get('long_pct', 50)
        
        # Funding regime
        if funding > 0.0005:  # 0.05%
            funding_regime = "HIGH_FUNDING_LONG"
        elif funding < -0.0005:
            funding_regime = "HIGH_FUNDING_SHORT"
        elif funding > 0.0001:
            funding_regime = "POSITIVE_FUNDING"
        elif funding < -0.0001:
            funding_regime = "NEGATIVE_FUNDING"
        else:
            funding_regime = "NEUTRAL_FUNDING"
        
        # Positioning regime
        if long_pct > 65:
            position_regime = "LONG_HEAVY"
        elif long_pct < 35:
            position_regime = "SHORT_HEAVY"
        elif long_pct > 55:
            position_regime = "LONG_BIAS"
        elif long_pct < 45:
            position_regime = "SHORT_BIAS"
        else:
            position_regime = "BALANCED"
        
        return f"{funding_regime}|{position_regime}"
    
    def detect_events(self, snapshot):
        """Detect notable events by comparing to previous snapshot."""
        events = []
        
        if not self.last_snapshot:
            return events
        
        prev = self.last_snapshot
        now = snapshot
        ts = now['datetime']
        
        # Price move
        if prev.get('btc_price') and now.get('btc_price'):
            price_change = now['btc_price'] - prev['btc_price']
            price_pct = (price_change / prev['btc_price']) * 100
            
            if abs(price_pct) > 0.5:  # 0.5% move
                events.append({
                    'timestamp': ts,
                    'type': 'PRICE_MOVE',
                    'direction': 'UP' if price_change > 0 else 'DOWN',
                    'magnitude': abs(price_pct),
                    'price': now['btc_price'],
                    'regime': now['regime'],
                })
        
        # OI change
        if prev.get('oi_value') and now.get('oi_value'):
            oi_change = now['oi_value'] - prev['oi_value']
            oi_pct = (oi_change / prev['oi_value']) * 100
            
            if abs(oi_pct) > 1:  # 1% OI change
                events.append({
                    'timestamp': ts,
                    'type': 'OI_CHANGE',
                    'direction': 'INCREASE' if oi_change > 0 else 'DECREASE',
                    'magnitude': abs(oi_pct),
                    'oi_value': now['oi_value'],
                    'regime': now['regime'],
                })
        
        # Funding flip
        if prev.get('funding_rate') and now.get('funding_rate'):
            if (prev['funding_rate'] > 0 and now['funding_rate'] < 0) or \
               (prev['funding_rate'] < 0 and now['funding_rate'] > 0):
                events.append({
                    'timestamp': ts,
                    'type': 'FUNDING_FLIP',
                    'from': prev['funding_rate'],
                    'to': now['funding_rate'],
                    'regime': now['regime'],
                })
        
        # L/S ratio shift
        if prev.get('long_pct') and now.get('long_pct'):
            ls_change = now['long_pct'] - prev['long_pct']
            
            if abs(ls_change) > 2:  # 2% shift
                events.append({
                    'timestamp': ts,
                    'type': 'LS_SHIFT',
                    'direction': 'LONGS_INCREASING' if ls_change > 0 else 'SHORTS_INCREASING',
                    'magnitude': abs(ls_change),
                    'long_pct': now['long_pct'],
                    'regime': now['regime'],
                })
        
        # Liquidation spike
        liq_total = now.get('liq_total_4h', 0)
        if liq_total > 1_000_000:  # $1M+ in 4h
            liq_long = now.get('liq_long_4h', 0)
            liq_short = now.get('liq_short_4h', 0)
            
            events.append({
                'timestamp': ts,
                'type': 'LIQUIDATION_SPIKE',
                'total': liq_total,
                'long_liqs': liq_long,
                'short_liqs': liq_short,
                'dominant': 'LONGS' if liq_long > liq_short else 'SHORTS',
                'regime': now['regime'],
            })
        
        return events
    
    def print_snapshot(self, snapshot):
        """Print snapshot summary."""
        print(f"\n{'='*60}")
        print(f"  MARKET SNAPSHOT - {snapshot['datetime'][:19]}")
        print(f"{'='*60}")
        
        print(f"  BTC Price:     ${snapshot.get('btc_price', 0):>12,.2f}")
        print(f"  Open Interest: ${snapshot.get('oi_value', 0)/1e9:>12.2f}B")
        print(f"  Funding Rate:  {snapshot.get('funding_rate', 0)*100:>12.4f}%")
        print(f"  Long/Short:    {snapshot.get('long_pct', 50):>11.1f}% / {snapshot.get('short_pct', 50):.1f}%")
        print(f"  Liqs (4h):     ${snapshot.get('liq_total_4h', 0)/1e3:>11.1f}K")
        print(f"  CVD Total:     ${snapshot.get('cvd_total', 0):>12,.0f}")
        print(f"  Active Wicks:  {snapshot.get('active_wicks', 0):>12}")
        print(f"  Active Stacks: {snapshot.get('active_stacks', 0):>12}")
        print(f"\n  REGIME: {snapshot.get('regime', 'UNKNOWN')}")
        print(f"{'='*60}")
    
    def run_continuous(self, interval_minutes=5):
        """Run continuous snapshot collection."""
        print("Starting Derivatives Correlation Tracker...")
        print(f"Snapshot interval: {interval_minutes} minutes")
        print(f"Data saved to: {DATA_DIR}")
        print("Ctrl+C to stop\n")
        
        try:
            while True:
                snapshot = self.take_snapshot()
                self.print_snapshot(snapshot)
                
                # Check for events
                recent_events = [e for e in self.events[-10:] if e['timestamp'] == snapshot['datetime']]
                if recent_events:
                    print(f"\n  EVENTS DETECTED:")
                    for event in recent_events:
                        print(f"    - {event['type']}: {event.get('direction', event.get('dominant', ''))}")
                
                print(f"\n  Next snapshot in {interval_minutes} minutes...")
                print(f"  Total snapshots: {len(self.snapshots)}")
                print(f"  Total events: {len(self.events)}")
                
                time.sleep(interval_minutes * 60)
        
        except KeyboardInterrupt:
            print("\n\nStopping...")
            print(f"Saved {len(self.snapshots)} snapshots, {len(self.events)} events")


def main():
    tracker = DerivativesTracker()
    
    # Take single snapshot or run continuous
    if len(sys.argv) > 1 and sys.argv[1] == '--continuous':
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        tracker.run_continuous(interval_minutes=interval)
    else:
        snapshot = tracker.take_snapshot()
        tracker.print_snapshot(snapshot)
        print(f"\nSaved to: {SNAPSHOT_PATH}")


if __name__ == '__main__':
    main()
