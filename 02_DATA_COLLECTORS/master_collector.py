"""
RaveBear Master Data Collector
Unified data collection for all systems with proper timestamped storage.
Sessions reset at UTC 00:00 daily.
"""

import os
import sys
import time
import json
import shutil
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("pip install pandas numpy pyarrow requests")
    sys.exit(1)

# Import validator
from data_validator import DataValidator, validate_and_alert

# === PATHS ===
BASE_DIR = Path(r"C:\Users\M.R Bear\Documents")
DATA_VAULT = BASE_DIR / "Data_Vault"
CANDLE_PATH = DATA_VAULT / "1m_Candles" / "BTC_USDT_SWAP_1m.parquet"

# Session data storage
SESSIONS_DIR = DATA_VAULT / "Sessions"
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

# Add paths for imports
sys.path.insert(0, str(BASE_DIR / "Candle_collector"))
sys.path.insert(0, str(BASE_DIR / "Whales"))
sys.path.insert(0, str(BASE_DIR / "Coin_anal"))

# === CONFIG ===
SNAPSHOT_INTERVAL = 300  # 5 minutes between full snapshots
OBJECT_UPDATE_INTERVAL = 60  # 1 minute for object updates


class SessionManager:
    """Manages daily trading sessions (UTC 00:00 reset)."""
    
    def __init__(self):
        self.current_session = None
        self.session_dir = None
        self.session_data = None
        self.initialize_session()
    
    def get_session_id(self):
        """Get current session ID based on UTC date."""
        now = datetime.now(timezone.utc)
        return now.strftime("%Y-%m-%d")
    
    def initialize_session(self):
        """Initialize or load current session."""
        session_id = self.get_session_id()
        
        if self.current_session != session_id:
            # New session
            self.current_session = session_id
            self.session_dir = SESSIONS_DIR / session_id
            self.session_dir.mkdir(parents=True, exist_ok=True)
            
            # Initialize session data file
            self.session_file = self.session_dir / "session_data.json"
            
            if self.session_file.exists():
                with open(self.session_file) as f:
                    self.session_data = json.load(f)
                print(f"Loaded existing session: {session_id}")
            else:
                self.session_data = {
                    'session_id': session_id,
                    'started_at': datetime.now(timezone.utc).isoformat(),
                    'snapshots': [],
                    'events': [],
                    'summary': {},
                }
                print(f"Created new session: {session_id}")
            
            # Create subdirs
            (self.session_dir / "objects").mkdir(exist_ok=True)
            (self.session_dir / "derivatives").mkdir(exist_ok=True)
            (self.session_dir / "whale_flow").mkdir(exist_ok=True)
            (self.session_dir / "snapshots").mkdir(exist_ok=True)
    
    def check_session_reset(self):
        """Check if we need to reset for new day."""
        current_id = self.get_session_id()
        if current_id != self.current_session:
            self.finalize_session()
            self.initialize_session()
            return True
        return False
    
    def finalize_session(self):
        """Finalize current session before reset."""
        if self.session_data:
            self.session_data['ended_at'] = datetime.now(timezone.utc).isoformat()
            self.session_data['snapshot_count'] = len(self.session_data.get('snapshots', []))
            self.save_session()
            print(f"Finalized session: {self.current_session}")
    
    def save_session(self):
        """Save session data."""
        with open(self.session_file, 'w') as f:
            json.dump(self.session_data, f, indent=2)
    
    def add_snapshot(self, snapshot):
        """Add a snapshot to session."""
        self.session_data['snapshots'].append(snapshot)
        # Keep last 500 snapshots per session
        if len(self.session_data['snapshots']) > 500:
            self.session_data['snapshots'] = self.session_data['snapshots'][-500:]
        self.save_session()
    
    def save_objects(self, object_type, data):
        """Save object data with timestamp."""
        ts = datetime.now(timezone.utc).strftime("%H%M%S")
        path = self.session_dir / "objects" / f"{object_type}_{ts}.json"
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
        
        # Also save as latest
        latest_path = self.session_dir / "objects" / f"{object_type}_latest.json"
        with open(latest_path, 'w') as f:
            json.dump(data, f, indent=2)


class ObjectFactory:
    """Unified factory for all tradeable objects."""
    
    def __init__(self):
        self.df = None
        self.load_candles()
    
    def load_candles(self):
        """Load candle data."""
        if CANDLE_PATH.exists():
            self.df = pd.read_parquet(CANDLE_PATH).sort_values('timestamp').reset_index(drop=True)
        else:
            self.df = None
    
    def find_wicks(self, min_wick=15.0):
        """Find untouched wicks."""
        if self.df is None or len(self.df) < 10:
            return []
        
        df = self.df
        n = len(df)
        highs = df['high'].values
        lows = df['low'].values
        opens = df['open'].values
        closes = df['close'].values
        ts = df['timestamp'].values
        
        run_max_h = np.maximum.accumulate(highs[::-1])[::-1]
        run_min_l = np.minimum.accumulate(lows[::-1])[::-1]
        
        wicks = []
        
        for i in range(n - 1):
            o, h, l, c = opens[i], highs[i], lows[i], closes[i]
            body_top = max(o, c)
            body_bot = min(o, c)
            upper_len = h - body_top
            lower_len = body_bot - l
            
            if upper_len >= min_wick and run_max_h[i+1] < h:
                wicks.append({
                    'type': 'WICK',
                    'dir': 'UP',
                    'price': float(h),
                    'ts': int(ts[i]),
                    'wick_size': float(upper_len),
                })
            
            if lower_len >= min_wick and run_min_l[i+1] > l:
                wicks.append({
                    'type': 'WICK',
                    'dir': 'DN',
                    'price': float(l),
                    'ts': int(ts[i]),
                    'wick_size': float(lower_len),
                })
        
        return wicks
    
    def find_poor_levels(self, lookback=3, max_ratio=0.3):
        """Find poor highs and lows."""
        if self.df is None or len(self.df) < lookback * 2 + 1:
            return []
        
        df = self.df
        n = len(df)
        highs = df['high'].values
        lows = df['low'].values
        opens = df['open'].values
        closes = df['close'].values
        ts = df['timestamp'].values
        
        run_max_h = np.maximum.accumulate(highs[::-1])[::-1]
        run_min_l = np.minimum.accumulate(lows[::-1])[::-1]
        
        poors = []
        
        for i in range(lookback, n - lookback - 1):
            o, h, l, c = opens[i], highs[i], lows[i], closes[i]
            body_top = max(o, c)
            body_bot = min(o, c)
            body = abs(c - o)
            upper_len = h - body_top
            lower_len = body_bot - l
            
            if body == 0:
                continue
            
            # Poor High
            is_swing_high = all(highs[j] < h for j in range(i - lookback, i + lookback + 1) if j != i)
            if is_swing_high and (upper_len / body) <= max_ratio and run_max_h[i+1] < h:
                poors.append({
                    'type': 'POOR',
                    'dir': 'HI',
                    'price': float(h),
                    'ts': int(ts[i]),
                    'body_size': float(body),
                })
            
            # Poor Low
            is_swing_low = all(lows[j] > l for j in range(i - lookback, i + lookback + 1) if j != i)
            if is_swing_low and (lower_len / body) <= max_ratio and run_min_l[i+1] > l:
                poors.append({
                    'type': 'POOR',
                    'dir': 'LO',
                    'price': float(l),
                    'ts': int(ts[i]),
                    'body_size': float(body),
                })
        
        return poors
    
    def find_boxes(self, min_candles=10, max_range_pct=0.3):
        """Find consolidation boxes."""
        if self.df is None or len(self.df) < min_candles:
            return []
        
        df = self.df
        n = len(df)
        highs = df['high'].values
        lows = df['low'].values
        closes = df['close'].values
        ts = df['timestamp'].values
        
        current_price = closes[-1]
        boxes = []
        
        i = 0
        while i < n - min_candles:
            box_start = i
            box_high = highs[i]
            box_low = lows[i]
            
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
                
                # Check if broken
                state = 'ACTIVE'
                if j < n:
                    for k in range(j, n):
                        if closes[k] > box_high:
                            state = 'BROKEN_UP'
                            break
                        elif closes[k] < box_low:
                            state = 'BROKEN_DOWN'
                            break
                
                boxes.append({
                    'type': 'BOX',
                    'high': float(box_high),
                    'low': float(box_low),
                    'mid': float((box_high + box_low) / 2),
                    'range': float(box_range),
                    'duration': box_len,
                    'ts_start': int(ts[box_start]),
                    'ts_end': int(ts[j - 1]),
                    'state': state,
                })
                
                i = j
            else:
                i += 1
        
        return boxes
    
    def get_all_objects(self):
        """Get all tradeable objects."""
        self.load_candles()
        
        if self.df is None:
            return {}
        
        current_price = float(self.df.iloc[-1]['close'])
        
        wicks = self.find_wicks()
        poors = self.find_poor_levels()
        boxes = self.find_boxes()
        
        # Separate by direction/type
        return {
            'timestamp': int(time.time() * 1000),
            'datetime': datetime.now(timezone.utc).isoformat(),
            'btc_price': current_price,
            'wicks_up': [w for w in wicks if w['dir'] == 'UP'],
            'wicks_dn': [w for w in wicks if w['dir'] == 'DN'],
            'poor_hi': [p for p in poors if p['dir'] == 'HI'],
            'poor_lo': [p for p in poors if p['dir'] == 'LO'],
            'boxes_active': [b for b in boxes if b['state'] == 'ACTIVE'],
            'boxes_broken': [b for b in boxes if b['state'] != 'ACTIVE'],
            'summary': {
                'total_wicks': len(wicks),
                'total_poors': len(poors),
                'total_boxes': len(boxes),
                'active_boxes': len([b for b in boxes if b['state'] == 'ACTIVE']),
                'wicks_above': len([w for w in wicks if w['price'] > current_price]),
                'wicks_below': len([w for w in wicks if w['price'] < current_price]),
                'poors_above': len([p for p in poors if p['price'] > current_price]),
                'poors_below': len([p for p in poors if p['price'] < current_price]),
            }
        }


class DerivativesCollector:
    """Collects derivatives data from Coinalyze."""
    
    def __init__(self):
        self.client = None
        try:
            from coinalyze_client import CoinalyzeClient
            self.client = CoinalyzeClient()
        except:
            pass
    
    def get_snapshot(self):
        """Get derivatives snapshot."""
        if not self.client:
            return None
        
        try:
            oi_data = self.client.get_open_interest()
            funding_data = self.client.get_funding_rate()
            ls_history = self.client.get_long_short_ratio_history(interval="1hour", hours=1)
            liq_history = self.client.get_liquidation_history(interval="1hour", hours=4)
            
            ls_latest = ls_history[-1] if ls_history else {}
            
            # Sum liquidations
            long_liqs = sum(l.get('l', 0) for l in liq_history) if liq_history else 0
            short_liqs = sum(l.get('s', 0) for l in liq_history) if liq_history else 0
            
            return {
                'timestamp': int(time.time() * 1000),
                'datetime': datetime.now(timezone.utc).isoformat(),
                'oi_value': oi_data[0].get('value', 0) if oi_data else 0,
                'funding_rate': funding_data[0].get('value', 0) if funding_data else 0,
                'long_pct': ls_latest.get('l', 50) if isinstance(ls_latest, dict) else 50,
                'short_pct': ls_latest.get('s', 50) if isinstance(ls_latest, dict) else 50,
                'ls_ratio': ls_latest.get('r', 1) if isinstance(ls_latest, dict) else 1,
                'long_liqs_4h': long_liqs,
                'short_liqs_4h': short_liqs,
            }
        except Exception as e:
            print(f"Derivatives error: {e}")
            return None


class WhaleCollector:
    """Collects whale flow data."""
    
    def __init__(self):
        self.client = None
        try:
            from whale_client import WhaleAlertClient
            self.client = WhaleAlertClient()
        except:
            pass
    
    def get_snapshot(self):
        """Get whale flow snapshot."""
        if not self.client:
            return None
        
        try:
            btc_txs = self.client.get_btc_transactions(hours=4, min_value=1000000)
            if not btc_txs:
                return None
            
            analysis = self.client.analyze_flow(btc_txs)
            
            return {
                'timestamp': int(time.time() * 1000),
                'datetime': datetime.now(timezone.utc).isoformat(),
                'btc_net_flow': analysis.get('net_exchange_flow', 0),
                'btc_inflow': analysis.get('exchange_inflow', 0),
                'btc_outflow': analysis.get('exchange_outflow', 0),
                'btc_volume': analysis.get('total_volume', 0),
                'tx_count': analysis.get('tx_count', 0),
                'inflow_count': analysis.get('inflow_count', 0),
                'outflow_count': analysis.get('outflow_count', 0),
            }
        except Exception as e:
            print(f"Whale error: {e}")
            return None


class MasterDataCollector:
    """Master collector that orchestrates all data collection."""
    
    def __init__(self):
        self.session = SessionManager()
        self.objects = ObjectFactory()
        self.derivatives = DerivativesCollector()
        self.whales = WhaleCollector()
        
        self.last_snapshot = 0
        self.last_objects = 0
        self.snapshot_count = 0
    
    def take_full_snapshot(self):
        """Take a complete market snapshot."""
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Taking full snapshot...")
        
        snapshot = {
            'id': self.snapshot_count + 1,
            'timestamp': int(time.time() * 1000),
            'datetime': datetime.now(timezone.utc).isoformat(),
            'validation': {'errors': 0, 'warnings': 0},
        }
        
        # Get objects
        print("  Scanning objects...")
        obj_data = self.objects.get_all_objects()
        if obj_data:
            # Validate objects
            if not validate_and_alert('objects', obj_data):
                snapshot['validation']['errors'] += 1
                print("  [!] Object validation failed")
            snapshot['btc_price'] = obj_data.get('btc_price', 0)
            snapshot['objects'] = obj_data.get('summary', {})
            self.session.save_objects('all_objects', obj_data)
        
        # Get derivatives
        print("  Fetching derivatives...")
        deriv_data = self.derivatives.get_snapshot()
        if deriv_data:
            # Validate derivatives
            if not validate_and_alert('derivatives', deriv_data):
                snapshot['validation']['errors'] += 1
                print("  [!] Derivatives validation failed")
            snapshot['derivatives'] = deriv_data
            # Save to session
            deriv_path = self.session.session_dir / "derivatives" / f"deriv_{datetime.now().strftime('%H%M%S')}.json"
            with open(deriv_path, 'w') as f:
                json.dump(deriv_data, f, indent=2)
        else:
            snapshot['validation']['warnings'] += 1
            print("  [!] No derivatives data")
        
        # Get whale flow
        print("  Fetching whale flow...")
        whale_data = self.whales.get_snapshot()
        if whale_data:
            # Validate whale data
            if not validate_and_alert('whale_flow', whale_data):
                snapshot['validation']['errors'] += 1
                print("  [!] Whale validation failed")
            snapshot['whale_flow'] = whale_data
            # Save to session
            whale_path = self.session.session_dir / "whale_flow" / f"whale_{datetime.now().strftime('%H%M%S')}.json"
            with open(whale_path, 'w') as f:
                json.dump(whale_data, f, indent=2)
        else:
            snapshot['validation']['warnings'] += 1
            print("  [!] No whale data")
        
        # Classify market state
        snapshot['regime'] = self.classify_regime(snapshot)
        
        # Final snapshot validation
        validate_and_alert('snapshot', snapshot)
        
        # Add to session
        self.session.add_snapshot(snapshot)
        self.snapshot_count += 1
        
        # Save full snapshot
        snap_path = self.session.session_dir / "snapshots" / f"snap_{self.snapshot_count:04d}.json"
        with open(snap_path, 'w') as f:
            json.dump(snapshot, f, indent=2)
        
        # Status
        errors = snapshot['validation']['errors']
        warnings = snapshot['validation']['warnings']
        status = "OK" if errors == 0 else f"ERRORS: {errors}"
        print(f"  Snapshot #{self.snapshot_count} saved [{status}]")
        
        return snapshot
    
    def classify_regime(self, snapshot):
        """Classify current market regime."""
        regime = []
        
        # Funding regime
        deriv = snapshot.get('derivatives', {})
        funding = deriv.get('funding_rate', 0)
        if funding > 0.0005:
            regime.append('HIGH_FUNDING_LONG')
        elif funding < -0.0005:
            regime.append('HIGH_FUNDING_SHORT')
        elif funding > 0:
            regime.append('POSITIVE_FUNDING')
        elif funding < 0:
            regime.append('NEGATIVE_FUNDING')
        else:
            regime.append('NEUTRAL_FUNDING')
        
        # Position regime
        long_pct = deriv.get('long_pct', 50)
        if long_pct > 65:
            regime.append('LONG_HEAVY')
        elif long_pct < 35:
            regime.append('SHORT_HEAVY')
        elif long_pct > 55:
            regime.append('LONG_BIAS')
        elif long_pct < 45:
            regime.append('SHORT_BIAS')
        else:
            regime.append('BALANCED')
        
        # Whale regime
        whale = snapshot.get('whale_flow', {})
        net_flow = whale.get('btc_net_flow', 0)
        if net_flow > 10_000_000:
            regime.append('WHALE_SELLING')
        elif net_flow < -10_000_000:
            regime.append('WHALE_ACCUMULATING')
        else:
            regime.append('WHALE_NEUTRAL')
        
        return '|'.join(regime)
    
    def run(self):
        """Run the master data collector."""
        print("=" * 60)
        print("  RAVEBEAR MASTER DATA COLLECTOR")
        print("=" * 60)
        print(f"  Session: {self.session.current_session}")
        print(f"  Snapshot interval: {SNAPSHOT_INTERVAL}s")
        print(f"  Session resets at: UTC 00:00")
        print("=" * 60)
        
        # Take initial snapshot
        self.take_full_snapshot()
        self.last_snapshot = time.time()
        
        try:
            while True:
                now = time.time()
                
                # Check for session reset (UTC 00:00)
                if self.session.check_session_reset():
                    print("\n*** NEW SESSION STARTED ***")
                    self.snapshot_count = 0
                
                # Take snapshot every SNAPSHOT_INTERVAL
                if now - self.last_snapshot >= SNAPSHOT_INTERVAL:
                    self.take_full_snapshot()
                    self.last_snapshot = now
                
                # Display status
                next_snap = int(SNAPSHOT_INTERVAL - (now - self.last_snapshot))
                print(f"\r  Next snapshot in {next_snap}s... (Total: {self.snapshot_count})", end='', flush=True)
                
                time.sleep(10)
        
        except KeyboardInterrupt:
            print("\n\nShutting down...")
            self.session.finalize_session()
            print("Session saved.")


def main():
    collector = MasterDataCollector()
    collector.run()


if __name__ == '__main__':
    main()
