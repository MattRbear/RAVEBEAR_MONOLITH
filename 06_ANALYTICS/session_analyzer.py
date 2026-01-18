"""
Session Correlation Analyzer
Analyzes stored session data to find trading edge patterns.
Works with timestamped session data from MasterDataCollector.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("pip install pandas numpy")
    exit(1)

SESSIONS_DIR = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\Sessions")
OUTPUT_DIR = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\Analysis")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_all_sessions():
    """Load all session data."""
    sessions = []
    
    for session_dir in sorted(SESSIONS_DIR.iterdir()):
        if not session_dir.is_dir():
            continue
        
        session_file = session_dir / "session_data.json"
        if session_file.exists():
            with open(session_file) as f:
                data = json.load(f)
                data['_dir'] = str(session_dir)
                sessions.append(data)
    
    return sessions


def load_all_snapshots():
    """Load all snapshots from all sessions."""
    snapshots = []
    
    for session_dir in sorted(SESSIONS_DIR.iterdir()):
        if not session_dir.is_dir():
            continue
        
        snap_dir = session_dir / "snapshots"
        if not snap_dir.exists():
            continue
        
        for snap_file in sorted(snap_dir.glob("snap_*.json")):
            with open(snap_file) as f:
                snap = json.load(f)
                snap['_session'] = session_dir.name
                snapshots.append(snap)
    
    return snapshots


def analyze_regimes(snapshots):
    """Analyze price outcomes by regime."""
    print("\n" + "=" * 70)
    print("  REGIME ANALYSIS")
    print("=" * 70)
    
    if len(snapshots) < 5:
        print("  Need more snapshots for analysis")
        return
    
    # Group by regime
    regime_outcomes = defaultdict(list)
    
    for i in range(len(snapshots) - 1):
        current = snapshots[i]
        next_snap = snapshots[i + 1]
        
        regime = current.get('regime', 'UNKNOWN')
        current_price = current.get('btc_price', 0)
        next_price = next_snap.get('btc_price', 0)
        
        if current_price and next_price:
            change_pct = ((next_price - current_price) / current_price) * 100
            regime_outcomes[regime].append({
                'change_pct': change_pct,
                'went_up': change_pct > 0,
            })
    
    print(f"\n  {'REGIME':<50} {'COUNT':>6} {'UP%':>8} {'AVG CHG':>10}")
    print("  " + "-" * 76)
    
    for regime, outcomes in sorted(regime_outcomes.items(), key=lambda x: len(x[1]), reverse=True):
        if len(outcomes) < 3:
            continue
        
        count = len(outcomes)
        up_pct = sum(1 for o in outcomes if o['went_up']) / count * 100
        avg_chg = np.mean([o['change_pct'] for o in outcomes])
        
        print(f"  {regime:<50} {count:>6} {up_pct:>7.1f}% {avg_chg:>+9.4f}%")


def analyze_whale_correlation(snapshots):
    """Analyze whale flow vs price movement."""
    print("\n" + "=" * 70)
    print("  WHALE FLOW CORRELATION")
    print("=" * 70)
    
    whale_outcomes = {
        'inflow': [],   # Net positive = to exchanges
        'outflow': [],  # Net negative = from exchanges
        'neutral': [],
    }
    
    for i in range(len(snapshots) - 1):
        current = snapshots[i]
        next_snap = snapshots[i + 1]
        
        whale = current.get('whale_flow', {})
        net_flow = whale.get('btc_net_flow', 0)
        
        current_price = current.get('btc_price', 0)
        next_price = next_snap.get('btc_price', 0)
        
        if current_price and next_price:
            change_pct = ((next_price - current_price) / current_price) * 100
            
            if net_flow > 5_000_000:
                whale_outcomes['inflow'].append(change_pct)
            elif net_flow < -5_000_000:
                whale_outcomes['outflow'].append(change_pct)
            else:
                whale_outcomes['neutral'].append(change_pct)
    
    print(f"\n  {'WHALE STATE':<20} {'COUNT':>8} {'AVG CHANGE':>12} {'UP%':>8}")
    print("  " + "-" * 50)
    
    for state, changes in whale_outcomes.items():
        if len(changes) < 3:
            continue
        
        count = len(changes)
        avg = np.mean(changes)
        up_pct = sum(1 for c in changes if c > 0) / count * 100
        
        signal = ""
        if state == 'inflow' and avg < 0:
            signal = " <- CONFIRMED BEARISH"
        elif state == 'outflow' and avg > 0:
            signal = " <- CONFIRMED BULLISH"
        
        print(f"  {state.upper():<20} {count:>8} {avg:>+11.4f}% {up_pct:>7.1f}%{signal}")


def analyze_funding_extremes(snapshots):
    """Analyze outcomes after extreme funding."""
    print("\n" + "=" * 70)
    print("  FUNDING EXTREME ANALYSIS")
    print("=" * 70)
    
    funding_outcomes = {
        'extreme_positive': [],  # > 0.05%
        'positive': [],          # 0.01% to 0.05%
        'neutral': [],           # -0.01% to 0.01%
        'negative': [],          # -0.05% to -0.01%
        'extreme_negative': [],  # < -0.05%
    }
    
    for i in range(len(snapshots) - 1):
        current = snapshots[i]
        next_snap = snapshots[i + 1]
        
        deriv = current.get('derivatives', {})
        funding = deriv.get('funding_rate', 0) * 100  # Convert to %
        
        current_price = current.get('btc_price', 0)
        next_price = next_snap.get('btc_price', 0)
        
        if current_price and next_price:
            change_pct = ((next_price - current_price) / current_price) * 100
            
            if funding > 0.05:
                funding_outcomes['extreme_positive'].append(change_pct)
            elif funding > 0.01:
                funding_outcomes['positive'].append(change_pct)
            elif funding > -0.01:
                funding_outcomes['neutral'].append(change_pct)
            elif funding > -0.05:
                funding_outcomes['negative'].append(change_pct)
            else:
                funding_outcomes['extreme_negative'].append(change_pct)
    
    print(f"\n  {'FUNDING LEVEL':<20} {'COUNT':>8} {'AVG CHANGE':>12} {'UP%':>8}")
    print("  " + "-" * 50)
    
    for level, changes in funding_outcomes.items():
        if len(changes) < 2:
            continue
        
        count = len(changes)
        avg = np.mean(changes)
        up_pct = sum(1 for c in changes if c > 0) / count * 100
        
        # Expected: extreme positive funding -> price should drop (long squeeze)
        signal = ""
        if level == 'extreme_positive' and avg < 0:
            signal = " <- SQUEEZE CONFIRMED"
        elif level == 'extreme_negative' and avg > 0:
            signal = " <- SQUEEZE CONFIRMED"
        
        print(f"  {level:<20} {count:>8} {avg:>+11.4f}% {up_pct:>7.1f}%{signal}")


def analyze_ls_extremes(snapshots):
    """Analyze outcomes after extreme L/S ratios."""
    print("\n" + "=" * 70)
    print("  LONG/SHORT RATIO ANALYSIS")
    print("=" * 70)
    
    ls_outcomes = {
        'very_long_heavy': [],   # > 70%
        'long_heavy': [],        # 60-70%
        'balanced': [],          # 40-60%
        'short_heavy': [],       # 30-40%
        'very_short_heavy': [],  # < 30%
    }
    
    for i in range(len(snapshots) - 1):
        current = snapshots[i]
        next_snap = snapshots[i + 1]
        
        deriv = current.get('derivatives', {})
        long_pct = deriv.get('long_pct', 50)
        
        current_price = current.get('btc_price', 0)
        next_price = next_snap.get('btc_price', 0)
        
        if current_price and next_price:
            change_pct = ((next_price - current_price) / current_price) * 100
            
            if long_pct > 70:
                ls_outcomes['very_long_heavy'].append(change_pct)
            elif long_pct > 60:
                ls_outcomes['long_heavy'].append(change_pct)
            elif long_pct > 40:
                ls_outcomes['balanced'].append(change_pct)
            elif long_pct > 30:
                ls_outcomes['short_heavy'].append(change_pct)
            else:
                ls_outcomes['very_short_heavy'].append(change_pct)
    
    print(f"\n  {'L/S LEVEL':<20} {'COUNT':>8} {'AVG CHANGE':>12} {'UP%':>8}")
    print("  " + "-" * 50)
    
    for level, changes in ls_outcomes.items():
        if len(changes) < 2:
            continue
        
        count = len(changes)
        avg = np.mean(changes)
        up_pct = sum(1 for c in changes if c > 0) / count * 100
        
        # Expected: very long heavy -> price should drop (hunt longs)
        signal = ""
        if level == 'very_long_heavy' and avg < 0:
            signal = " <- LONGS HUNTED"
        elif level == 'very_short_heavy' and avg > 0:
            signal = " <- SHORTS HUNTED"
        
        print(f"  {level:<20} {count:>8} {avg:>+11.4f}% {up_pct:>7.1f}%{signal}")


def analyze_object_density(snapshots):
    """Analyze if object density correlates with price movement."""
    print("\n" + "=" * 70)
    print("  OBJECT DENSITY VS PRICE MOVEMENT")
    print("=" * 70)
    
    density_outcomes = {
        'high_above': [],   # Many objects above price
        'high_below': [],   # Many objects below price
        'balanced': [],     # Similar above/below
    }
    
    for i in range(len(snapshots) - 1):
        current = snapshots[i]
        next_snap = snapshots[i + 1]
        
        objects = current.get('objects', {})
        above = objects.get('wicks_above', 0) + objects.get('poors_above', 0)
        below = objects.get('wicks_below', 0) + objects.get('poors_below', 0)
        
        current_price = current.get('btc_price', 0)
        next_price = next_snap.get('btc_price', 0)
        
        if current_price and next_price and (above + below) > 0:
            change_pct = ((next_price - current_price) / current_price) * 100
            
            ratio = above / (above + below) if (above + below) > 0 else 0.5
            
            if ratio > 0.6:
                density_outcomes['high_above'].append(change_pct)
            elif ratio < 0.4:
                density_outcomes['high_below'].append(change_pct)
            else:
                density_outcomes['balanced'].append(change_pct)
    
    print(f"\n  {'DENSITY BIAS':<20} {'COUNT':>8} {'AVG CHANGE':>12} {'UP%':>8}")
    print("  " + "-" * 50)
    
    for level, changes in density_outcomes.items():
        if len(changes) < 2:
            continue
        
        count = len(changes)
        avg = np.mean(changes)
        up_pct = sum(1 for c in changes if c > 0) / count * 100
        
        print(f"  {level:<20} {count:>8} {avg:>+11.4f}% {up_pct:>7.1f}%")


def generate_report():
    """Generate full correlation report."""
    print("=" * 70)
    print("  RAVEBEAR SESSION CORRELATION REPORT")
    print("=" * 70)
    
    sessions = load_all_sessions()
    print(f"\n  Sessions loaded: {len(sessions)}")
    
    snapshots = load_all_snapshots()
    print(f"  Snapshots loaded: {len(snapshots)}")
    
    if len(snapshots) < 5:
        print("\n  Need more data! Run master_collector.py for a while first.")
        print("  Minimum 5 snapshots required for analysis.")
        return
    
    # Run analyses
    analyze_regimes(snapshots)
    analyze_whale_correlation(snapshots)
    analyze_funding_extremes(snapshots)
    analyze_ls_extremes(snapshots)
    analyze_object_density(snapshots)
    
    print("\n" + "=" * 70)
    print("  REPORT COMPLETE")
    print("=" * 70)
    
    # Save summary
    summary = {
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'sessions_analyzed': len(sessions),
        'snapshots_analyzed': len(snapshots),
    }
    
    with open(OUTPUT_DIR / "correlation_summary.json", 'w') as f:
        json.dump(summary, f, indent=2)


if __name__ == '__main__':
    generate_report()
