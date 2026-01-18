"""
Derivatives Correlation Analyzer
Finds patterns between derivatives data and price/level outcomes.
"""

import json
from datetime import datetime
from pathlib import Path
from collections import defaultdict

DATA_DIR = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\Correlations")
SNAPSHOT_PATH = DATA_DIR / "market_snapshots.json"
EVENTS_PATH = DATA_DIR / "derivative_events.json"


def load_snapshots():
    if SNAPSHOT_PATH.exists():
        with open(SNAPSHOT_PATH) as f:
            return json.load(f)
    return []


def load_events():
    if EVENTS_PATH.exists():
        with open(EVENTS_PATH) as f:
            return json.load(f)
    return []


def analyze_regime_outcomes(snapshots):
    """Analyze what happens after each regime."""
    if len(snapshots) < 2:
        return {}
    
    regime_stats = defaultdict(lambda: {
        'count': 0,
        'price_up': 0,
        'price_down': 0,
        'avg_price_change': 0,
        'oi_increase': 0,
        'oi_decrease': 0,
    })
    
    for i in range(len(snapshots) - 1):
        current = snapshots[i]
        next_snap = snapshots[i + 1]
        
        regime = current.get('regime', 'UNKNOWN')
        
        price_now = current.get('btc_price', 0)
        price_next = next_snap.get('btc_price', 0)
        
        if price_now and price_next:
            price_change = ((price_next - price_now) / price_now) * 100
            
            regime_stats[regime]['count'] += 1
            regime_stats[regime]['avg_price_change'] += price_change
            
            if price_change > 0:
                regime_stats[regime]['price_up'] += 1
            else:
                regime_stats[regime]['price_down'] += 1
        
        oi_now = current.get('oi_value', 0)
        oi_next = next_snap.get('oi_value', 0)
        
        if oi_now and oi_next:
            if oi_next > oi_now:
                regime_stats[regime]['oi_increase'] += 1
            else:
                regime_stats[regime]['oi_decrease'] += 1
    
    # Calculate averages
    for regime, stats in regime_stats.items():
        if stats['count'] > 0:
            stats['avg_price_change'] /= stats['count']
            stats['up_rate'] = stats['price_up'] / stats['count'] * 100
    
    return dict(regime_stats)


def analyze_funding_extremes(snapshots):
    """Find correlation between extreme funding and price reversals."""
    extreme_funding = []
    
    for i, snap in enumerate(snapshots):
        funding = snap.get('funding_rate', 0)
        
        # High positive funding (>0.05%)
        if funding > 0.0005:
            extreme_funding.append({
                'type': 'HIGH_POSITIVE',
                'funding': funding,
                'price': snap.get('btc_price'),
                'index': i,
            })
        # High negative funding (<-0.05%)
        elif funding < -0.0005:
            extreme_funding.append({
                'type': 'HIGH_NEGATIVE',
                'funding': funding,
                'price': snap.get('btc_price'),
                'index': i,
            })
    
    # Check what happened after
    results = {'HIGH_POSITIVE': [], 'HIGH_NEGATIVE': []}
    
    for extreme in extreme_funding:
        idx = extreme['index']
        if idx + 1 < len(snapshots):
            next_price = snapshots[idx + 1].get('btc_price', 0)
            if extreme['price'] and next_price:
                change = ((next_price - extreme['price']) / extreme['price']) * 100
                results[extreme['type']].append(change)
    
    return results


def analyze_ls_extremes(snapshots):
    """Find correlation between extreme L/S ratios and squeezes."""
    extreme_positions = []
    
    for i, snap in enumerate(snapshots):
        long_pct = snap.get('long_pct', 50)
        
        if long_pct > 65:  # Heavy long
            extreme_positions.append({
                'type': 'LONG_HEAVY',
                'long_pct': long_pct,
                'price': snap.get('btc_price'),
                'index': i,
            })
        elif long_pct < 35:  # Heavy short
            extreme_positions.append({
                'type': 'SHORT_HEAVY',
                'long_pct': long_pct,
                'price': snap.get('btc_price'),
                'index': i,
            })
    
    results = {'LONG_HEAVY': [], 'SHORT_HEAVY': []}
    
    for extreme in extreme_positions:
        idx = extreme['index']
        if idx + 1 < len(snapshots):
            next_price = snapshots[idx + 1].get('btc_price', 0)
            if extreme['price'] and next_price:
                change = ((next_price - extreme['price']) / extreme['price']) * 100
                results[extreme['type']].append(change)
    
    return results


def analyze_events(events):
    """Analyze event patterns."""
    event_counts = defaultdict(int)
    event_by_regime = defaultdict(lambda: defaultdict(int))
    
    for event in events:
        event_type = event.get('type', 'UNKNOWN')
        regime = event.get('regime', 'UNKNOWN')
        
        event_counts[event_type] += 1
        event_by_regime[regime][event_type] += 1
    
    return dict(event_counts), dict(event_by_regime)


def print_analysis():
    """Print full correlation analysis."""
    snapshots = load_snapshots()
    events = load_events()
    
    print(f"\n{'='*70}")
    print(f"  DERIVATIVES CORRELATION ANALYSIS")
    print(f"  Snapshots: {len(snapshots)}  |  Events: {len(events)}")
    print(f"{'='*70}")
    
    if len(snapshots) < 5:
        print(f"\n  Need more data. Run tracker continuously to collect snapshots.")
        print(f"  Command: python derivatives_tracker.py --continuous 5")
        print(f"\n  Current data points: {len(snapshots)}")
        return
    
    # Regime analysis
    regime_stats = analyze_regime_outcomes(snapshots)
    
    if regime_stats:
        print(f"\n  REGIME -> NEXT PERIOD OUTCOMES")
        print(f"  {'-'*60}")
        print(f"  {'REGIME':<35} {'COUNT':>6} {'UP%':>8} {'AVG CHG':>10}")
        print(f"  {'-'*60}")
        
        for regime, stats in sorted(regime_stats.items(), key=lambda x: x[1]['count'], reverse=True):
            if stats['count'] >= 2:
                print(f"  {regime:<35} {stats['count']:>6} {stats.get('up_rate', 0):>7.1f}% {stats['avg_price_change']:>+9.3f}%")
    
    # Funding extremes
    funding_results = analyze_funding_extremes(snapshots)
    
    print(f"\n  EXTREME FUNDING -> PRICE OUTCOME")
    print(f"  {'-'*60}")
    
    for ftype, changes in funding_results.items():
        if changes:
            avg = sum(changes) / len(changes)
            up_count = len([c for c in changes if c > 0])
            print(f"  {ftype}: {len(changes)} occurrences, avg change: {avg:+.3f}%, up: {up_count}/{len(changes)}")
    
    # L/S extremes
    ls_results = analyze_ls_extremes(snapshots)
    
    print(f"\n  EXTREME L/S RATIO -> PRICE OUTCOME")
    print(f"  {'-'*60}")
    
    for ltype, changes in ls_results.items():
        if changes:
            avg = sum(changes) / len(changes)
            # For long heavy, we expect DOWN (squeeze). For short heavy, we expect UP
            expected = 'DOWN' if ltype == 'LONG_HEAVY' else 'UP'
            actual_expected = len([c for c in changes if (c < 0 if ltype == 'LONG_HEAVY' else c > 0)])
            print(f"  {ltype}: {len(changes)} occurrences, avg: {avg:+.3f}%, moved {expected}: {actual_expected}/{len(changes)}")
    
    # Events
    event_counts, event_by_regime = analyze_events(events)
    
    if event_counts:
        print(f"\n  EVENT FREQUENCY")
        print(f"  {'-'*60}")
        for event_type, count in sorted(event_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {event_type}: {count}")
    
    # Current state assessment
    if snapshots:
        latest = snapshots[-1]
        print(f"\n  CURRENT STATE")
        print(f"  {'-'*60}")
        print(f"  Regime: {latest.get('regime', 'UNKNOWN')}")
        print(f"  Funding: {latest.get('funding_rate', 0)*100:.4f}%")
        print(f"  L/S: {latest.get('long_pct', 50):.1f}% / {latest.get('short_pct', 50):.1f}%")
        
        # Historical edge for current regime
        current_regime = latest.get('regime', '')
        if current_regime in regime_stats:
            stats = regime_stats[current_regime]
            print(f"\n  HISTORICAL EDGE FOR {current_regime}:")
            print(f"    Sample size: {stats['count']}")
            print(f"    Up rate: {stats.get('up_rate', 0):.1f}%")
            print(f"    Avg change: {stats['avg_price_change']:+.3f}%")
            
            if stats.get('up_rate', 50) > 60:
                print(f"    -> BULLISH BIAS historically")
            elif stats.get('up_rate', 50) < 40:
                print(f"    -> BEARISH BIAS historically")
            else:
                print(f"    -> NO CLEAR EDGE")
    
    print(f"\n{'='*70}")


if __name__ == '__main__':
    print_analysis()
