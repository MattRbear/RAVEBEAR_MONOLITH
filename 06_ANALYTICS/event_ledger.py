"""
Event Ledger v3
Logs when levels get touched/crossed with FULL market state.
Includes: stablecoin + derivatives + WHALE FLOW data
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path
import sys

LEDGER_PATH = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\event_ledger.json")

# Try to import Coinalyze client
sys.path.insert(0, str(Path(r"C:\Users\M.R Bear\Documents\Coin_anal")))
try:
    from coinalyze_client import CoinalyzeClient
    COINALYZE_AVAILABLE = True
except:
    COINALYZE_AVAILABLE = False


def load_ledger():
    if LEDGER_PATH.exists():
        with open(LEDGER_PATH) as f:
            return json.load(f)
    return {'events': []}


def save_ledger(ledger):
    with open(LEDGER_PATH, 'w') as f:
        json.dump(ledger, f, indent=2)


def get_derivatives_state():
    """Get current derivatives state from Coinalyze."""
    if not COINALYZE_AVAILABLE:
        return None
    
    try:
        client = CoinalyzeClient()
        
        oi_data = client.get_open_interest()
        funding_data = client.get_funding_rate()
        ls_history = client.get_long_short_ratio_history(interval="1hour", hours=1)
        
        ls_latest = ls_history[-1] if ls_history else {}
        
        return {
            'oi_value': oi_data[0].get('value', 0) if oi_data else 0,
            'funding_rate': funding_data[0].get('value', 0) if funding_data else 0,
            'long_pct': ls_latest.get('l', 50) if isinstance(ls_latest, dict) else 50,
            'short_pct': ls_latest.get('s', 50) if isinstance(ls_latest, dict) else 50,
            'ls_ratio': ls_latest.get('r', 1) if isinstance(ls_latest, dict) else 1,
        }
    except Exception as e:
        print(f"Derivatives fetch error: {e}")
        return None


def log_event(
    event_type: str,
    level_price: float,
    level_type: str,
    level_dir: str,
    btc_price: float,
    stablecoin_state: dict = None,
    derivatives_state: dict = None,
    extra: dict = None
):
    """
    Log an event when a level is touched/crossed.
    
    event_type: 'TOUCHED', 'CROSSED', 'INVALIDATED'
    level_type: 'WICK', 'POOR'
    level_dir: 'UP', 'DN', 'HI', 'LO'
    stablecoin_state: dict with usdt_supply, usdc_supply, etc.
    derivatives_state: dict with oi_value, funding_rate, long_pct, etc.
    extra: dict with whale data and other info
    """
    ledger = load_ledger()
    
    event = {
        'id': len(ledger['events']) + 1,
        'timestamp': int(time.time() * 1000),
        'datetime': datetime.now(timezone.utc).isoformat(),
        'event_type': event_type,
        'level_price': level_price,
        'level_type': level_type,
        'level_dir': level_dir,
        'btc_price': btc_price,
        'distance': btc_price - level_price,
        'distance_pct': ((btc_price - level_price) / level_price) * 100 if level_price else 0,
    }
    
    # Stablecoin state
    if stablecoin_state:
        event['usdt_supply'] = stablecoin_state.get('usdt_supply', 0)
        event['usdc_supply'] = stablecoin_state.get('usdc_supply', 0)
        event['usdt_net_flow'] = stablecoin_state.get('usdt_net_flow', 0)
        event['total_stable'] = stablecoin_state.get('total_stable_supply', 0)
    
    # Derivatives state
    if derivatives_state:
        event['oi_value'] = derivatives_state.get('oi_value', 0)
        event['funding_rate'] = derivatives_state.get('funding_rate', 0)
        event['long_pct'] = derivatives_state.get('long_pct', 50)
        event['short_pct'] = derivatives_state.get('short_pct', 50)
        event['ls_ratio'] = derivatives_state.get('ls_ratio', 1)
    
    # Extra data (includes whale flow)
    if extra:
        event.update(extra)
    
    ledger['events'].append(event)
    
    # Keep last 2000 events
    if len(ledger['events']) > 2000:
        ledger['events'] = ledger['events'][-2000:]
    
    save_ledger(ledger)
    
    return event


def get_recent_events(n=20):
    ledger = load_ledger()
    return ledger['events'][-n:]


def get_events_by_type(event_type: str):
    ledger = load_ledger()
    return [e for e in ledger['events'] if e['event_type'] == event_type]


def get_events_with_whale_data():
    """Get events that have whale flow data."""
    ledger = load_ledger()
    return [e for e in ledger['events'] if e.get('whale_btc_net_flow') is not None]


def analyze_whale_correlation():
    """Analyze if whale flow correlates with level outcomes."""
    ledger = load_ledger()
    events = ledger['events']
    
    # Events with whale data
    whale_events = [e for e in events if e.get('whale_btc_net_flow') is not None]
    
    if len(whale_events) < 5:
        return "Need more events with whale data"
    
    # Split by whale flow direction
    inflow_events = [e for e in whale_events if e.get('whale_btc_net_flow', 0) > 0]
    outflow_events = [e for e in whale_events if e.get('whale_btc_net_flow', 0) < 0]
    
    # Count touched vs crossed for each
    def outcome_ratio(events_list):
        if not events_list:
            return 0, 0, 0
        touched = len([e for e in events_list if e['event_type'] == 'TOUCHED'])
        crossed = len([e for e in events_list if e['event_type'] == 'CROSSED'])
        total = touched + crossed
        return touched, crossed, (touched / total * 100) if total > 0 else 0
    
    inflow_t, inflow_c, inflow_pct = outcome_ratio(inflow_events)
    outflow_t, outflow_c, outflow_pct = outcome_ratio(outflow_events)
    
    return {
        'total_whale_events': len(whale_events),
        'inflow_events': len(inflow_events),
        'inflow_touched': inflow_t,
        'inflow_crossed': inflow_c,
        'inflow_hold_rate': inflow_pct,
        'outflow_events': len(outflow_events),
        'outflow_touched': outflow_t,
        'outflow_crossed': outflow_c,
        'outflow_hold_rate': outflow_pct,
    }


def print_ledger_summary():
    ledger = load_ledger()
    events = ledger['events']
    
    print(f"\n{'='*65}")
    print(f"  EVENT LEDGER SUMMARY")
    print(f"{'='*65}")
    print(f"  Total events: {len(events)}")
    
    if events:
        touched = len([e for e in events if e['event_type'] == 'TOUCHED'])
        crossed = len([e for e in events if e['event_type'] == 'CROSSED'])
        
        print(f"  TOUCHED: {touched} ({touched/len(events)*100:.1f}%)")
        print(f"  CROSSED: {crossed} ({crossed/len(events)*100:.1f}%)")
        
        # Events with different data types
        with_derivs = len([e for e in events if e.get('oi_value')])
        with_whales = len([e for e in events if e.get('whale_btc_net_flow') is not None])
        print(f"  With derivatives: {with_derivs}")
        print(f"  With whale data: {with_whales}")
        
        # Whale correlation
        if with_whales >= 5:
            print(f"\n  WHALE FLOW CORRELATION:")
            corr = analyze_whale_correlation()
            if isinstance(corr, dict):
                print(f"    Inflow (bearish): {corr['inflow_events']} events, {corr['inflow_hold_rate']:.1f}% held")
                print(f"    Outflow (bullish): {corr['outflow_events']} events, {corr['outflow_hold_rate']:.1f}% held")
        
        print(f"\n  LAST 10 EVENTS:")
        print(f"  {'TYPE':<10} {'LEVEL':<8} {'PRICE':>12} {'FUND':>10} {'WHALE':>12}")
        print(f"  {'-'*58}")
        
        for e in events[-10:]:
            lvl = f"{e['level_type']}-{e['level_dir']}"
            funding = e.get('funding_rate', 0) * 100 if e.get('funding_rate') else 0
            whale = e.get('whale_btc_net_flow', 0)
            whale_str = f"${whale/1e6:+.1f}M" if whale else "N/A"
            print(f"  {e['event_type']:<10} {lvl:<8} ${e['level_price']:>10,.2f} {funding:>9.4f}% {whale_str:>12}")
    
    print(f"{'='*65}")


if __name__ == '__main__':
    print_ledger_summary()
