"""
Correlation Analyzer
Reads the event ledger and finds patterns between level touches and stablecoin state.
"""

import json
from pathlib import Path
import pandas as pd
import numpy as np

LEDGER_PATH = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\event_ledger.json")
OUTPUT_DIR = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\Analytics")


def load_events():
    if LEDGER_PATH.exists():
        with open(LEDGER_PATH) as f:
            data = json.load(f)
            return pd.DataFrame(data.get('events', []))
    return pd.DataFrame()


def analyze_correlations():
    events = load_events()
    
    if events.empty:
        print("No events logged yet. Run the dashboard and wait for levels to get touched.")
        return
    
    print(f"\n{'='*70}")
    print(f"  CORRELATION ANALYSIS")
    print(f"  Total events: {len(events)}")
    print(f"{'='*70}")
    
    # Basic stats
    touched = events[events['event_type'] == 'TOUCHED']
    crossed = events[events['event_type'] == 'CROSSED']
    
    print(f"\n  EVENT BREAKDOWN:")
    print(f"    TOUCHED: {len(touched)} ({100*len(touched)/len(events):.1f}%)")
    print(f"    CROSSED: {len(crossed)} ({100*len(crossed)/len(events):.1f}%)")
    
    # By level type
    print(f"\n  BY LEVEL TYPE:")
    for ltype in events['level_type'].unique():
        subset = events[events['level_type'] == ltype]
        print(f"    {ltype}: {len(subset)} events")
    
    # By direction
    print(f"\n  BY DIRECTION:")
    for ldir in events['level_dir'].unique():
        subset = events[events['level_dir'] == ldir]
        t = len(subset[subset['event_type'] == 'TOUCHED'])
        c = len(subset[subset['event_type'] == 'CROSSED'])
        print(f"    {ldir}: {len(subset)} events (T:{t} / C:{c})")
    
    # Stablecoin correlation (if we have the data)
    if 'usdt_supply' in events.columns:
        events_with_stable = events[events['usdt_supply'] > 0]
        
        if len(events_with_stable) > 5:
            print(f"\n  STABLECOIN STATE AT EVENTS:")
            
            # Group by event type
            for etype in ['TOUCHED', 'CROSSED']:
                subset = events_with_stable[events_with_stable['event_type'] == etype]
                if len(subset) > 0:
                    avg_usdt = subset['usdt_supply'].mean() / 1e9
                    avg_flow = subset['usdt_net_flow'].mean() / 1e6
                    print(f"\n    {etype}:")
                    print(f"      Avg USDT Supply: ${avg_usdt:.2f}B")
                    print(f"      Avg Net Flow:    ${avg_flow:+.2f}M")
            
            # Correlation between net flow and crossed vs touched
            if 'usdt_net_flow' in events_with_stable.columns:
                crossed_flow = events_with_stable[events_with_stable['event_type'] == 'CROSSED']['usdt_net_flow'].mean()
                touched_flow = events_with_stable[events_with_stable['event_type'] == 'TOUCHED']['usdt_net_flow'].mean()
                
                print(f"\n    FLOW COMPARISON:")
                print(f"      When CROSSED: ${crossed_flow/1e6:+.2f}M avg flow")
                print(f"      When TOUCHED: ${touched_flow/1e6:+.2f}M avg flow")
                
                if crossed_flow > touched_flow:
                    print(f"      -> CROSSED happens more during inflows")
                else:
                    print(f"      -> TOUCHED happens more during inflows")
    
    # Distance patterns
    if 'distance' in events.columns:
        print(f"\n  DISTANCE FROM CURRENT PRICE AT EVENT:")
        print(f"    Avg distance: ${events['distance'].abs().mean():.2f}")
        print(f"    Max distance: ${events['distance'].abs().max():.2f}")
    
    # Save detailed analysis
    events.to_csv(OUTPUT_DIR / "events_analysis.csv", index=False)
    print(f"\n  Detailed data saved to: {OUTPUT_DIR / 'events_analysis.csv'}")
    
    print(f"\n  NOTE: More events = better correlation data.")
    print(f"  Keep the dashboard running to collect more.")


if __name__ == '__main__':
    analyze_correlations()
