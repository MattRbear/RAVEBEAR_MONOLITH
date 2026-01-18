"""
Stablecoin Flow Collector
Pulls from Base44 API - real stablecoin data, mint/burn volumes.
"""

import sys
import time
import requests
from datetime import datetime, timezone
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("pip install pandas pyarrow requests")
    sys.exit(1)

OUTPUT_DIR = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\Stablecoin_Flow")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

API_KEY = "4e74006674014662b63ada48b3b5b7b3"
API_BASE = "https://app.base44.com/api/apps/692da87b98a5f8b26242d9cf/entities"


def fetch_stablecoin_data():
    """Pull latest stablecoin snapshot from Base44."""
    url = f"{API_BASE}/StablecoinSnapshot"
    headers = {
        'api_key': API_KEY,
        'Content-Type': 'application/json'
    }
    
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_latest_by_symbol(raw_list):
    """Get the most recent record for each symbol."""
    by_symbol = {}
    for item in raw_list:
        symbol = item.get('symbol', 'UNKNOWN')
        created = item.get('created_date', '')
        
        if symbol not in by_symbol or created > by_symbol[symbol].get('created_date', ''):
            by_symbol[symbol] = item
    
    return by_symbol


def parse_snapshot(raw_list):
    """Parse the API response - get latest record per symbol."""
    ts = int(time.time() * 1000)
    
    # Get latest record for each symbol
    stables = get_latest_by_symbol(raw_list)
    
    # Extract values
    usdt = stables.get('USDT', {})
    usdc = stables.get('USDC', {})
    dai = stables.get('DAI', {})
    
    # Calculate total
    total_stable = sum(
        s.get('circulating_supply', 0) for s in stables.values()
    )
    
    snapshot = {
        'timestamp': ts,
        'datetime': datetime.now(timezone.utc).isoformat(),
        
        # USDT
        'usdt_supply': usdt.get('circulating_supply', 0),
        'usdt_mint_24h': usdt.get('mint_volume_24h', 0),
        'usdt_burn_24h': usdt.get('burn_volume_24h', 0),
        'usdt_change_pct': usdt.get('supply_change_pct', 0),
        'usdt_velocity': usdt.get('velocity_score', 0),
        
        # USDC
        'usdc_supply': usdc.get('circulating_supply', 0),
        'usdc_mint_24h': usdc.get('mint_volume_24h', 0),
        'usdc_burn_24h': usdc.get('burn_volume_24h', 0),
        'usdc_change_pct': usdc.get('supply_change_pct', 0),
        
        # DAI
        'dai_supply': dai.get('circulating_supply', 0),
        'dai_change_pct': dai.get('supply_change_pct', 0),
        
        # Totals
        'total_stable_supply': total_stable,
        
        # Net flow
        'usdt_net_flow': usdt.get('mint_volume_24h', 0) - usdt.get('burn_volume_24h', 0),
        'usdc_net_flow': usdc.get('mint_volume_24h', 0) - usdc.get('burn_volume_24h', 0),
    }
    
    return snapshot


def load_existing():
    path = OUTPUT_DIR / "stablecoin_flow.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def save_data(df):
    df = df.sort_values('timestamp').drop_duplicates(subset=['timestamp'], keep='last')
    df.to_parquet(OUTPUT_DIR / "stablecoin_flow.parquet", index=False)
    df.to_csv(OUTPUT_DIR / "stablecoin_flow.csv", index=False)


def collect_once():
    """Collect one snapshot."""
    raw = fetch_stablecoin_data()
    snapshot = parse_snapshot(raw)
    
    print(f"\n{'='*60}")
    print(f"  STABLECOIN FLOW | {snapshot['datetime'][:19]}")
    print(f"{'='*60}")
    
    print(f"\n  USDT:")
    print(f"    Supply:     ${snapshot['usdt_supply']:>20,.0f}")
    print(f"    Mint 24h:   ${snapshot['usdt_mint_24h']:>20,.0f}")
    print(f"    Burn 24h:   ${snapshot['usdt_burn_24h']:>20,.0f}")
    print(f"    Net Flow:   ${snapshot['usdt_net_flow']:>20,.0f}")
    
    print(f"\n  USDC:")
    print(f"    Supply:     ${snapshot['usdc_supply']:>20,.0f}")
    print(f"    Net Flow:   ${snapshot['usdc_net_flow']:>20,.0f}")
    
    print(f"\n  TOTAL STABLE: ${snapshot['total_stable_supply']:>20,.0f}")
    print(f"{'='*60}")
    
    existing = load_existing()
    new_row = pd.DataFrame([snapshot])
    combined = pd.concat([existing, new_row], ignore_index=True)
    save_data(combined)
    print(f"Saved. Total records: {len(combined)}")
    
    return snapshot


def collect_continuous(interval=60):
    """Collect continuously."""
    print(f"Collecting stablecoin flow every {interval} seconds...")
    print("Ctrl+C to stop\n")
    
    while True:
        try:
            collect_once()
            time.sleep(interval)
        except KeyboardInterrupt:
            print("\nStopped.")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(interval)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--continuous', '-c', action='store_true')
    parser.add_argument('--interval', '-i', type=int, default=60)
    args = parser.parse_args()
    
    if args.continuous:
        collect_continuous(args.interval)
    else:
        collect_once()


if __name__ == '__main__':
    main()
