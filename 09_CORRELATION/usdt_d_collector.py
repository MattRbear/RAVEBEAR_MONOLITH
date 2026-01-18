"""
USDT.D Collector
Pulls USDT dominance data and stores it.
"""

import os
import sys
import time
import json
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("pip install pandas pyarrow")
    sys.exit(1)

OUTPUT_DIR = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\USDT_D")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

COINGECKO_API = "https://api.coingecko.com/api/v3"


def fetch_global_data():
    """Get total market cap and USDT market cap."""
    url = f"{COINGECKO_API}/global"
    req = urllib.request.Request(url, headers={"User-Agent": "RaveBear/1.0"})
    
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode('utf-8'))
    
    total_mcap = data['data']['total_market_cap']['usd']
    
    # Get USDT market cap
    url2 = f"{COINGECKO_API}/simple/price?ids=tether&vs_currencies=usd&include_market_cap=true"
    req2 = urllib.request.Request(url2, headers={"User-Agent": "RaveBear/1.0"})
    
    with urllib.request.urlopen(req2, timeout=30) as resp2:
        data2 = json.loads(resp2.read().decode('utf-8'))
    
    usdt_mcap = data2['tether']['usd_market_cap']
    
    usdt_d = (usdt_mcap / total_mcap) * 100
    
    return {
        'timestamp': int(time.time() * 1000),
        'usdt_d': round(usdt_d, 4),
        'usdt_mcap': usdt_mcap,
        'total_mcap': total_mcap,
    }


def load_existing():
    path = OUTPUT_DIR / "usdt_d.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def save_data(df):
    df = df.sort_values('timestamp').drop_duplicates(subset=['timestamp'], keep='last')
    df.to_parquet(OUTPUT_DIR / "usdt_d.parquet", index=False)
    df.to_csv(OUTPUT_DIR / "usdt_d.csv", index=False)
    print(f"Saved {len(df)} records")


def collect_once():
    """Collect one data point."""
    data = fetch_global_data()
    dt = datetime.fromtimestamp(data['timestamp'] / 1000, tz=timezone.utc)
    print(f"USDT.D: {data['usdt_d']:.4f}% at {dt.strftime('%Y-%m-%d %H:%M:%S')}")
    
    existing = load_existing()
    new_row = pd.DataFrame([data])
    combined = pd.concat([existing, new_row], ignore_index=True)
    save_data(combined)


def collect_continuous(interval=60):
    """Collect continuously."""
    print(f"Collecting USDT.D every {interval} seconds...")
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
