"""
OKX 1-Minute Candle Collector
Pulls 1m candles from OKX REST API and stores in Parquet format.
Simple. Honest. Reliable.
"""

import os
import time
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional

# Try to import pandas/pyarrow for Parquet, fall back to CSV if not available
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("WARNING: pandas not installed. Using CSV format only.")
    print("Install with: pip install pandas pyarrow")


class OKXCandleCollector:
    """Collects 1m candles from OKX and stores them properly."""
    
    BASE_URL = "https://www.okx.com/api/v5/market/history-candles"
    CANDLES_PER_REQUEST = 100  # OKX max
    
    def __init__(
        self,
        symbol: str = "BTC-USDT-SWAP",
        storage_dir: str = r"C:\Users\M.R Bear\Documents\Data_Vault\1m_Candles",
    ):
        self.symbol = symbol
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
        # File paths
        self.parquet_path = self.storage_dir / f"{symbol.replace('-', '_')}_1m.parquet"
        self.csv_path = self.storage_dir / f"{symbol.replace('-', '_')}_1m.csv"
        self.meta_path = self.storage_dir / f"{symbol.replace('-', '_')}_1m_meta.json"
        
        # Load existing metadata
        self.meta = self._load_meta()
    
    def _load_meta(self) -> Dict:
        """Load metadata about what's already collected."""
        if self.meta_path.exists():
            with open(self.meta_path, 'r') as f:
                return json.load(f)
        return {
            'symbol': self.symbol,
            'oldest_ts': None,
            'newest_ts': None,
            'total_candles': 0,
            'last_updated': None,
        }
    
    def _save_meta(self):
        """Save metadata."""
        self.meta['last_updated'] = datetime.now(timezone.utc).isoformat()
        with open(self.meta_path, 'w') as f:
            json.dump(self.meta, f, indent=2)
    
    def _fetch_candles(self, after: Optional[int] = None) -> List[Dict]:
        """
        Fetch candles from OKX.
        
        Args:
            after: Timestamp in ms. Returns candles OLDER than this.
        
        Returns:
            List of candle dicts sorted oldest to newest.
        """
        url = f"{self.BASE_URL}?instId={self.symbol}&bar=1m&limit={self.CANDLES_PER_REQUEST}"
        if after:
            url += f"&after={after}"
        
        req = urllib.request.Request(url, headers={
            "User-Agent": "RaveBear-Collector/1.0",
            "Accept": "application/json",
        })
        
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode('utf-8'))
        except urllib.error.URLError as e:
            print(f"ERROR: Failed to fetch candles: {e}")
            return []
        
        if data.get('code') != '0':
            print(f"ERROR: OKX API error: {data.get('msg', 'Unknown error')}")
            return []
        
        raw_candles = data.get('data', [])
        
        # Convert to structured format
        # OKX format: [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
        candles = []
        invalid_count = 0
        for c in raw_candles:
            try:
                o, h, l, cl = float(c[1]), float(c[2]), float(c[3]), float(c[4])
                
                # Validate OHLC
                if not (l <= o <= h and l <= cl <= h):
                    invalid_count += 1
                    continue
                
                # Validate price range
                if not (10000 <= cl <= 500000):
                    invalid_count += 1
                    continue
                
                candles.append({
                    'timestamp': int(c[0]),
                    'open': o,
                    'high': h,
                    'low': l,
                    'close': cl,
                    'volume': float(c[5]),
                    'volume_ccy': float(c[6]),
                    'volume_quote': float(c[7]),
                    'confirmed': c[8] == '1',
                })
            except (ValueError, IndexError) as e:
                invalid_count += 1
                continue
        
        if invalid_count > 0:
            print(f"  [!] Skipped {invalid_count} invalid candles")
        
        # OKX returns newest first, reverse to oldest first
        candles.reverse()
        return candles
    
    def _load_existing(self) -> pd.DataFrame:
        """Load existing candles from storage."""
        if not HAS_PANDAS:
            return None
        
        if self.parquet_path.exists():
            try:
                return pd.read_parquet(self.parquet_path)
            except Exception as e:
                print(f"WARNING: Could not read parquet: {e}")
        
        if self.csv_path.exists():
            try:
                return pd.read_csv(self.csv_path)
            except Exception as e:
                print(f"WARNING: Could not read csv: {e}")
        
        return pd.DataFrame()
    
    def _save_candles(self, df: pd.DataFrame):
        """Save candles to storage."""
        if not HAS_PANDAS or df.empty:
            return
        
        # Sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['timestamp'], keep='last')
        
        # Save as Parquet (primary - machine optimized)
        try:
            df.to_parquet(self.parquet_path, index=False, compression='snappy')
            print(f"  Saved {len(df)} candles to Parquet")
        except Exception as e:
            print(f"WARNING: Could not save parquet: {e}")
        
        # Save as CSV (backup - human readable)
        try:
            df.to_csv(self.csv_path, index=False)
        except Exception as e:
            print(f"WARNING: Could not save csv: {e}")
        
        # Update metadata
        self.meta['oldest_ts'] = int(df['timestamp'].min())
        self.meta['newest_ts'] = int(df['timestamp'].max())
        self.meta['total_candles'] = len(df)
        self._save_meta()

    def collect_historical(self, days: int = 30, progress_interval: int = 1000):
        """
        Collect historical candles going back N days.
        
        Args:
            days: How many days of history to collect
            progress_interval: Print progress every N candles
        """
        if not HAS_PANDAS:
            print("ERROR: pandas required for historical collection")
            return
        
        print(f"Collecting {days} days of 1m candles for {self.symbol}...")
        target_candles = days * 24 * 60  # minutes in N days
        print(f"Target: ~{target_candles} candles")
        
        # Load existing data
        existing = self._load_existing()
        if not existing.empty:
            print(f"Found {len(existing)} existing candles")
        
        # Start from now and work backwards
        end_ts = int(time.time() * 1000)
        start_ts = end_ts - (days * 24 * 60 * 60 * 1000)
        
        all_candles = []
        after = None
        request_count = 0
        
        while True:
            candles = self._fetch_candles(after=after)
            request_count += 1
            
            if not candles:
                print("No more candles available")
                break
            
            all_candles.extend(candles)
            oldest_ts = candles[0]['timestamp']
            
            if len(all_candles) % progress_interval < len(candles):
                dt = datetime.fromtimestamp(oldest_ts / 1000, tz=timezone.utc)
                print(f"  Collected {len(all_candles)} candles, oldest: {dt.strftime('%Y-%m-%d %H:%M')}")
            
            # Check if we've gone far enough back
            if oldest_ts <= start_ts:
                print(f"Reached target date after {request_count} requests")
                break
            
            # Set pagination cursor
            after = oldest_ts
            
            # Rate limiting - OKX allows 20 req/2s for public endpoints
            time.sleep(0.15)
        
        # Filter to target range
        all_candles = [c for c in all_candles if c['timestamp'] >= start_ts]
        
        print(f"\nFetched {len(all_candles)} new candles")
        
        # Merge with existing
        new_df = pd.DataFrame(all_candles)
        if not existing.empty:
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df
        
        # Save
        self._save_candles(combined)
        print(f"Total candles in storage: {self.meta['total_candles']}")
    
    def collect_new(self):
        """
        Collect only new candles since last collection.
        Call this periodically to stay current.
        """
        if not HAS_PANDAS:
            print("ERROR: pandas required")
            return
        
        existing = self._load_existing()
        
        if existing.empty:
            print("No existing data. Run collect_historical() first.")
            return
        
        newest_ts = int(existing['timestamp'].max())
        now_ts = int(time.time() * 1000)
        
        # How many minutes behind?
        gap_minutes = (now_ts - newest_ts) / 60000
        print(f"Current data is {gap_minutes:.1f} minutes behind")
        
        if gap_minutes < 1:
            print("Already up to date")
            return
        
        # Fetch recent candles (no pagination needed for small gaps)
        all_new = []
        after = None
        
        while True:
            candles = self._fetch_candles(after=after)
            if not candles:
                break
            
            # Filter to only candles newer than what we have
            new_candles = [c for c in candles if c['timestamp'] > newest_ts]
            all_new.extend(new_candles)
            
            # If oldest fetched is newer than our newest, keep going back
            oldest_fetched = candles[0]['timestamp']
            if oldest_fetched <= newest_ts:
                break
            
            after = oldest_fetched
            time.sleep(0.15)
        
        if not all_new:
            print("No new candles")
            return
        
        print(f"Found {len(all_new)} new candles")
        
        # Merge and save
        new_df = pd.DataFrame(all_new)
        combined = pd.concat([existing, new_df], ignore_index=True)
        self._save_candles(combined)
        print(f"Total candles: {self.meta['total_candles']}")
    
    def run_continuous(self, interval_seconds: int = 60):
        """
        Run continuously, collecting new candles every interval.
        
        Args:
            interval_seconds: How often to check for new candles
        """
        print(f"Starting continuous collection for {self.symbol}")
        print(f"Checking every {interval_seconds} seconds")
        print("Press Ctrl+C to stop\n")
        
        while True:
            try:
                self.collect_new()
                time.sleep(interval_seconds)
            except KeyboardInterrupt:
                print("\nStopped by user")
                break
            except Exception as e:
                print(f"ERROR: {e}")
                time.sleep(interval_seconds)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='OKX 1-Minute Candle Collector',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect 30 days of BTC history
  python okx_collector.py --symbol BTC-USDT-SWAP --days 30
  
  # Collect 7 days of ETH history
  python okx_collector.py --symbol ETH-USDT-SWAP --days 7
  
  # Update existing data with new candles
  python okx_collector.py --symbol BTC-USDT-SWAP --update
  
  # Run continuously (updates every 60 seconds)
  python okx_collector.py --symbol BTC-USDT-SWAP --continuous
  
  # Collect multiple symbols
  python okx_collector.py --symbol BTC-USDT-SWAP ETH-USDT-SWAP SOL-USDT-SWAP --days 30
        """
    )
    
    parser.add_argument(
        '--symbol', '-s',
        nargs='+',
        default=['BTC-USDT-SWAP'],
        help='Symbol(s) to collect (default: BTC-USDT-SWAP)'
    )
    
    parser.add_argument(
        '--days', '-d',
        type=int,
        default=30,
        help='Days of history to collect (default: 30)'
    )
    
    parser.add_argument(
        '--update', '-u',
        action='store_true',
        help='Only fetch new candles since last collection'
    )
    
    parser.add_argument(
        '--continuous', '-c',
        action='store_true',
        help='Run continuously, updating every minute'
    )
    
    parser.add_argument(
        '--interval', '-i',
        type=int,
        default=60,
        help='Interval in seconds for continuous mode (default: 60)'
    )
    
    parser.add_argument(
        '--output', '-o',
        default=r'C:\Users\M.R Bear\Documents\Data_Vault\1m_Candles',
        help='Output directory for candle data'
    )
    
    args = parser.parse_args()
    
    for symbol in args.symbol:
        print(f"\n{'='*60}")
        print(f"Processing: {symbol}")
        print('='*60)
        
        collector = OKXCandleCollector(
            symbol=symbol,
            storage_dir=args.output,
        )
        
        if args.continuous:
            collector.run_continuous(interval_seconds=args.interval)
        elif args.update:
            collector.collect_new()
        else:
            collector.collect_historical(days=args.days)
    
    print("\nDone.")


if __name__ == '__main__':
    main()
