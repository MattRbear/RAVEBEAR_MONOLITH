"""
DATA INTEGRITY AUDIT SUITE - FIXED VERSION
==========================================
Only checks actual venue directories, not leftover folders.
"""
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

import pyarrow.parquet as pq
import pandas as pd

# Expected schema
REQUIRED_COLUMNS = {
    "venue": "string",
    "symbol": "string",
    "timeframe": "string",
    "open_time_ms": "int64",
    "close_time_ms": "int64",
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "float64",
}

TIMEFRAME_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}

DATA_DIR = Path("data")

# ONLY check these venues (ignore other folders)
KNOWN_VENUES = {"coinbase", "kraken", "okx", "binance"}


class AuditResult:
    """Collect audit results."""
    
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.warnings = 0
        self.errors = []
        self.warnings_list = []
    
    def ok(self, msg: str):
        self.passed += 1
        print(f"  ✅ {msg}")
    
    def fail(self, msg: str):
        self.failed += 1
        self.errors.append(msg)
        print(f"  ❌ {msg}")
    
    def warn(self, msg: str):
        self.warnings += 1
        self.warnings_list.append(msg)
        print(f"  ⚠️  {msg}")
    
    def summary(self):
        print("\n" + "=" * 60)
        print("AUDIT SUMMARY")
        print("=" * 60)
        print(f"  Passed:   {self.passed}")
        print(f"  Failed:   {self.failed}")
        print(f"  Warnings: {self.warnings}")
        
        if self.failed > 0:
            print("\n  FAILURES:")
            for err in self.errors[:10]:
                print(f"    - {err}")
            if len(self.errors) > 10:
                print(f"    ... and {len(self.errors) - 10} more")
        
        print("=" * 60)
        return self.failed == 0


def get_venue_dirs():
    """Get only actual venue directories."""
    venues = []
    for d in DATA_DIR.iterdir():
        if d.is_dir() and d.name.lower() in KNOWN_VENUES:
            venues.append(d)
    return venues


def audit_schema(result: AuditResult, verbose: bool = False):
    """Check parquet file schemas."""
    print("\n[1] SCHEMA VALIDATION")
    print("-" * 40)
    
    parquet_files = []
    for venue_dir in get_venue_dirs():
        parquet_files.extend(venue_dir.rglob("*.parquet"))
    
    if not parquet_files:
        result.fail("No parquet files found in venue directories")
        return
    
    result.ok(f"Found {len(parquet_files)} parquet files")
    
    schema_issues = 0
    for pf in parquet_files:
        try:
            schema = pq.read_schema(pf)
            col_names = set(schema.names)
            
            missing = set(REQUIRED_COLUMNS.keys()) - col_names
            if missing:
                result.fail(f"{pf.name}: Missing columns {missing}")
                schema_issues += 1
        except Exception as e:
            result.fail(f"{pf.name}: Cannot read schema - {e}")
            schema_issues += 1
    
    if schema_issues == 0:
        result.ok("All files have required columns")


def audit_freshness(result: AuditResult, max_stale_minutes: int = 10):
    """Check data freshness."""
    print("\n[2] DATA FRESHNESS")
    print("-" * 40)
    
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    max_stale_ms = max_stale_minutes * 60 * 1000
    
    for venue_dir in get_venue_dirs():
        venue = venue_dir.name
        latest_ms = 0
        
        for pf in venue_dir.rglob("*.parquet"):
            try:
                df = pd.read_parquet(pf, columns=["open_time_ms"])
                if len(df) > 0:
                    latest_ms = max(latest_ms, df["open_time_ms"].max())
            except:
                pass
        
        if latest_ms > 0:
            stale_ms = now_ms - latest_ms
            stale_min = stale_ms / 60_000
            
            if stale_min > max_stale_minutes:
                result.warn(f"{venue}: Data is {stale_min:.1f} min stale")
            else:
                result.ok(f"{venue}: Fresh ({stale_min:.1f} min old)")
        else:
            result.fail(f"{venue}: No data found")


def audit_completeness(result: AuditResult, expected_symbols: dict = None):
    """Check all expected symbols are present."""
    print("\n[3] SYMBOL COMPLETENESS")
    print("-" * 40)
    
    # Default expected symbols (core only)
    if expected_symbols is None:
        expected_symbols = {
            "coinbase": ["BTC-USD", "ETH-USD"],
            "kraken": ["BTC/USD", "ETH/USD"],
            "okx": ["BTC-USDT", "ETH-USDT"],
        }
    
    for venue_dir in get_venue_dirs():
        venue = venue_dir.name
        if venue not in expected_symbols:
            continue
        
        symbols = expected_symbols[venue]
        found_symbols = set()
        
        for symbol_dir in venue_dir.iterdir():
            if symbol_dir.is_dir():
                found_symbols.add(symbol_dir.name)
        
        missing = set(symbols) - found_symbols
        if missing:
            result.warn(f"{venue}: Missing symbols {missing}")
        else:
            result.ok(f"{venue}: Core symbols present ({len(found_symbols)} total)")


def audit_duplicates(result: AuditResult, sample_files: int = 20):
    """Check for duplicate candles."""
    print("\n[4] DUPLICATE CHECK")
    print("-" * 40)
    
    parquet_files = []
    for venue_dir in get_venue_dirs():
        parquet_files.extend(list(venue_dir.rglob("*.parquet"))[:sample_files])
    
    total_rows = 0
    total_dupes = 0
    
    for pf in parquet_files[:sample_files]:
        try:
            df = pd.read_parquet(pf)
            total_rows += len(df)
            
            key_cols = ["venue", "symbol", "timeframe", "open_time_ms"]
            available_keys = [c for c in key_cols if c in df.columns]
            
            if available_keys:
                dupes = df.duplicated(subset=available_keys, keep=False).sum()
                if dupes > 0:
                    total_dupes += dupes
                    result.fail(f"{pf.name}: {dupes} duplicate rows")
        except Exception as e:
            result.warn(f"{pf.name}: Cannot check - {e}")
    
    if total_dupes == 0:
        result.ok(f"No duplicates in {min(len(parquet_files), sample_files)} sampled files ({total_rows} rows)")


def audit_ohlc_sanity(result: AuditResult, sample_files: int = 20):
    """Verify OHLC data makes sense."""
    print("\n[5] OHLC SANITY")
    print("-" * 40)
    
    parquet_files = []
    for venue_dir in get_venue_dirs():
        parquet_files.extend(list(venue_dir.rglob("*.parquet"))[:sample_files])
    
    issues = defaultdict(int)
    rows_checked = 0
    
    for pf in parquet_files[:sample_files]:
        try:
            df = pd.read_parquet(pf)
            rows_checked += len(df)
            
            # High >= Low
            bad_hl = (df["high"] < df["low"]).sum()
            if bad_hl > 0:
                issues["high < low"] += bad_hl
            
            # High >= Open and Close
            bad_ho = (df["high"] < df["open"]).sum()
            bad_hc = (df["high"] < df["close"]).sum()
            if bad_ho > 0:
                issues["high < open"] += bad_ho
            if bad_hc > 0:
                issues["high < close"] += bad_hc
            
            # Low <= Open and Close
            bad_lo = (df["low"] > df["open"]).sum()
            bad_lc = (df["low"] > df["close"]).sum()
            if bad_lo > 0:
                issues["low > open"] += bad_lo
            if bad_lc > 0:
                issues["low > close"] += bad_lc
            
            # Positive prices
            neg_prices = ((df["open"] <= 0) | (df["high"] <= 0) | 
                         (df["low"] <= 0) | (df["close"] <= 0)).sum()
            if neg_prices > 0:
                issues["non-positive price"] += neg_prices
            
            # Negative volume (allow 0)
            if "volume" in df.columns:
                neg_vol = (df["volume"] < 0).sum()
                if neg_vol > 0:
                    issues["negative volume"] += neg_vol
                    
        except Exception as e:
            result.warn(f"{pf.name}: Cannot check - {e}")
    
    if not issues:
        result.ok(f"All OHLC data valid ({rows_checked} rows checked)")
    else:
        for issue, count in issues.items():
            result.fail(f"{issue}: {count} rows")


def audit_timestamp_alignment(result: AuditResult, sample_files: int = 20):
    """Verify timestamps are aligned to timeframe boundaries."""
    print("\n[6] TIMESTAMP ALIGNMENT")
    print("-" * 40)
    
    parquet_files = []
    for venue_dir in get_venue_dirs():
        parquet_files.extend(list(venue_dir.rglob("*.parquet"))[:sample_files])
    
    misaligned = 0
    total = 0
    
    for pf in parquet_files[:sample_files]:
        try:
            df = pd.read_parquet(pf)
            
            for tf, duration_ms in TIMEFRAME_MS.items():
                tf_rows = df[df["timeframe"] == tf]
                if len(tf_rows) == 0:
                    continue
                
                total += len(tf_rows)
                bad = (tf_rows["open_time_ms"] % duration_ms != 0).sum()
                if bad > 0:
                    misaligned += bad
                    result.fail(f"{pf.name}: {bad} rows misaligned for {tf}")
                    
        except Exception as e:
            result.warn(f"{pf.name}: Cannot check - {e}")
    
    if misaligned == 0 and total > 0:
        result.ok(f"All timestamps aligned ({total} rows checked)")


def audit_gaps(result: AuditResult, max_gap_candles: int = 5):
    """Detect gaps in candle sequences."""
    print("\n[7] GAP DETECTION")
    print("-" * 40)
    
    gaps_found = defaultdict(list)
    
    for venue_dir in get_venue_dirs():
        venue = venue_dir.name
        
        for symbol_dir in venue_dir.iterdir():
            if not symbol_dir.is_dir():
                continue
            
            symbol = symbol_dir.name
            
            for tf_dir in symbol_dir.iterdir():
                if not tf_dir.is_dir():
                    continue
                
                tf = tf_dir.name
                duration_ms = TIMEFRAME_MS.get(tf)
                if not duration_ms:
                    continue
                
                all_times = []
                for pf in tf_dir.glob("*.parquet"):
                    try:
                        df = pd.read_parquet(pf, columns=["open_time_ms"])
                        all_times.extend(df["open_time_ms"].tolist())
                    except:
                        pass
                
                if len(all_times) < 2:
                    continue
                
                all_times = sorted(set(all_times))
                
                for i in range(1, len(all_times)):
                    expected_gap = duration_ms
                    actual_gap = all_times[i] - all_times[i-1]
                    
                    if actual_gap > expected_gap * max_gap_candles:
                        gap_candles = actual_gap // duration_ms
                        gaps_found[f"{venue}/{symbol}/{tf}"].append(gap_candles)
    
    if not gaps_found:
        result.ok("No significant gaps detected")
    else:
        total_gaps = sum(len(g) for g in gaps_found.values())
        result.warn(f"Found {total_gaps} gaps across {len(gaps_found)} streams")
        
        for stream, gaps in sorted(gaps_found.items(), key=lambda x: -max(x[1]))[:5]:
            max_gap = max(gaps)
            result.warn(f"  {stream}: max gap = {max_gap} candles")


def audit_cross_venue(result: AuditResult):
    """Check price consistency across venues for same asset."""
    print("\n[8] CROSS-VENUE CONSISTENCY")
    print("-" * 40)
    
    asset_map = {
        ("coinbase", "BTC-USD"): "BTC",
        ("kraken", "BTC/USD"): "BTC",
        ("okx", "BTC-USDT"): "BTC",
        ("coinbase", "ETH-USD"): "ETH",
        ("kraken", "ETH/USD"): "ETH",
        ("okx", "ETH-USDT"): "ETH",
    }
    
    latest_prices = defaultdict(dict)
    
    for (venue, symbol), asset in asset_map.items():
        venue_dir = DATA_DIR / venue / symbol / "1m"
        if not venue_dir.exists():
            continue
        
        parquet_files = sorted(venue_dir.glob("*.parquet"))
        if not parquet_files:
            continue
        
        try:
            df = pd.read_parquet(parquet_files[-1])
            if len(df) > 0:
                latest = df.sort_values("open_time_ms").iloc[-1]
                latest_prices[asset][venue] = float(latest["close"])
        except:
            pass
    
    for asset, venue_prices in latest_prices.items():
        if len(venue_prices) < 2:
            continue
        
        prices = list(venue_prices.values())
        avg_price = sum(prices) / len(prices)
        max_dev = max(abs(p - avg_price) / avg_price * 100 for p in prices)
        
        if max_dev > 2.0:  # Allow 2% deviation (USDT vs USD)
            result.warn(f"{asset}: {max_dev:.2f}% deviation across venues")
            for v, p in venue_prices.items():
                print(f"      {v}: ${p:,.2f}")
        else:
            result.ok(f"{asset}: Prices consistent ({max_dev:.2f}% max dev)")


def main():
    parser = argparse.ArgumentParser(description="Audit candle data integrity")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()
    
    print("=" * 60)
    print("  CANDLE DATA INTEGRITY AUDIT")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)
    
    if not DATA_DIR.exists():
        print(f"\n❌ Data directory not found: {DATA_DIR}")
        sys.exit(1)
    
    venue_dirs = get_venue_dirs()
    if not venue_dirs:
        print(f"\n❌ No venue directories found in {DATA_DIR}")
        print(f"   Looking for: {KNOWN_VENUES}")
        sys.exit(1)
    
    print(f"\n  Venues found: {[d.name for d in venue_dirs]}")
    
    result = AuditResult()
    
    audit_schema(result, args.verbose)
    audit_freshness(result)
    audit_completeness(result)
    audit_duplicates(result)
    audit_ohlc_sanity(result)
    audit_timestamp_alignment(result)
    audit_gaps(result)
    audit_cross_venue(result)
    
    passed = result.summary()
    
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
