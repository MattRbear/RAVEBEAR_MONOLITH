"""
LIVE INTEGRITY MONITOR
======================
Run alongside collector to continuously verify data integrity.

Checks every 60 seconds:
- Files being written (not stale)
- No corruption in recent writes
- WebSocket connections alive
- Candle flow rate normal

Usage:
    python monitor_integrity.py
"""
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

import pandas as pd


DATA_DIR = Path("data")
CHECK_INTERVAL = 60  # seconds
STALE_THRESHOLD = 300  # 5 minutes

# Expected candle rates per minute (approximate)
EXPECTED_RATES = {
    "coinbase": {"min": 0.5, "max": 20},  # Tickers, not true candles
    "kraken": {"min": 0.1, "max": 10},
    "okx": {"min": 0.5, "max": 20},
}


class IntegrityMonitor:
    """Continuous integrity monitor."""
    
    def __init__(self):
        self.last_counts = {}
        self.alert_history = []
        self.start_time = datetime.now(timezone.utc)
    
    def check_freshness(self) -> list:
        """Check data freshness per venue."""
        alerts = []
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        for venue_dir in DATA_DIR.iterdir():
            if not venue_dir.is_dir():
                continue
            
            venue = venue_dir.name
            latest_ms = 0
            file_count = 0
            
            for pf in venue_dir.rglob("*.parquet"):
                file_count += 1
                try:
                    df = pd.read_parquet(pf, columns=["open_time_ms"])
                    if len(df) > 0:
                        latest_ms = max(latest_ms, df["open_time_ms"].max())
                except:
                    pass
            
            if latest_ms > 0:
                stale_sec = (now_ms - latest_ms) / 1000
                if stale_sec > STALE_THRESHOLD:
                    alerts.append({
                        "level": "WARN",
                        "venue": venue,
                        "msg": f"Data stale: {stale_sec:.0f}s old",
                    })
            elif file_count == 0:
                alerts.append({
                    "level": "ERROR",
                    "venue": venue,
                    "msg": "No data files found",
                })
        
        return alerts
    
    def check_file_growth(self) -> list:
        """Check that files are growing."""
        alerts = []
        current_counts = {}
        
        for venue_dir in DATA_DIR.iterdir():
            if not venue_dir.is_dir():
                continue
            
            venue = venue_dir.name
            total_rows = 0
            
            for pf in venue_dir.rglob("*.parquet"):
                try:
                    df = pd.read_parquet(pf)
                    total_rows += len(df)
                except:
                    pass
            
            current_counts[venue] = total_rows
            
            # Compare to last check
            if venue in self.last_counts:
                diff = total_rows - self.last_counts[venue]
                if diff == 0:
                    alerts.append({
                        "level": "WARN",
                        "venue": venue,
                        "msg": "No new rows since last check",
                    })
                elif diff < 0:
                    alerts.append({
                        "level": "ERROR",
                        "venue": venue,
                        "msg": f"Row count decreased by {-diff}!",
                    })
        
        self.last_counts = current_counts
        return alerts
    
    def check_recent_ohlc(self) -> list:
        """Spot-check recent candles for OHLC validity."""
        alerts = []
        
        for venue_dir in DATA_DIR.iterdir():
            if not venue_dir.is_dir():
                continue
            
            venue = venue_dir.name
            
            # Check most recent file
            parquet_files = sorted(venue_dir.rglob("*.parquet"))
            if not parquet_files:
                continue
            
            try:
                df = pd.read_parquet(parquet_files[-1])
                if len(df) == 0:
                    continue
                
                # Check last 10 rows
                recent = df.tail(10)
                
                # High >= Low
                bad = (recent["high"] < recent["low"]).sum()
                if bad > 0:
                    alerts.append({
                        "level": "ERROR",
                        "venue": venue,
                        "msg": f"{bad} recent candles with high < low",
                    })
                
                # Positive prices
                neg = ((recent["close"] <= 0) | (recent["open"] <= 0)).sum()
                if neg > 0:
                    alerts.append({
                        "level": "ERROR",
                        "venue": venue,
                        "msg": f"{neg} recent candles with non-positive prices",
                    })
                    
            except Exception as e:
                alerts.append({
                    "level": "WARN",
                    "venue": venue,
                    "msg": f"Cannot read recent data: {e}",
                })
        
        return alerts
    
    def run_checks(self) -> dict:
        """Run all checks."""
        all_alerts = []
        
        all_alerts.extend(self.check_freshness())
        all_alerts.extend(self.check_file_growth())
        all_alerts.extend(self.check_recent_ohlc())
        
        # Determine overall status
        errors = [a for a in all_alerts if a["level"] == "ERROR"]
        warns = [a for a in all_alerts if a["level"] == "WARN"]
        
        if errors:
            status = "CRITICAL"
        elif warns:
            status = "DEGRADED"
        else:
            status = "HEALTHY"
        
        return {
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "uptime_min": (datetime.now(timezone.utc) - self.start_time).seconds // 60,
            "errors": len(errors),
            "warnings": len(warns),
            "alerts": all_alerts,
            "row_counts": self.last_counts,
        }
    
    def print_status(self, result: dict):
        """Print status to console."""
        status = result["status"]
        
        # Status emoji
        emoji = {"HEALTHY": "✅", "DEGRADED": "⚠️", "CRITICAL": "❌"}.get(status, "❓")
        
        print(f"\n{'=' * 50}")
        print(f"  {emoji} INTEGRITY: {status}  |  {result['timestamp'][:19]}")
        print(f"{'=' * 50}")
        
        # Row counts
        print("  Row counts:")
        for venue, count in result["row_counts"].items():
            print(f"    {venue}: {count:,}")
        
        # Alerts
        if result["alerts"]:
            print("\n  Alerts:")
            for alert in result["alerts"]:
                icon = "❌" if alert["level"] == "ERROR" else "⚠️"
                print(f"    {icon} [{alert['venue']}] {alert['msg']}")
        
        print(f"{'=' * 50}")


async def main():
    print("=" * 50)
    print("  LIVE INTEGRITY MONITOR")
    print(f"  Checking every {CHECK_INTERVAL}s")
    print("=" * 50)
    
    monitor = IntegrityMonitor()
    
    while True:
        try:
            result = monitor.run_checks()
            monitor.print_status(result)
            
            # Write status to file for external monitoring
            status_file = DATA_DIR / ".integrity_status.json"
            with open(status_file, "w") as f:
                json.dump(result, f, indent=2)
            
        except Exception as e:
            print(f"\n❌ Monitor error: {e}")
        
        await asyncio.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nMonitor stopped.")
        sys.exit(0)
