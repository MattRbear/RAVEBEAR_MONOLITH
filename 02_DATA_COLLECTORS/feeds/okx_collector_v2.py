import time
import requests
import pandas as pd
from datetime import datetime, timezone
import sys

from config import settings, secrets
from utils import get_logger, AlarmManager, SafeFileWriter, timestamps
from data_validator_v2 import EnhancedValidator

logger = get_logger("okx_collector")

class HardenedOKXCollector:
    def __init__(self):
        self.alarms = AlarmManager()
        self.validator = EnhancedValidator()
        self.file_writer = SafeFileWriter(settings.CANDLES_DIR)
        
        self.consecutive_failures = 0
        self.max_failures = 5
        self.running = True

    def fetch_candles(self, after=None, limit=100):
        """Fetch candles from OKX API."""
        params = {
            "instId": settings.OKX_SYMBOL,
            "bar": settings.OKX_BAR,
            "limit": limit
        }
        if after:
            params['after'] = after

        try:
            # We don't have a rate limiter for OKX in settings.py, assuming high limit.
            # But we should sleep a bit.
            time.sleep(settings.OKX_SLEEP)
            
            resp = requests.get(secrets.OKX_API_URL, params=params, timeout=10)
            
            if resp.status_code == 200:
                data = resp.json()
                if data['code'] == '0':
                    return data['data']
                else:
                    self.alarms.send("ERROR", f"OKX API Error: {data['msg']}")
            else:
                self.alarms.send("ERROR", f"OKX HTTP Error: {resp.status_code}")
                
        except Exception as e:
            self.alarms.send("ERROR", f"OKX Connection Error: {e}")
        
        return None

    def process_candles(self, raw_data):
        """Convert raw OKX data to DataFrame and validate."""
        if not raw_data:
            return None

        # OKX Format: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
        cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'volCcy', 'volCcyQuote', 'confirm']
        
        valid_candles = []
        for row in raw_data:
            try:
                candle = {
                    'timestamp': int(row[0]),
                    'open': float(row[1]),
                    'high': float(row[2]),
                    'low': float(row[3]),
                    'close': float(row[4]),
                    'volume': float(row[5]),
                    'volCcy': float(row[6])
                }
                
                # Validation Layer
                is_valid, errors = self.validator.validate_candle(candle)
                if is_valid:
                    valid_candles.append(candle)
                else:
                    logger.warning(f"Invalid candle dropped: {errors}")
            except Exception as e:
                logger.error(f"Candle parsing error: {e}")

        if not valid_candles:
            return None
            
        return pd.DataFrame(valid_candles)

    def run_cycle(self):
        """Run one collection cycle."""
        logger.info("Starting collection cycle")
        
        raw_candles = self.fetch_candles()
        
        if raw_candles:
            new_df = self.process_candles(raw_candles)
            
            if new_df is not None and not new_df.empty:
                # Load existing
                existing_df = self.file_writer.read_parquet(settings.CANDLE_PARQUET_PATH)
                
                # Merge
                if not existing_df.empty:
                    combined = pd.concat([existing_df, new_df])
                    combined = combined.drop_duplicates(subset='timestamp', keep='last')
                    combined = combined.sort_values('timestamp')
                else:
                    combined = new_df.sort_values('timestamp')
                
                # Save
                success = self.file_writer.write_parquet(combined, settings.CANDLE_PARQUET_PATH)
                
                if success:
                    self.consecutive_failures = 0
                    
                    # Update metadata
                    meta = {
                        "last_update": timestamps.ts_to_iso(timestamps.get_current_ts()),
                        "count": len(combined),
                        "newest_ts": int(combined['timestamp'].iloc[-1]),
                        "oldest_ts": int(combined['timestamp'].iloc[0])
                    }
                    self.file_writer.write_json(meta, settings.CANDLE_META_PATH)
                    return True
        
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.max_failures:
            self.alarms.send("CRITICAL", "SHUTDOWN: 5 consecutive OKX failures")
            # In real app, raise SystemExit or similar
            return False
            
        return False

    def run_continuous(self):
        """Run continuously."""
        while self.running:
            self.run_cycle()
            time.sleep(60) # 1 minute candles

if __name__ == "__main__":
    collector = HardenedOKXCollector()
    if len(sys.argv) > 1 and sys.argv[1] == "--continuous":
        collector.run_continuous()
    else:
        collector.run_cycle()