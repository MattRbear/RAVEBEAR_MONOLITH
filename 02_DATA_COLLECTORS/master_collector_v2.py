import time
import signal
import sys
import json
from datetime import datetime, timezone
from pathlib import Path

from config import settings
from utils import get_logger, AlarmManager, send_alert, send_info, safe_write_json, SafeFileWriter
from okx_collector_v2 import HardenedOKXCollector
from object_factory_v2 import HardenedObjectFactory
from derivatives_collector_v2 import HardenedDerivativesCollector
from data_validator_v2 import EnhancedValidator

logger = get_logger("master")

class MasterCollector:
    def __init__(self):
        self.alarms = AlarmManager()
        self.running = True
        self.health_path = settings.DATA_VAULT_DIR / "health.json"
        self.file_reader = SafeFileWriter(settings.CANDLES_DIR)
        
        # Components
        # Note: We do NOT run candle_collector here; it runs in a separate process via supervisor.
        self.object_factory = HardenedObjectFactory()
        self.deriv_collector = HardenedDerivativesCollector()
        self.validator = EnhancedValidator()
        self.session_manager = None # TODO: Add session manager if needed, or simple file rotation
        
        # Signal Handlers
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame):
        logger.info("Shutdown signal received")
        self.running = False

    def _update_health(self):
        """Write heartbeat to disk."""
        health = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "RUNNING",
            "last_cycle": time.time()
        }
        safe_write_json(self.health_path, health)

    def run(self):
        logger.info("Master Collector Starting...")
        send_info("Master Collector Started")
        
        while self.running:
            start_time = time.time()
            
            try:
                # 1. Load latest candles from disk (updated by okx_collector_v2)
                df = self.file_reader.read_parquet(settings.CANDLE_PARQUET_PATH)
                
                if df is not None and not df.empty:
                    # 2. Detect Objects
                    objects = self.object_factory.detect_all(df)
                    
                    # 3. Fetch Derivatives
                    # Note: verify deriv_collector returns data or None
                    deriv_data = self.deriv_collector.fetch_oi() # Adjust method name if needed
                    
                    # 4. Create Snapshot
                    snapshot = {
                        "timestamp": int(time.time() * 1000),
                        "data_timestamp": int(df.iloc[-1]['timestamp']),
                        "datetime": datetime.now(timezone.utc).isoformat(),
                        "btc_price": float(df.iloc[-1]['close']),
                        "objects": objects,
                        "derivatives": deriv_data
                    }
                    
                    # 5. Validate
                    is_valid, errors, should_shutdown = self.validator.validate_snapshot(snapshot)
                    
                    if should_shutdown:
                        self.alarms.send("CRITICAL", "Shutdown triggered by validation failure")
                        self.running = False
                        break

                    if not is_valid:
                        logger.warning(f"Snapshot validation errors: {errors}")
                        # We might still save it but mark it invalid, or skip. 
                        # For now, let's log and proceed (saving happens below).

                    # 6. Save Snapshot
                    # Determine session folder (simple daily rotation)
                    session_id = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    session_dir = settings.SESSIONS_DIR / session_id / "snapshots"
                    session_dir.mkdir(parents=True, exist_ok=True)
                    
                    snap_id = int(time.time())
                    snap_path = session_dir / f"snap_{snap_id}.json"
                    
                    safe_write_json(snap_path, snapshot)
                    logger.info(f"Saved snapshot {snap_id}")

                else:
                    logger.warning("No candle data available yet.")

                self._update_health()
                
            except Exception as e:
                logger.error(f"Master cycle crash: {e}")
                self.alarms.send("ERROR", f"Master cycle crash: {e}")
            
            # Sleep remainder of cycle
            elapsed = time.time() - start_time
            if elapsed > settings.SNAPSHOT_INTERVAL:
                self.alarms.send("WARNING", f"Cycle took too long: {elapsed:.1f}s")
            else:
                sleep_time = max(1, settings.SNAPSHOT_INTERVAL - elapsed)
                logger.debug(f"Sleeping {sleep_time:.1f}s")
                time.sleep(sleep_time)
        
        logger.info("Master Collector Stopped")

if __name__ == "__main__":
    MasterCollector().run()