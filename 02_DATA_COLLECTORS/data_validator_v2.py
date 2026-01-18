
import sys
import time
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

from config import settings
from utils import get_logger, AlarmManager, send_alert, timestamps

logger = get_logger("validator")

class EnhancedValidator:
    def __init__(self):
        self.alarms = AlarmManager()
        self.error_window = []  # (timestamp, severity)
        self.last_valid_price = None
        self.consecutive_criticals = 0
        self.shutdown_flag_path = settings.DATA_VAULT_DIR / "SHUTDOWN_FLAG"

    def validate_candle(self, candle: dict) -> tuple[bool, list]:
        errors = []
        
        # 1. Existence Check
        if not candle:
            return False, [('ERROR', 'Empty candle data')]

        # 2. NaN/Inf Check
        for field in ['open', 'high', 'low', 'close', 'volume']:
            val = candle.get(field)
            if val is None:
                errors.append(('ERROR', f'Missing field: {field}'))
            elif isinstance(val, (int, float)) and (pd.isna(val) or np.isinf(val)):
                errors.append(('ERROR', f'Field {field} is NaN or Inf'))

        # 3. OHLC Logic
        try:
            o = float(candle.get('open', 0))
            h = float(candle.get('high', 0))
            l = float(candle.get('low', 0))
            c = float(candle.get('close', 0))
            
            if o > 0 and h > 0 and l > 0 and c > 0:
                if not (l <= o <= h and l <= c <= h):
                    errors.append(('ERROR', f'OHLC violation: {o}, {h}, {l}, {c}'))
                
                if c < 10000 or c > 500000:
                    errors.append(('ERROR', f'Price out of sane range: {c}'))

                if self.last_valid_price:
                    pct_change = abs(c - self.last_valid_price) / self.last_valid_price
                    if pct_change > 0.20: # 20% move
                        errors.append(('CRITICAL', f'Price deviation {pct_change:.1%}'))
            else:
                 errors.append(('ERROR', f'Non-positive price detected'))

        except (ValueError, TypeError):
             errors.append(('ERROR', 'Non-numeric price data'))

        # 6. Timestamp Logic
        ts = candle.get('timestamp')
        if ts:
            try:
                ts_ms = timestamps.normalize_ts(ts)
                now_ms = timestamps.get_current_ts()
                diff_sec = (now_ms - ts_ms) / 1000
                
                if diff_sec < -300: # Allow some clock drift, but 5 mins in future is bad
                    errors.append(('ERROR', f'Future timestamp: {-diff_sec:.0f}s ahead'))
            except ValueError as e:
                errors.append(('ERROR', str(e)))
        else:
            errors.append(('ERROR', 'Missing timestamp'))

        is_valid = (len(errors) == 0) or (all(e[0] == 'WARNING' for e in errors))
        
        if is_valid and 'close' in candle:
            try:
                self.last_valid_price = float(candle['close'])
                # Reset criticals on valid data
                self.consecutive_criticals = 0 
            except: pass

        self._check_escalation(errors)
        return is_valid, errors

    def _check_escalation(self, errors):
        now = time.time()
        has_critical = False
        
        for severity, msg in errors:
            self.error_window.append((now, severity))
            self.alarms.send(severity, msg)
            if severity == 'CRITICAL':
                has_critical = True

        # Only increment criticals if THIS batch had one
        if has_critical:
            self.consecutive_criticals += 1
        
        # Prune old
        self.error_window = [x for x in self.error_window if now - x[0] < 300]
        
        # Shutdown Checks
        errors_count = len([x for x in self.error_window if x[1] == 'ERROR'])
        
        if self.consecutive_criticals >= 3:
            self._shutdown("3 consecutive CRITICAL errors")
            
        if errors_count >= 10:
            self._shutdown("10 ERRORs in 5 minutes")

    def validate_snapshot(self, snapshot: dict) -> tuple[bool, list, bool]:
        """
        Validate a full market snapshot.
        Returns: (is_valid, errors, should_shutdown)
        """
        errors = []
        should_shutdown = False
        
        if not snapshot:
            return False, [('ERROR', 'Empty snapshot')], False

        # 1. Price Check
        price = snapshot.get('btc_price')
        if not price or price <= 0:
            errors.append(('ERROR', f'Invalid snapshot price: {price}'))
        
        # 2. Objects Summary
        objects = snapshot.get('objects')
        if not objects:
            errors.append(('WARNING', 'Missing objects summary'))
        
        # 3. Derivatives
        deriv = snapshot.get('derivatives')
        if not deriv:
            errors.append(('WARNING', 'Missing derivatives data'))
        
        # 4. Critical Logic (e.g. Price Crash)
        if self.last_valid_price and price:
            diff = abs(price - self.last_valid_price) / self.last_valid_price
            if diff > 0.30: # 30% instant move is suspicious or crash
                errors.append(('CRITICAL', f'Snapshot price deviation {diff:.1%}'))
                should_shutdown = True # Immediate shutdown risk

        # 5. Timestamp
        ts = snapshot.get('timestamp')
        if not ts:
            errors.append(('ERROR', 'Snapshot missing timestamp'))
        
        # 6. Data Staleness (check age of data_timestamp vs system time)
        data_ts = snapshot.get('data_timestamp')
        if data_ts:
            try:
                now_ms = timestamps.get_current_ts()
                diff_sec = (now_ms - data_ts) / 1000
                if diff_sec > 600: # 10 minutes
                    errors.append(('WARNING', f'Stale data: {diff_sec:.0f}s old'))
            except Exception:
                pass

        is_valid = (len(errors) == 0) or (all(e[0] == 'WARNING' for e in errors))
        
        # Pass errors to escalation handler
        self._check_escalation(errors)
        
        # Check if escalation triggered a shutdown
        if self.shutdown_flag_path.exists():
            should_shutdown = True
            
        return is_valid, errors, should_shutdown

    def _shutdown(self, reason):
        logger.critical(f"SHUTDOWN TRIGGERED: {reason}")
        self.alarms.send("CRITICAL", f"SYSTEM SHUTDOWN: {reason}")
        try:
            with open(self.shutdown_flag_path, 'w') as f:
                f.write(f"Shutdown triggered at {datetime.now()} reason: {reason}")
        except Exception as e:
            print(f"Failed to write shutdown flag: {e}")
