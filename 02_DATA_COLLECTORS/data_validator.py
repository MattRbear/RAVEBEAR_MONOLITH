"""
RaveBear Data Validator
Validates all collected data for quality and consistency.
Sends Discord alerts on errors.
"""

import time
import json
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

# Discord webhook for error alerts
ERROR_WEBHOOK = "https://discord.com/api/webhooks/1435559676916797442/p-CVNHGuGGnmieCxuSZvddT0eTsa3P6QjLt-gjyDiKFAet98JlJI7MajVbeDmC-4R34v"

# Validation thresholds
MAX_DATA_AGE_SECONDS = 300        # Data older than 5 min is stale
MIN_BTC_PRICE = 10000             # Sanity check
MAX_BTC_PRICE = 500000            # Sanity check
MAX_PRICE_CHANGE_PCT = 10         # Max 10% change between snapshots
MAX_OI_VALUE = 50_000_000_000     # $50B max OI
MAX_FUNDING_RATE = 0.01           # 1% max funding (already extreme)
MIN_LONG_PCT = 0                  # 0-100 range
MAX_LONG_PCT = 100

# Error log
ERROR_LOG_PATH = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\validation_errors.json")


class ValidationError:
    """Represents a validation error."""
    
    def __init__(self, source: str, field: str, message: str, value: Any = None, severity: str = "WARNING"):
        self.source = source
        self.field = field
        self.message = message
        self.value = value
        self.severity = severity  # WARNING, ERROR, CRITICAL
        self.timestamp = datetime.now(timezone.utc).isoformat()
    
    def to_dict(self):
        return {
            'source': self.source,
            'field': self.field,
            'message': self.message,
            'value': str(self.value)[:100] if self.value else None,
            'severity': self.severity,
            'timestamp': self.timestamp,
        }
    
    def __str__(self):
        return f"[{self.severity}] {self.source}.{self.field}: {self.message}"


class DataValidator:
    """Validates data from all sources."""
    
    def __init__(self, discord_webhook: str = ERROR_WEBHOOK):
        self.webhook = discord_webhook
        self.errors: List[ValidationError] = []
        self.last_values: Dict[str, Any] = {}
        self.error_counts: Dict[str, int] = {}
        self.last_alert_time: Dict[str, float] = {}
        
        # Load previous values if exists
        self.state_path = Path(r"C:\Users\M.R Bear\Documents\Data_Vault\validator_state.json")
        self.load_state()
    
    def load_state(self):
        """Load previous state for continuity checks."""
        if self.state_path.exists():
            try:
                with open(self.state_path) as f:
                    state = json.load(f)
                    self.last_values = state.get('last_values', {})
                    self.error_counts = state.get('error_counts', {})
            except:
                pass
    
    def save_state(self):
        """Save state for next run."""
        state = {
            'last_values': self.last_values,
            'error_counts': self.error_counts,
            'last_save': datetime.now(timezone.utc).isoformat(),
        }
        with open(self.state_path, 'w') as f:
            json.dump(state, f, indent=2)
    
    def add_error(self, error: ValidationError):
        """Add an error to the list."""
        self.errors.append(error)
        
        # Track error counts by source
        key = f"{error.source}.{error.field}"
        self.error_counts[key] = self.error_counts.get(key, 0) + 1
        
        print(f"  VALIDATION: {error}")
    
    def clear_errors(self):
        """Clear current error list."""
        self.errors = []
    
    def has_errors(self, severity: str = None) -> bool:
        """Check if there are errors of given severity."""
        if severity:
            return any(e.severity == severity for e in self.errors)
        return len(self.errors) > 0
    
    def get_errors(self, severity: str = None) -> List[ValidationError]:
        """Get errors, optionally filtered by severity."""
        if severity:
            return [e for e in self.errors if e.severity == severity]
        return self.errors
    
    # === CANDLE DATA VALIDATION ===
    
    def validate_candle(self, candle: Dict) -> bool:
        """Validate a single candle."""
        source = "candle"
        valid = True
        
        # Required fields
        required = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        for field in required:
            if field not in candle:
                self.add_error(ValidationError(source, field, f"Missing required field", severity="ERROR"))
                valid = False
        
        if not valid:
            return False
        
        # OHLC sanity
        o, h, l, c = candle['open'], candle['high'], candle['low'], candle['close']
        
        if not (l <= o <= h and l <= c <= h):
            self.add_error(ValidationError(source, "ohlc", f"Invalid OHLC: L={l} O={o} H={h} C={c}", severity="ERROR"))
            valid = False
        
        # Price range check
        if not (MIN_BTC_PRICE <= c <= MAX_BTC_PRICE):
            self.add_error(ValidationError(source, "close", f"Price out of range: {c}", value=c, severity="CRITICAL"))
            valid = False
        
        # Volume check
        if candle['volume'] < 0:
            self.add_error(ValidationError(source, "volume", f"Negative volume: {candle['volume']}", severity="ERROR"))
            valid = False
        
        # Timestamp check
        ts = candle['timestamp']
        now_ms = int(time.time() * 1000)
        age_seconds = (now_ms - ts) / 1000
        
        if age_seconds > MAX_DATA_AGE_SECONDS:
            self.add_error(ValidationError(source, "timestamp", f"Stale data: {age_seconds:.0f}s old", severity="WARNING"))
        
        if ts > now_ms + 60000:  # More than 1 min in future
            self.add_error(ValidationError(source, "timestamp", f"Future timestamp detected", value=ts, severity="ERROR"))
            valid = False
        
        # Price continuity check
        last_price = self.last_values.get('btc_price')
        if last_price:
            change_pct = abs((c - last_price) / last_price) * 100
            if change_pct > MAX_PRICE_CHANGE_PCT:
                self.add_error(ValidationError(source, "close", f"Large price jump: {change_pct:.2f}%", value=c, severity="WARNING"))
        
        self.last_values['btc_price'] = c
        
        return valid
    
    # === DERIVATIVES DATA VALIDATION ===
    
    def validate_derivatives(self, data: Dict) -> bool:
        """Validate derivatives data."""
        source = "derivatives"
        valid = True
        
        if data is None:
            self.add_error(ValidationError(source, "data", "Derivatives data is None", severity="ERROR"))
            return False
        
        # OI validation
        oi = data.get('oi_value', 0)
        if oi < 0:
            self.add_error(ValidationError(source, "oi_value", f"Negative OI: {oi}", severity="ERROR"))
            valid = False
        elif oi > MAX_OI_VALUE:
            self.add_error(ValidationError(source, "oi_value", f"OI too high: {oi}", severity="WARNING"))
        elif oi == 0:
            self.add_error(ValidationError(source, "oi_value", "OI is zero - possible API issue", severity="WARNING"))
        
        # Funding validation
        funding = data.get('funding_rate', 0)
        if abs(funding) > MAX_FUNDING_RATE:
            self.add_error(ValidationError(source, "funding_rate", f"Extreme funding: {funding*100:.4f}%", severity="WARNING"))
        
        # L/S validation
        long_pct = data.get('long_pct', 50)
        short_pct = data.get('short_pct', 50)
        
        if not (MIN_LONG_PCT <= long_pct <= MAX_LONG_PCT):
            self.add_error(ValidationError(source, "long_pct", f"Invalid long_pct: {long_pct}", severity="ERROR"))
            valid = False
        
        if abs(long_pct + short_pct - 100) > 1:  # Allow 1% tolerance
            self.add_error(ValidationError(source, "ls_ratio", f"L/S doesn't sum to 100: {long_pct}+{short_pct}", severity="WARNING"))
        
        # Timestamp check
        ts = data.get('timestamp', 0)
        if ts:
            now_ms = int(time.time() * 1000)
            age_seconds = (now_ms - ts) / 1000
            if age_seconds > MAX_DATA_AGE_SECONDS:
                self.add_error(ValidationError(source, "timestamp", f"Stale derivatives: {age_seconds:.0f}s", severity="WARNING"))
        
        return valid
    
    # === WHALE DATA VALIDATION ===
    
    def validate_whale_flow(self, data: Dict) -> bool:
        """Validate whale flow data."""
        source = "whale_flow"
        valid = True
        
        if data is None:
            self.add_error(ValidationError(source, "data", "Whale data is None", severity="ERROR"))
            return False
        
        # Flow values check
        inflow = data.get('btc_inflow', 0)
        outflow = data.get('btc_outflow', 0)
        net_flow = data.get('btc_net_flow', 0)
        
        if inflow < 0 or outflow < 0:
            self.add_error(ValidationError(source, "flow", f"Negative flow: in={inflow}, out={outflow}", severity="ERROR"))
            valid = False
        
        # Net flow should equal inflow - outflow
        expected_net = inflow - outflow
        if abs(net_flow - expected_net) > 1000:  # $1000 tolerance
            self.add_error(ValidationError(source, "net_flow", f"Net flow mismatch: {net_flow} vs expected {expected_net}", severity="WARNING"))
        
        # Volume sanity
        volume = data.get('btc_volume', 0)
        if volume < 0:
            self.add_error(ValidationError(source, "volume", f"Negative volume: {volume}", severity="ERROR"))
            valid = False
        
        # TX count
        tx_count = data.get('tx_count', 0)
        if tx_count < 0:
            self.add_error(ValidationError(source, "tx_count", f"Negative tx_count: {tx_count}", severity="ERROR"))
            valid = False
        
        return valid
    
    # === OBJECTS VALIDATION ===
    
    def validate_objects(self, data: Dict) -> bool:
        """Validate tradeable objects data."""
        source = "objects"
        valid = True
        
        if data is None:
            self.add_error(ValidationError(source, "data", "Objects data is None", severity="ERROR"))
            return False
        
        # BTC price check
        btc_price = data.get('btc_price', 0)
        if not (MIN_BTC_PRICE <= btc_price <= MAX_BTC_PRICE):
            self.add_error(ValidationError(source, "btc_price", f"Invalid price: {btc_price}", severity="ERROR"))
            valid = False
        
        # Check summary exists
        summary = data.get('summary', {})
        if not summary:
            self.add_error(ValidationError(source, "summary", "Missing summary", severity="WARNING"))
        
        # Check counts are non-negative
        for field in ['total_wicks', 'total_poors', 'total_boxes']:
            value = summary.get(field, 0)
            if value < 0:
                self.add_error(ValidationError(source, field, f"Negative count: {value}", severity="ERROR"))
                valid = False
        
        # Validate individual objects
        for obj_type in ['wicks_up', 'wicks_dn', 'poor_hi', 'poor_lo']:
            objects = data.get(obj_type, [])
            for obj in objects[:5]:  # Check first 5 of each type
                if 'price' not in obj:
                    self.add_error(ValidationError(source, obj_type, "Object missing price", severity="ERROR"))
                    valid = False
                elif not (MIN_BTC_PRICE <= obj['price'] <= MAX_BTC_PRICE):
                    self.add_error(ValidationError(source, obj_type, f"Object price out of range: {obj['price']}", severity="ERROR"))
                    valid = False
        
        return valid
    
    # === SNAPSHOT VALIDATION ===
    
    def validate_snapshot(self, snapshot: Dict) -> bool:
        """Validate a complete market snapshot."""
        valid = True
        
        # Validate each component
        if 'btc_price' in snapshot:
            if not (MIN_BTC_PRICE <= snapshot['btc_price'] <= MAX_BTC_PRICE):
                self.add_error(ValidationError("snapshot", "btc_price", f"Invalid price: {snapshot['btc_price']}", severity="ERROR"))
                valid = False
        
        if 'derivatives' in snapshot:
            valid = self.validate_derivatives(snapshot['derivatives']) and valid
        
        if 'whale_flow' in snapshot:
            valid = self.validate_whale_flow(snapshot['whale_flow']) and valid
        
        if 'objects' in snapshot:
            valid = self.validate_objects(snapshot['objects']) and valid
        
        # Check regime
        regime = snapshot.get('regime', '')
        if not regime:
            self.add_error(ValidationError("snapshot", "regime", "Missing regime classification", severity="WARNING"))
        
        return valid
    
    # === DISCORD ALERTS ===
    
    def send_discord_alert(self, title: str, errors: List[ValidationError], color: int = 0xff0000):
        """Send validation error alert to Discord."""
        # Rate limit: max 1 alert per error type per 10 minutes
        alert_key = title
        now = time.time()
        last_alert = self.last_alert_time.get(alert_key, 0)
        
        if now - last_alert < 600:  # 10 min cooldown
            return
        
        self.last_alert_time[alert_key] = now
        
        # Build error list
        error_text = "\n".join([f"• {e.source}.{e.field}: {e.message}" for e in errors[:10]])
        if len(errors) > 10:
            error_text += f"\n... and {len(errors) - 10} more"
        
        payload = {
            'embeds': [{
                'title': title,
                'description': error_text,
                'color': color,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'footer': {'text': f'Total errors: {len(errors)}'}
            }]
        }
        
        try:
            requests.post(self.webhook, json=payload, timeout=10)
            print(f"  [DISCORD] Sent validation alert: {title}")
        except Exception as e:
            print(f"  [DISCORD] Failed to send alert: {e}")
    
    def check_and_alert(self):
        """Check errors and send alerts if needed."""
        if not self.errors:
            return
        
        critical = self.get_errors("CRITICAL")
        errors = self.get_errors("ERROR")
        warnings = self.get_errors("WARNING")
        
        if critical:
            self.send_discord_alert("🚨 CRITICAL DATA ERROR", critical, color=0xff0000)
        
        if errors:
            self.send_discord_alert("❌ Data Validation Errors", errors, color=0xff6600)
        
        # Only alert on warnings if there are many
        if len(warnings) >= 5:
            self.send_discord_alert("⚠️ Data Validation Warnings", warnings, color=0xffaa00)
        
        # Log all errors
        self.log_errors()
        
        # Save state
        self.save_state()
    
    def log_errors(self):
        """Log errors to file."""
        if not self.errors:
            return
        
        log_data = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'errors': [e.to_dict() for e in self.errors],
        }
        
        # Append to log file
        existing = []
        if ERROR_LOG_PATH.exists():
            try:
                with open(ERROR_LOG_PATH) as f:
                    existing = json.load(f)
            except:
                pass
        
        existing.append(log_data)
        
        # Keep last 1000 error batches
        existing = existing[-1000:]
        
        with open(ERROR_LOG_PATH, 'w') as f:
            json.dump(existing, f, indent=2)


# === CONVENIENCE FUNCTIONS ===

_validator = None

def get_validator() -> DataValidator:
    """Get or create the global validator instance."""
    global _validator
    if _validator is None:
        _validator = DataValidator()
    return _validator


def validate_and_alert(data_type: str, data: Any) -> bool:
    """Validate data and send alerts if needed."""
    validator = get_validator()
    validator.clear_errors()
    
    valid = True
    
    if data_type == 'candle':
        valid = validator.validate_candle(data)
    elif data_type == 'derivatives':
        valid = validator.validate_derivatives(data)
    elif data_type == 'whale_flow':
        valid = validator.validate_whale_flow(data)
    elif data_type == 'objects':
        valid = validator.validate_objects(data)
    elif data_type == 'snapshot':
        valid = validator.validate_snapshot(data)
    
    if not valid:
        validator.check_and_alert()
    
    return valid


def test_validator():
    """Test the validator with sample data."""
    print("Testing Data Validator...\n")
    
    validator = DataValidator()
    
    # Test valid candle
    print("Testing valid candle...")
    valid_candle = {
        'timestamp': int(time.time() * 1000),
        'open': 90000,
        'high': 90100,
        'low': 89900,
        'close': 90050,
        'volume': 100.5,
    }
    result = validator.validate_candle(valid_candle)
    print(f"  Valid candle: {'PASS' if result else 'FAIL'}")
    
    # Test invalid candle
    print("\nTesting invalid candle (bad OHLC)...")
    validator.clear_errors()
    invalid_candle = {
        'timestamp': int(time.time() * 1000),
        'open': 90000,
        'high': 89000,  # High < Open = invalid
        'low': 89900,
        'close': 90050,
        'volume': 100.5,
    }
    result = validator.validate_candle(invalid_candle)
    print(f"  Invalid candle: {'PASS (detected)' if not result else 'FAIL (missed)'}")
    
    # Test derivatives
    print("\nTesting derivatives data...")
    validator.clear_errors()
    deriv_data = {
        'timestamp': int(time.time() * 1000),
        'oi_value': 8_000_000_000,
        'funding_rate': 0.0001,
        'long_pct': 68,
        'short_pct': 32,
    }
    result = validator.validate_derivatives(deriv_data)
    print(f"  Valid derivatives: {'PASS' if result else 'FAIL'}")
    
    # Test whale flow
    print("\nTesting whale flow data...")
    validator.clear_errors()
    whale_data = {
        'btc_net_flow': 5000000,
        'btc_inflow': 10000000,
        'btc_outflow': 5000000,
        'btc_volume': 500000000,
        'tx_count': 50,
    }
    result = validator.validate_whale_flow(whale_data)
    print(f"  Valid whale flow: {'PASS' if result else 'FAIL'}")
    
    print("\nValidator test complete!")


if __name__ == '__main__':
    test_validator()
