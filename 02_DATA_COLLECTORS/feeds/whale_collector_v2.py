"""
RaveBear Trading Pipeline - Hardened Whale Collector
=====================================================
Fetches whale transaction data from Whale Alert with full safety layers.

Integrates:
- Config system (Step 1)
- Structured logging (Step 2)
- Discord alarms (Step 2)
- Rate limiting (Step 6)
- Data validation

Data Collected:
- BTC whale transactions (>$1M)
- Exchange inflows/outflows
- Net flow analysis

Shutdown Conditions:
- API key invalid (401) → CRITICAL + shutdown
- 10 consecutive failures → shutdown

Last Updated: 2025-12-30
"""

import sys
import time
import math
import requests
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field

# Ensure imports work
sys.path.insert(0, str(Path(__file__).parent))

from config import settings, secrets
from utils import (
    get_logger,
    send_warning, send_error, send_critical,
    safe_write_json, safe_read_json,
    acquire_rate_limit, report_429,
    log_startup, log_shutdown,
)

logger = get_logger('whale_collector')

# ============================================================================
# CONSTANTS
# ============================================================================

BASE_URL = "https://api.whale-alert.io/v1"

# Known exchanges
EXCHANGE_LABELS = [
    'binance', 'coinbase', 'kraken', 'bitfinex', 'huobi',
    'okex', 'kucoin', 'bybit', 'ftx', 'gemini', 'bitstamp',
    'bittrex', 'gate.io', 'crypto.com', 'deribit'
]

# Validation
MIN_TX_VALUE = 100_000  # $100K minimum to be considered
MAX_TX_VALUE = 10_000_000_000  # $10B max (sanity check)

# State
STATE_PATH = settings.DATA_VAULT / "whale_collector_state.json"
OUTPUT_PATH = settings.DATA_VAULT / "Whale_Flow" / "whale_latest.json"
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# Failure thresholds
MAX_CONSECUTIVE_FAILURES = 10

# ============================================================================
# VALIDATION
# ============================================================================

def is_valid_number(value: Any) -> bool:
    """Check if value is a valid number."""
    if value is None:
        return False
    try:
        f = float(value)
        return not (math.isnan(f) or math.isinf(f))
    except (TypeError, ValueError):
        return False


def validate_transaction(tx: Dict) -> Tuple[bool, str]:
    """Validate a single transaction."""
    # Check required fields
    required = ['amount_usd', 'timestamp', 'symbol']
    for field in required:
        if field not in tx:
            return False, f"Missing field: {field}"
    
    # Validate amount
    amount = tx.get('amount_usd')
    if not is_valid_number(amount):
        return False, f"Invalid amount: {amount}"
    
    amount = float(amount)
    if amount < MIN_TX_VALUE:
        return False, f"Amount too small: ${amount:,.0f}"
    if amount > MAX_TX_VALUE:
        return False, f"Amount suspiciously large: ${amount:,.0f}"
    
    # Validate timestamp (should be recent, not future)
    ts = tx.get('timestamp')
    if not is_valid_number(ts):
        return False, f"Invalid timestamp: {ts}"
    
    ts = int(ts)
    now = int(datetime.now(timezone.utc).timestamp())
    
    if ts > now + 3600:  # More than 1 hour in future
        return False, f"Timestamp in future: {ts}"
    if ts < now - (7 * 24 * 3600):  # More than 7 days old
        return False, f"Timestamp too old: {ts}"
    
    return True, ""


def validate_flow_analysis(analysis: Dict) -> Tuple[bool, str]:
    """Validate flow analysis results."""
    required = ['total_volume', 'exchange_inflow', 'exchange_outflow', 'net_exchange_flow']
    
    for field in required:
        if field not in analysis:
            return False, f"Missing field: {field}"
        if not is_valid_number(analysis[field]):
            return False, f"Invalid {field}: {analysis[field]}"
    
    # Sanity checks
    total = analysis['total_volume']
    inflow = analysis['exchange_inflow']
    outflow = analysis['exchange_outflow']
    
    if total < 0 or inflow < 0 or outflow < 0:
        return False, "Negative values in flow analysis"
    
    if inflow + outflow > total * 2:  # Allow some slack for categorization
        return False, "Inflow + outflow > total (data inconsistency)"
    
    return True, ""


# ============================================================================
# STATE MANAGEMENT
# ============================================================================

@dataclass
class CollectorState:
    """Persistent state for whale collector."""
    last_successful_fetch: Optional[str] = None
    consecutive_failures: int = 0
    total_fetches: int = 0
    total_transactions: int = 0
    last_inflow: Optional[float] = None
    last_outflow: Optional[float] = None
    last_net_flow: Optional[float] = None
    
    def to_dict(self) -> dict:
        return {
            'last_successful_fetch': self.last_successful_fetch,
            'consecutive_failures': self.consecutive_failures,
            'total_fetches': self.total_fetches,
            'total_transactions': self.total_transactions,
            'last_inflow': self.last_inflow,
            'last_outflow': self.last_outflow,
            'last_net_flow': self.last_net_flow,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'CollectorState':
        return cls(
            last_successful_fetch=data.get('last_successful_fetch'),
            consecutive_failures=data.get('consecutive_failures', 0),
            total_fetches=data.get('total_fetches', 0),
            total_transactions=data.get('total_transactions', 0),
            last_inflow=data.get('last_inflow'),
            last_outflow=data.get('last_outflow'),
            last_net_flow=data.get('last_net_flow'),
        )


# ============================================================================
# HARDENED WHALE COLLECTOR
# ============================================================================

class HardenedWhaleCollector:
    """
    Hardened Whale Alert data collector.
    """
    
    def __init__(self):
        self.api_key = secrets.WHALE_ALERT_API_KEY
        self.session = requests.Session()
        
        self.state = self._load_state()
        self._shutdown_requested = False
        
        logger.info("HardenedWhaleCollector initialized")
    
    def _load_state(self) -> CollectorState:
        """Load state from file."""
        data = safe_read_json(STATE_PATH, default={})
        return CollectorState.from_dict(data)
    
    def _save_state(self):
        """Save state to file."""
        safe_write_json(STATE_PATH, self.state.to_dict())
    
    def _api_get(self, endpoint: str, params: Dict = None) -> Tuple[Optional[Any], Optional[str]]:
        """
        Make rate-limited API request.
        
        Returns: (data, error_message)
        """
        # Rate limit check
        if not acquire_rate_limit('whale_alert', timeout=60, block=True):
            return None, "Rate limit - could not acquire"
        
        params = params or {}
        params['api_key'] = self.api_key
        
        url = f"{BASE_URL}{endpoint}"
        
        try:
            resp = self.session.get(url, params=params, timeout=30)
            
            # Check for errors
            if resp.status_code == 401:
                logger.critical("Whale Alert API key invalid!")
                send_critical("Whale Alert API key invalid (401)", {'endpoint': endpoint})
                self._shutdown_requested = True
                return None, "API key invalid"
            
            if resp.status_code == 429:
                logger.error("Whale Alert rate limited (429)")
                report_429('whale_alert')
                return None, "Rate limited (429)"
            
            if resp.status_code != 200:
                logger.error(f"Whale Alert API error: {resp.status_code}")
                return None, f"HTTP {resp.status_code}"
            
            return resp.json(), None
            
        except requests.Timeout:
            logger.warning("Whale Alert API timeout")
            send_warning("Whale Alert API timeout", {'endpoint': endpoint})
            return None, "Timeout"
        except requests.RequestException as e:
            logger.error(f"Whale Alert request error: {e}")
            return None, str(e)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None, str(e)
    
    # =========================================================================
    # TRANSACTION CLASSIFICATION
    # =========================================================================
    
    def classify_transaction(self, tx: Dict) -> str:
        """
        Classify transaction type.
        
        Returns:
            'EXCHANGE_INFLOW' - To exchange (selling pressure)
            'EXCHANGE_OUTFLOW' - From exchange (accumulation)
            'EXCHANGE_INTERNAL' - Exchange to exchange
            'WHALE_TRANSFER' - Between unknown wallets
            'MINT' - New tokens minted
            'BURN' - Tokens burned
        """
        from_owner = (tx.get('from', {}).get('owner', '') or '').lower()
        to_owner = (tx.get('to', {}).get('owner', '') or '').lower()
        from_type = tx.get('from', {}).get('owner_type', '')
        to_type = tx.get('to', {}).get('owner_type', '')
        
        from_is_exchange = from_type == 'exchange' or any(ex in from_owner for ex in EXCHANGE_LABELS)
        to_is_exchange = to_type == 'exchange' or any(ex in to_owner for ex in EXCHANGE_LABELS)
        
        tx_type = tx.get('transaction_type', '')
        
        if tx_type == 'mint':
            return 'MINT'
        elif tx_type == 'burn':
            return 'BURN'
        elif from_is_exchange and to_is_exchange:
            return 'EXCHANGE_INTERNAL'
        elif to_is_exchange and not from_is_exchange:
            return 'EXCHANGE_INFLOW'
        elif from_is_exchange and not to_is_exchange:
            return 'EXCHANGE_OUTFLOW'
        else:
            return 'WHALE_TRANSFER'
    
    # =========================================================================
    # DATA FETCHING
    # =========================================================================
    
    def fetch_transactions(
        self,
        currency: str = 'btc',
        hours: int = 1,
        min_value: int = 1_000_000
    ) -> List[Dict]:
        """
        Fetch whale transactions.
        
        Returns list of validated transactions.
        """
        start = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp())
        end = int(datetime.now(timezone.utc).timestamp())
        
        params = {
            'start': start,
            'end': end,
            'currency': currency.lower(),
            'min_value': min_value,
            'limit': 100,
        }
        
        data, error = self._api_get('/transactions', params)
        
        if error:
            logger.warning(f"Transaction fetch failed: {error}")
            return []
        
        if not data or data.get('result') != 'success':
            return []
        
        raw_txs = data.get('transactions', [])
        
        # Validate each transaction
        valid_txs = []
        invalid_count = 0
        
        for tx in raw_txs:
            is_valid, err_msg = validate_transaction(tx)
            if is_valid:
                # Add classification
                tx['classification'] = self.classify_transaction(tx)
                valid_txs.append(tx)
            else:
                invalid_count += 1
                if invalid_count <= 3:  # Log first few
                    logger.debug(f"Invalid tx: {err_msg}")
        
        if invalid_count > 0:
            logger.info(f"Filtered {invalid_count} invalid transactions")
        
        return valid_txs
    
    def analyze_flow(self, transactions: List[Dict]) -> Dict:
        """
        Analyze transaction flow.
        """
        analysis = {
            'total_volume': 0.0,
            'exchange_inflow': 0.0,
            'exchange_outflow': 0.0,
            'whale_transfers': 0.0,
            'mints': 0.0,
            'burns': 0.0,
            'tx_count': len(transactions),
            'inflow_count': 0,
            'outflow_count': 0,
            'net_exchange_flow': 0.0,
        }
        
        for tx in transactions:
            amount_usd = float(tx.get('amount_usd', 0) or 0)
            analysis['total_volume'] += amount_usd
            
            tx_class = tx.get('classification', self.classify_transaction(tx))
            
            if tx_class == 'EXCHANGE_INFLOW':
                analysis['exchange_inflow'] += amount_usd
                analysis['inflow_count'] += 1
            elif tx_class == 'EXCHANGE_OUTFLOW':
                analysis['exchange_outflow'] += amount_usd
                analysis['outflow_count'] += 1
            elif tx_class == 'WHALE_TRANSFER':
                analysis['whale_transfers'] += amount_usd
            elif tx_class == 'MINT':
                analysis['mints'] += amount_usd
            elif tx_class == 'BURN':
                analysis['burns'] += amount_usd
        
        analysis['net_exchange_flow'] = analysis['exchange_inflow'] - analysis['exchange_outflow']
        
        return analysis
    
    # =========================================================================
    # MAIN COLLECTION
    # =========================================================================
    
    def fetch_all(self, hours: int = 1, min_value: int = 1_000_000) -> Dict:
        """
        Fetch all whale data and analyze.
        """
        self.state.total_fetches += 1
        
        result = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'hours': hours,
            'min_value': min_value,
            'transactions': [],
            'analysis': None,
            'valid': False,
            'errors': [],
        }
        
        # Fetch BTC transactions
        txs = self.fetch_transactions(currency='btc', hours=hours, min_value=min_value)
        
        if not txs:
            result['errors'].append('No transactions fetched')
            self.state.consecutive_failures += 1
            logger.warning(f"No whale transactions ({self.state.consecutive_failures}/{MAX_CONSECUTIVE_FAILURES})")
            self._save_state()
            return result
        
        result['transactions'] = txs
        self.state.total_transactions += len(txs)
        
        # Analyze flow
        analysis = self.analyze_flow(txs)
        
        # Validate analysis
        is_valid, err_msg = validate_flow_analysis(analysis)
        if not is_valid:
            result['errors'].append(f"Analysis validation failed: {err_msg}")
            logger.warning(f"Flow analysis validation failed: {err_msg}")
        else:
            result['analysis'] = analysis
            result['valid'] = True
            
            # Update state
            self.state.consecutive_failures = 0
            self.state.last_successful_fetch = result['timestamp']
            self.state.last_inflow = analysis['exchange_inflow']
            self.state.last_outflow = analysis['exchange_outflow']
            self.state.last_net_flow = analysis['net_exchange_flow']
            
            direction = "BEARISH (to exchanges)" if analysis['net_exchange_flow'] > 0 else "BULLISH (from exchanges)"
            logger.info(f"Whale flow: {len(txs)} txs, Net=${analysis['net_exchange_flow']/1e6:+.2f}M ({direction})")
        
        self._save_state()
        return result
    
    def save_result(self, result: Dict) -> bool:
        """Save result to file."""
        # Don't save full transaction list (too large)
        save_data = {
            'timestamp': result['timestamp'],
            'hours': result['hours'],
            'tx_count': len(result.get('transactions', [])),
            'analysis': result.get('analysis'),
            'valid': result['valid'],
            'errors': result.get('errors', []),
        }
        return safe_write_json(OUTPUT_PATH, save_data)
    
    def check_shutdown(self) -> bool:
        """Check if shutdown is needed."""
        if self._shutdown_requested:
            return True
        
        if self.state.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            logger.critical(f"SHUTDOWN: {MAX_CONSECUTIVE_FAILURES} consecutive failures")
            send_critical("Whale collector shutdown", {
                'consecutive_failures': self.state.consecutive_failures
            })
            return True
        
        return False
    
    def get_status(self) -> Dict:
        """Get collector status."""
        return {
            'last_successful': self.state.last_successful_fetch,
            'consecutive_failures': self.state.consecutive_failures,
            'total_fetches': self.state.total_fetches,
            'total_transactions': self.state.total_transactions,
            'last_inflow': self.state.last_inflow,
            'last_outflow': self.state.last_outflow,
            'last_net_flow': self.state.last_net_flow,
        }


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

_collector: Optional[HardenedWhaleCollector] = None


def get_collector() -> HardenedWhaleCollector:
    """Get or create collector instance."""
    global _collector
    if _collector is None:
        _collector = HardenedWhaleCollector()
    return _collector


def fetch_whale_flow(hours: int = 1) -> Dict:
    """Fetch whale flow data."""
    return get_collector().fetch_all(hours=hours)


# ============================================================================
# CLI
# ============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Hardened Whale Collector')
    parser.add_argument('--status', action='store_true', help='Show status')
    parser.add_argument('--continuous', '-c', action='store_true', help='Run continuously')
    parser.add_argument('--interval', '-i', type=int, default=300, help='Interval in seconds (default: 300)')
    parser.add_argument('--hours', type=int, default=1, help='Hours of history to fetch (default: 1)')
    
    args = parser.parse_args()
    
    collector = HardenedWhaleCollector()
    
    if args.status:
        status = collector.get_status()
        print("\n" + "=" * 50)
        print("WHALE COLLECTOR STATUS")
        print("=" * 50)
        for k, v in status.items():
            if v and ('inflow' in k.lower() or 'outflow' in k.lower() or 'net' in k.lower()):
                print(f"  {k}: ${v/1e6:.2f}M")
            else:
                print(f"  {k}: {v}")
        return
    
    if args.continuous:
        log_startup('whale_collector_v2')
        logger.info(f"Starting continuous collection, interval={args.interval}s")
        
        try:
            while not collector.check_shutdown():
                result = collector.fetch_all(hours=args.hours)
                if result['valid']:
                    collector.save_result(result)
                    analysis = result['analysis']
                    print(f"[{result['timestamp'][:19]}] {result.get('tx_count', 0)} txs, "
                          f"Net=${analysis['net_exchange_flow']/1e6:+.2f}M")
                else:
                    print(f"[{result['timestamp'][:19]}] FAILED: {result.get('errors', [])}")
                
                time.sleep(args.interval)
        except KeyboardInterrupt:
            logger.info("Stopped by user")
        finally:
            log_shutdown('whale_collector_v2')
    else:
        result = collector.fetch_all(hours=args.hours)
        
        print("\n" + "=" * 50)
        print("WHALE FLOW DATA")
        print("=" * 50)
        
        if result['valid']:
            analysis = result['analysis']
            print(f"  Transactions:   {analysis['tx_count']}")
            print(f"  Total Volume:   ${analysis['total_volume']/1e6:.2f}M")
            print(f"  Exchange In:    ${analysis['exchange_inflow']/1e6:.2f}M ({analysis['inflow_count']} txs)")
            print(f"  Exchange Out:   ${analysis['exchange_outflow']/1e6:.2f}M ({analysis['outflow_count']} txs)")
            print(f"  Net Flow:       ${analysis['net_exchange_flow']/1e6:+.2f}M")
            
            if analysis['net_exchange_flow'] > 0:
                print(f"\n  -> BEARISH (more flowing TO exchanges)")
            else:
                print(f"\n  -> BULLISH (more flowing FROM exchanges)")
        else:
            print(f"  Valid: False")
            print(f"  Errors: {result.get('errors', [])}")
        
        collector.save_result(result)


if __name__ == '__main__':
    main()
