"""
Whale Alert API Client
Premium tier - tracks whale transactions across chains.
"""

import time
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path

# API Config
API_KEY = "nFrlRURj0nC5pz6d33a1s7QIRFpoGk2W"
BASE_URL = "https://api.whale-alert.io/v1"

# Rate limit: Premium is generous but we'll be safe
# Free: 10/min, Premium: Higher but undocumented, using 30/min to be safe
CALLS_PER_MINUTE = 30

# Known exchange wallets (Whale Alert labels these automatically)
EXCHANGE_LABELS = [
    'binance', 'coinbase', 'kraken', 'bitfinex', 'huobi', 
    'okex', 'kucoin', 'bybit', 'ftx', 'gemini', 'bitstamp',
    'bittrex', 'gate.io', 'crypto.com', 'deribit'
]


class RateLimiter:
    def __init__(self, calls_per_minute: int = CALLS_PER_MINUTE):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute
        self.last_call = 0
        self.call_count = 0
        self.window_start = time.time()
    
    def wait(self):
        now = time.time()
        
        if now - self.window_start >= 60:
            self.call_count = 0
            self.window_start = now
        
        if self.call_count >= self.calls_per_minute:
            sleep_time = 60 - (now - self.window_start) + 0.5
            if sleep_time > 0:
                print(f"Rate limit: waiting {sleep_time:.1f}s...")
                time.sleep(sleep_time)
                self.call_count = 0
                self.window_start = time.time()
        
        elapsed = now - self.last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        
        self.last_call = time.time()
        self.call_count += 1


class WhaleAlertClient:
    """Whale Alert API client with rate limiting."""
    
    def __init__(self, api_key: str = API_KEY):
        self.api_key = api_key
        self.limiter = RateLimiter()
        self.session = requests.Session()
    
    def _get(self, endpoint: str, params: Dict = None) -> Any:
        """Make rate-limited GET request."""
        self.limiter.wait()
        
        params = params or {}
        params['api_key'] = self.api_key
        
        url = f"{BASE_URL}{endpoint}"
        
        try:
            resp = self.session.get(url, params=params, timeout=30)
            
            if resp.status_code == 429:
                retry_after = int(resp.headers.get('Retry-After', 60))
                print(f"Rate limited. Waiting {retry_after}s...")
                time.sleep(retry_after + 1)
                return self._get(endpoint, params)
            
            resp.raise_for_status()
            return resp.json()
        
        except Exception as e:
            print(f"API error: {e}")
            return None
    
    def get_status(self) -> Dict:
        """Get API status and blockchain status."""
        return self._get('/status')
    
    def get_transactions(
        self,
        start: int = None,
        end: int = None,
        currency: str = None,
        min_value: int = 500000,
        limit: int = 100
    ) -> List[Dict]:
        """
        Get whale transactions.
        
        Args:
            start: Unix timestamp (default: 1 hour ago)
            end: Unix timestamp (default: now)
            currency: 'btc', 'eth', 'usdt', etc. (None = all)
            min_value: Minimum USD value (default: $500k)
            limit: Max results (max 100)
        
        Returns:
            List of transaction dicts
        """
        if start is None:
            start = int((datetime.now(timezone.utc) - timedelta(hours=1)).timestamp())
        if end is None:
            end = int(datetime.now(timezone.utc).timestamp())
        
        params = {
            'start': start,
            'end': end,
            'min_value': min_value,
            'limit': limit,
        }
        
        if currency:
            params['currency'] = currency.lower()
        
        data = self._get('/transactions', params)
        
        if data and data.get('result') == 'success':
            return data.get('transactions', [])
        
        return []
    
    def get_btc_transactions(self, hours: int = 1, min_value: int = 1000000) -> List[Dict]:
        """Get BTC whale transactions."""
        start = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp())
        return self.get_transactions(start=start, currency='btc', min_value=min_value)
    
    def get_eth_transactions(self, hours: int = 1, min_value: int = 1000000) -> List[Dict]:
        """Get ETH whale transactions."""
        start = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp())
        return self.get_transactions(start=start, currency='eth', min_value=min_value)
    
    def get_usdt_transactions(self, hours: int = 1, min_value: int = 1000000) -> List[Dict]:
        """Get USDT whale transactions."""
        start = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp())
        return self.get_transactions(start=start, currency='usdt', min_value=min_value)
    
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
    
    def analyze_flow(self, transactions: List[Dict]) -> Dict:
        """
        Analyze transaction flow.
        
        Returns dict with:
            - total_volume
            - exchange_inflow (bearish)
            - exchange_outflow (bullish)
            - net_exchange_flow (negative = bullish)
            - whale_transfers
            - mints
            - burns
        """
        analysis = {
            'total_volume': 0,
            'exchange_inflow': 0,
            'exchange_outflow': 0,
            'whale_transfers': 0,
            'mints': 0,
            'burns': 0,
            'tx_count': len(transactions),
            'inflow_count': 0,
            'outflow_count': 0,
        }
        
        for tx in transactions:
            amount_usd = tx.get('amount_usd', 0) or 0
            analysis['total_volume'] += amount_usd
            
            tx_class = self.classify_transaction(tx)
            
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


def test_client():
    """Test the API connection."""
    client = WhaleAlertClient()
    
    print("Testing Whale Alert API...")
    
    # Test status
    status = client.get_status()
    if status:
        print(f"API Status: {status.get('result', 'unknown')}")
        blockchains = status.get('blockchains', [])
        print(f"Active blockchains: {len(blockchains)}")
    
    # Test BTC transactions
    print("\nFetching BTC transactions (last hour, >$1M)...")
    btc_txs = client.get_btc_transactions(hours=1, min_value=1000000)
    print(f"Found {len(btc_txs)} BTC whale transactions")
    
    if btc_txs:
        analysis = client.analyze_flow(btc_txs)
        print(f"\nBTC Flow Analysis:")
        print(f"  Total Volume: ${analysis['total_volume']/1e6:.2f}M")
        print(f"  Exchange Inflow: ${analysis['exchange_inflow']/1e6:.2f}M ({analysis['inflow_count']} txs)")
        print(f"  Exchange Outflow: ${analysis['exchange_outflow']/1e6:.2f}M ({analysis['outflow_count']} txs)")
        print(f"  Net Flow: ${analysis['net_exchange_flow']/1e6:+.2f}M")
        
        if analysis['net_exchange_flow'] > 0:
            print("  -> BEARISH (more flowing TO exchanges)")
        elif analysis['net_exchange_flow'] < 0:
            print("  -> BULLISH (more flowing FROM exchanges)")
    
    print("\nAPI test complete!")


if __name__ == '__main__':
    test_client()
