"""
Etherscan API Client
Free tier: 5 calls/sec, 100k/day
Tracks ETH whale wallets and exchange flows.
"""

import time
import requests
from datetime import datetime, timezone
from typing import Dict, List, Any

API_KEY = "FVYR1HPEM3A9HNNKQS46WSQ5QZBE44I1V"
BASE_URL = "https://api.etherscan.io/api"

# Known exchange hot wallets (ETH)
EXCHANGE_WALLETS = {
    '0x28c6c06298d514db089934071355e5743bf21d60': 'binance',
    '0x21a31ee1afc51d94c2efccaa2092ad1028285549': 'binance',
    '0xdfd5293d8e347dfe59e90efd55b2956a1343963d': 'binance',
    '0x56eddb7aa87536c09ccc2793473599fd21a8b17f': 'binance',
    '0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43': 'coinbase',
    '0x71660c4005ba85c37ccec55d0c4493e66fe775d3': 'coinbase',
    '0x503828976d22510aad0201ac7ec88293211d23da': 'coinbase',
    '0x77134cbc06cb00b66f4c7e623d5fdbf6777635ec': 'kraken',
    '0x2910543af39aba0cd09dbb2d50200b3e800a63d2': 'kraken',
    '0x1151314c646ce4e0efd76d1af4760ae66a9fe30f': 'bitfinex',
    '0x876eabf441b2ee5b5b0554fd502a8e0600950cfa': 'bitfinex',
}


class RateLimiter:
    def __init__(self, calls_per_second: float = 4.5):
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0
    
    def wait(self):
        elapsed = time.time() - self.last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_call = time.time()


class EtherscanClient:
    def __init__(self, api_key: str = API_KEY):
        self.api_key = api_key
        self.limiter = RateLimiter()
        self.session = requests.Session()
    
    def _get(self, params: Dict) -> Any:
        self.limiter.wait()
        params['apikey'] = self.api_key
        
        try:
            resp = self.session.get(BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            if data.get('status') == '1' or data.get('message') == 'OK':
                return data.get('result')
            return None
        except Exception as e:
            print(f"Etherscan error: {e}")
            return None
    
    def get_eth_balance(self, address: str) -> float:
        """Get ETH balance in ETH."""
        result = self._get({
            'module': 'account',
            'action': 'balance',
            'address': address,
            'tag': 'latest'
        })
        if result:
            return int(result) / 1e18
        return 0
    
    def get_eth_price(self) -> float:
        """Get current ETH price in USD."""
        result = self._get({
            'module': 'stats',
            'action': 'ethprice'
        })
        if result:
            return float(result.get('ethusd', 0))
        return 0
    
    def get_gas_price(self) -> Dict:
        """Get current gas prices."""
        result = self._get({
            'module': 'gastracker',
            'action': 'gasoracle'
        })
        if result:
            return {
                'safe': int(result.get('SafeGasPrice', 0)),
                'propose': int(result.get('ProposeGasPrice', 0)),
                'fast': int(result.get('FastGasPrice', 0)),
            }
        return {}
    
    def get_recent_transactions(self, address: str, limit: int = 50) -> List[Dict]:
        """Get recent transactions for an address."""
        result = self._get({
            'module': 'account',
            'action': 'txlist',
            'address': address,
            'startblock': 0,
            'endblock': 99999999,
            'page': 1,
            'offset': limit,
            'sort': 'desc'
        })
        return result if isinstance(result, list) else []
    
    def get_exchange_balances(self) -> Dict[str, float]:
        """Get ETH balances of known exchange wallets."""
        balances = {}
        for address, name in EXCHANGE_WALLETS.items():
            balance = self.get_eth_balance(address)
            if name in balances:
                balances[name] += balance
            else:
                balances[name] = balance
        return balances


def test_client():
    client = EtherscanClient()
    
    print("Testing Etherscan API...")
    
    # ETH price
    price = client.get_eth_price()
    print(f"ETH Price: ${price:,.2f}")
    
    # Gas prices
    gas = client.get_gas_price()
    print(f"Gas: Safe={gas.get('safe')} | Propose={gas.get('propose')} | Fast={gas.get('fast')}")
    
    # Sample exchange balance
    binance_addr = '0x28c6c06298d514db089934071355e5743bf21d60'
    balance = client.get_eth_balance(binance_addr)
    print(f"Binance Hot Wallet: {balance:,.2f} ETH (${balance * price:,.0f})")
    
    print("Etherscan API test complete!")


if __name__ == '__main__':
    test_client()
