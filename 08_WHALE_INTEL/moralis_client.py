"""
Moralis API Client  
Free tier: 40k compute units/month
Real-time wallet tracking and token transfers.
"""

import time
import requests
from datetime import datetime, timezone
from typing import Dict, List, Any

API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6ImQ5ODA1MmY2LWM0ZDUtNDFhYS1hMjMwLWY4ZjI0MTJiNTFlZSIsIm9yZ0lkIjoiNDY1NjU2IiwidXNlcklkIjoiNDc5MDU5IiwidHlwZUlkIjoiNzk1MjMzMjgtZTZiNy00ODk2LTgyNTItNTY2ODc5ZTkzOWFiIiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3NTU0NjEwMDYsImV4cCI6NDkxMTIyMTAwNn0.4MNpcrN4YX8KZejyHnhs33Lfos4pV7AswEVQiu9HgFs"
BASE_URL = "https://deep-index.moralis.io/api/v2.2"


class RateLimiter:
    def __init__(self, calls_per_second: float = 2):
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0
    
    def wait(self):
        elapsed = time.time() - self.last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_call = time.time()


class MoralisClient:
    def __init__(self, api_key: str = API_KEY):
        self.api_key = api_key
        self.limiter = RateLimiter()
        self.session = requests.Session()
        self.session.headers['X-API-Key'] = api_key
        self.session.headers['Accept'] = 'application/json'
    
    def _get(self, endpoint: str, params: Dict = None) -> Any:
        self.limiter.wait()
        url = f"{BASE_URL}{endpoint}"
        
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Moralis error: {e}")
            return None
    
    def get_wallet_balance(self, address: str, chain: str = "eth") -> Dict:
        """Get native balance for a wallet."""
        return self._get(f"/{address}/balance", {'chain': chain})
    
    def get_wallet_tokens(self, address: str, chain: str = "eth") -> List[Dict]:
        """Get ERC20 token balances."""
        result = self._get(f"/{address}/erc20", {'chain': chain})
        return result if isinstance(result, list) else []
    
    def get_wallet_transactions(self, address: str, chain: str = "eth", limit: int = 50) -> List[Dict]:
        """Get recent transactions for a wallet."""
        result = self._get(f"/{address}", {'chain': chain, 'limit': limit})
        if result and 'result' in result:
            return result['result']
        return []
    
    def get_token_transfers(self, address: str, chain: str = "eth", limit: int = 50) -> List[Dict]:
        """Get ERC20 token transfers."""
        result = self._get(f"/{address}/erc20/transfers", {'chain': chain, 'limit': limit})
        if result and 'result' in result:
            return result['result']
        return []
    
    def get_block_stats(self, chain: str = "eth") -> Dict:
        """Get latest block info."""
        return self._get(f"/block/latest", {'chain': chain})


def test_client():
    client = MoralisClient()
    
    print("Testing Moralis API...")
    
    # Test with a known wallet
    test_addr = "0x28c6c06298d514db089934071355e5743bf21d60"  # Binance
    
    balance = client.get_wallet_balance(test_addr)
    if balance:
        eth_balance = int(balance.get('balance', 0)) / 1e18
        print(f"Wallet Balance: {eth_balance:,.2f} ETH")
    
    print("Moralis API test complete!")


if __name__ == '__main__':
    test_client()
