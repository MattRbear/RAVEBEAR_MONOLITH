import asyncio
import aiohttp
import logging
from datetime import datetime
from typing import Dict, List, Optional
from collections import defaultdict

logger = logging.getLogger("feeds.whale_alert")

class WhaleAlertClient:
    """
    Client for Whale Alert API.
    Monitors large transactions for BTC, ETH, SOL.
    """
    
    BASE_URL = "https://api.whale-alert.io/v1/transactions"
    
    def __init__(self, api_key: str, min_value_usd: int = 1000000, check_interval: int = 60):
        self.api_key = api_key
        self.min_value_usd = min_value_usd
        self.check_interval = check_interval
        self.running = False
        self.latest_events: Dict[str, List[dict]] = defaultdict(list) # Symbol -> list of recent txs
        
    async def start(self):
        """Start monitoring loop."""
        self.running = True
        logger.info("[WHALE] Monitor started")
        while self.running:
            try:
                await self._check_alerts()
            except Exception as e:
                logger.error(f"[WHALE] Error: {e}")
            
            await asyncio.sleep(self.check_interval)

    async def stop(self):
        """Stop monitoring."""
        self.running = False
        logger.info("[WHALE] Monitor stopped")

    async def _check_alerts(self):
        if not self.api_key:
            return

        params = {
            'api_key': self.api_key,
            'min_value': self.min_value_usd,
            'limit': 10
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(self.BASE_URL, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self._process_transactions(data.get('transactions', []))
                else:
                    logger.warning(f"[WHALE] API request failed: {resp.status}")

    def _process_transactions(self, transactions: List[dict]):
        # Clear old events periodically or just append new ones? 
        # For simplicity, we replace the list with the latest batch, 
        # assuming the batch covers the relevant "recent" window.
        # Ideally we'd maintain a time window.
        
        current_batch: Dict[str, List[dict]] = defaultdict(list)
        
        symbol_map = {
            'btc': 'BTC-USDT',
            'eth': 'ETH-USDT',
            'sol': 'SOL-USDT'
        }
        
        for tx in transactions:
            symbol_lower = tx.get('symbol', '').lower()
            if symbol_lower in symbol_map:
                std_symbol = symbol_map[symbol_lower]
                amount_usd = tx.get('amount_usd', 0)
                
                event = {
                    'timestamp': tx.get('timestamp'),
                    'amount_usd': amount_usd,
                    'from_type': tx.get('from', {}).get('owner_type', 'unknown'),
                    'to_type': tx.get('to', {}).get('owner_type', 'unknown'),
                    'hash': tx.get('hash')
                }
                
                current_batch[std_symbol].append(event)
                logger.info(f"[WHALE] {std_symbol} ${amount_usd:,.0f}")
                
        # Merge or update? 
        # For now, we update our latest snapshot to be these transactions.
        # A more complex logic would be to keep them for X minutes.
        for sym, events in current_batch.items():
            self.latest_events[sym] = events

    def get_recent_whales(self, symbol: str, window_seconds: int = 300) -> List[dict]:
        """Get whale events for a symbol within the last N seconds."""
        # Note: 'timestamp' from API is unix seconds
        now = datetime.utcnow().timestamp()
        cutoff = now - window_seconds
        
        # We need to filter 'latest_events' (which might be from previous polls)
        # Actually, since we overwrite 'latest_events' on poll, it might lose info if we poll slowly.
        # But assuming 60s poll and 300s window, we might miss older ones if we overwrite.
        # IMPROVEMENT: Append to a deque in _process_transactions.
        # For now, simplified: return what we have if it's recent.
        
        events = self.latest_events.get(symbol, [])
        valid_events = [e for e in events if e['timestamp'] >= cutoff]
        return valid_events
