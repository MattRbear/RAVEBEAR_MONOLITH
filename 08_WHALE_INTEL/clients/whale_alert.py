"""
Flint's Whale Intelligence System - Whale Alert Client

Purpose:
    Primary trigger layer - connects to Whale Alert WebSocket/REST API
    to receive real-time whale transaction alerts.
    
    This is the "Tip of the Spear" - all downstream processing starts here.

Inputs:
    - Whale Alert API key (Premium)
    - Minimum USD threshold ($500k default)
    
Outputs:
    - Parsed, classified whale transactions
    - Stored in database
    - Pushed to processing queue

Failure Modes:
    - WebSocket disconnect: Auto-reconnect with backoff
    - Rate limit: Backs off, logs warning
    - Invalid message: Logs, skips (doesn't crash)
    - Timestamp drift > 5min: Logs warning, still processes

Transaction Classification:
    - EXCHANGE_INFLOW: To exchange (selling pressure)
    - EXCHANGE_OUTFLOW: From exchange (accumulation)
    - EXCHANGE_INTERNAL: Exchange to exchange
    - WHALE_TRANSFER: Between unknown wallets
    - MINT: New tokens minted
    - BURN: Tokens destroyed

Logging:
    - INFO: New whale transaction received
    - WARNING: Reconnection, rate limit
    - ERROR: Parse failure, storage failure

Usage:
    client = WhaleAlertClient.from_config(config)
    await client.connect()
    await client.run()  # Blocks, processes messages
"""

import json
import asyncio
import aiohttp
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Callable, Set
from datetime import datetime, timezone, timedelta
from enum import Enum

from .base_client import BaseClient, BackoffConfig, ConnectionState

logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# =============================================================================

# Known exchange labels (Whale Alert uses these)
EXCHANGE_LABELS = frozenset([
    'binance', 'coinbase', 'kraken', 'bitfinex', 'huobi',
    'okex', 'okx', 'kucoin', 'bybit', 'ftx', 'gemini', 'bitstamp',
    'bittrex', 'gate.io', 'gateio', 'crypto.com', 'cryptocom',
    'deribit', 'mexc', 'bitget', 'bitmex', 'poloniex', 'upbit',
    'coinone', 'korbit', 'bithumb', 'liquid', 'bitflyer',
])

# Supported currencies
SUPPORTED_CURRENCIES = frozenset([
    'btc', 'eth', 'usdt', 'usdc', 'xrp', 'sol', 'ada', 'avax',
    'doge', 'matic', 'link', 'dot', 'shib', 'ltc', 'bch', 'uni',
    'xlm', 'atom', 'etc', 'fil', 'ape', 'near', 'algo', 'vet',
])

# Stablecoins (for filtering sentiment queries)
STABLECOINS = frozenset(['usdt', 'usdc', 'dai', 'busd', 'tusd', 'usdp', 'gusd', 'frax'])


# =============================================================================
# ENUMS
# =============================================================================

class TransactionType(Enum):
    """Whale transaction classification types."""
    EXCHANGE_INFLOW = "exchange_inflow"      # To exchange (bearish)
    EXCHANGE_OUTFLOW = "exchange_outflow"    # From exchange (bullish)
    EXCHANGE_INTERNAL = "exchange_internal"  # Exchange to exchange
    WHALE_TRANSFER = "whale_transfer"        # Unknown to unknown
    MINT = "mint"                            # New tokens
    BURN = "burn"                            # Tokens destroyed
    UNKNOWN = "unknown"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class WhaleTransaction:
    """Parsed whale transaction."""
    # Identifiers
    tx_hash: str
    whale_alert_id: str
    blockchain: str
    
    # Timing
    timestamp: datetime
    block_number: Optional[int] = None
    
    # Addresses
    from_address: str = ""
    from_owner: str = ""
    from_owner_type: str = ""
    to_address: str = ""
    to_owner: str = ""
    to_owner_type: str = ""
    
    # Token
    symbol: str = ""
    token_contract: Optional[str] = None
    
    # Values
    amount: float = 0.0
    amount_usd: float = 0.0
    
    # Classification
    tx_type: TransactionType = TransactionType.UNKNOWN
    
    # Flags
    is_exchange_inflow: bool = False
    is_exchange_outflow: bool = False
    is_stablecoin: bool = False
    
    # Metadata
    raw_data: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "tx_hash": self.tx_hash,
            "whale_alert_id": self.whale_alert_id,
            "blockchain": self.blockchain,
            "timestamp": self.timestamp.isoformat(),
            "block_number": self.block_number,
            "from_address": self.from_address,
            "from_owner": self.from_owner,
            "from_owner_type": self.from_owner_type,
            "to_address": self.to_address,
            "to_owner": self.to_owner,
            "to_owner_type": self.to_owner_type,
            "symbol": self.symbol,
            "token_contract": self.token_contract,
            "amount": self.amount,
            "amount_usd": self.amount_usd,
            "tx_type": self.tx_type.value,
            "is_exchange_inflow": self.is_exchange_inflow,
            "is_exchange_outflow": self.is_exchange_outflow,
            "is_stablecoin": self.is_stablecoin,
        }


# =============================================================================
# WHALE ALERT CLIENT
# =============================================================================

class WhaleAlertClient(BaseClient):
    """
    Whale Alert API client.
    
    Supports both REST polling and WebSocket (when available).
    Primary trigger for the waterfall validation system.
    """
    
    def __init__(
        self,
        api_key: str,
        min_value_usd: float = 500_000,
        poll_interval: float = 60.0,
        config: Any = None,
    ):
        """
        Initialize Whale Alert client.
        
        Args:
            api_key: Whale Alert API key (Premium recommended)
            min_value_usd: Minimum transaction value in USD
            poll_interval: Seconds between REST polls
            config: Configuration object
        """
        super().__init__(
            api_name="whale_alert",
            config=config,
            backoff=BackoffConfig(
                initial_delay=5.0,
                max_delay=120.0,
                max_attempts=10,
            ),
        )
        
        self.api_key = api_key
        self.min_value_usd = min_value_usd
        self.poll_interval = poll_interval
        
        # API URLs
        self.rest_url = "https://api.whale-alert.io/v1"
        
        # State
        self._session: Optional[aiohttp.ClientSession] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._seen_tx_hashes: Set[str] = set()  # Deduplication
        self._max_seen_cache = 10000  # Prevent unbounded growth
        
        # Transaction handlers
        self._transaction_handlers: List[Callable] = []
        
        # Metrics
        self._transactions_received = 0
        self._transactions_filtered = 0
        self._api_calls = 0
        
        logger.info(f"WhaleAlertClient initialized: min_value=${min_value_usd:,.0f}")
    
    @classmethod
    def from_config(cls, config) -> "WhaleAlertClient":
        """Create client from Config object."""
        return cls(
            api_key=config.whale_alert.api_key,
            min_value_usd=config.thresholds.whale_min_usd,
            config=config,
        )
    
    def add_transaction_handler(self, handler: Callable[[WhaleTransaction], Any]) -> None:
        """
        Add a transaction handler callback.
        
        Handler receives parsed WhaleTransaction objects.
        """
        self._transaction_handlers.append(handler)
    
    async def _connect(self) -> None:
        """Establish connection (create HTTP session)."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={"Accept": "application/json"},
            )
    
    async def _disconnect(self) -> None:
        """Close connection."""
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
    
    async def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """
        Make rate-limited API request.
        
        Args:
            endpoint: API endpoint (e.g., "/transactions")
            params: Query parameters
            
        Returns:
            JSON response or None on failure
        """
        if not self._acquire_rate_limit():
            logger.warning("Whale Alert rate limit exhausted")
            return None
        
        params = params or {}
        params["api_key"] = self.api_key
        
        url = f"{self.rest_url}{endpoint}"
        
        try:
            async with self._session.get(url, params=params) as resp:
                self._api_calls += 1
                self._record_activity()
                
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    logger.warning(f"Whale Alert rate limited, retry after {retry_after}s")
                    await asyncio.sleep(retry_after)
                    return None
                
                if resp.status != 200:
                    logger.error(f"Whale Alert API error: {resp.status}")
                    return None
                
                return await resp.json()
                
        except asyncio.TimeoutError:
            self._record_error("Request timeout")
            return None
        except Exception as e:
            self._record_error(f"Request failed: {e}")
            return None
    
    def _classify_transaction(self, tx_data: Dict) -> TransactionType:
        """
        Classify transaction type based on from/to addresses.
        
        Args:
            tx_data: Raw transaction data from API
            
        Returns:
            TransactionType enum
        """
        tx_type_raw = tx_data.get("transaction_type", "").lower()
        
        # Check for mint/burn
        if tx_type_raw == "mint":
            return TransactionType.MINT
        elif tx_type_raw == "burn":
            return TransactionType.BURN
        
        # Get owner info
        from_data = tx_data.get("from", {})
        to_data = tx_data.get("to", {})
        
        from_owner = (from_data.get("owner", "") or "").lower()
        to_owner = (to_data.get("owner", "") or "").lower()
        from_type = (from_data.get("owner_type", "") or "").lower()
        to_type = (to_data.get("owner_type", "") or "").lower()
        
        # Check if addresses are exchanges
        from_is_exchange = (
            from_type == "exchange" or
            any(ex in from_owner for ex in EXCHANGE_LABELS)
        )
        to_is_exchange = (
            to_type == "exchange" or
            any(ex in to_owner for ex in EXCHANGE_LABELS)
        )
        
        # Classify
        if from_is_exchange and to_is_exchange:
            return TransactionType.EXCHANGE_INTERNAL
        elif to_is_exchange and not from_is_exchange:
            return TransactionType.EXCHANGE_INFLOW
        elif from_is_exchange and not to_is_exchange:
            return TransactionType.EXCHANGE_OUTFLOW
        else:
            return TransactionType.WHALE_TRANSFER
    
    def _parse_transaction(self, tx_data: Dict) -> Optional[WhaleTransaction]:
        """
        Parse raw transaction data into WhaleTransaction.
        
        Args:
            tx_data: Raw transaction data from API
            
        Returns:
            WhaleTransaction or None if invalid
        """
        try:
            # Extract basic fields
            tx_hash = tx_data.get("hash", "")
            whale_id = tx_data.get("id", "")
            
            if not tx_hash:
                logger.warning("Transaction missing hash")
                return None
            
            # Parse timestamp
            ts_raw = tx_data.get("timestamp", 0)
            if isinstance(ts_raw, (int, float)):
                timestamp = datetime.fromtimestamp(ts_raw, tz=timezone.utc)
            else:
                timestamp = datetime.now(timezone.utc)
            
            # Check timestamp drift
            drift = abs((datetime.now(timezone.utc) - timestamp).total_seconds())
            if drift > 300:  # 5 minutes
                logger.warning(f"Transaction timestamp drift: {drift:.0f}s", extra={
                    "tx_hash": tx_hash[:16],
                    "drift_seconds": drift,
                })
            
            # Extract address info
            from_data = tx_data.get("from", {})
            to_data = tx_data.get("to", {})
            
            # Get symbol and check if stablecoin
            symbol = (tx_data.get("symbol", "") or "").lower()
            is_stablecoin = symbol in STABLECOINS
            
            # Classify
            tx_type = self._classify_transaction(tx_data)
            
            # Build transaction
            tx = WhaleTransaction(
                tx_hash=tx_hash,
                whale_alert_id=whale_id,
                blockchain=tx_data.get("blockchain", "unknown"),
                timestamp=timestamp,
                block_number=tx_data.get("block_height"),
                from_address=from_data.get("address", ""),
                from_owner=from_data.get("owner", ""),
                from_owner_type=from_data.get("owner_type", ""),
                to_address=to_data.get("address", ""),
                to_owner=to_data.get("owner", ""),
                to_owner_type=to_data.get("owner_type", ""),
                symbol=symbol.upper(),
                amount=float(tx_data.get("amount", 0)),
                amount_usd=float(tx_data.get("amount_usd", 0)),
                tx_type=tx_type,
                is_exchange_inflow=tx_type == TransactionType.EXCHANGE_INFLOW,
                is_exchange_outflow=tx_type == TransactionType.EXCHANGE_OUTFLOW,
                is_stablecoin=is_stablecoin,
                raw_data=tx_data,
            )
            
            return tx
            
        except Exception as e:
            logger.error(f"Failed to parse transaction: {e}", extra={
                "error": str(e),
                "tx_data_preview": str(tx_data)[:200],
            })
            return None
    
    def _is_duplicate(self, tx_hash: str) -> bool:
        """Check if transaction was already processed."""
        if tx_hash in self._seen_tx_hashes:
            return True
        
        # Add to seen set
        self._seen_tx_hashes.add(tx_hash)
        
        # Prune if too large
        if len(self._seen_tx_hashes) > self._max_seen_cache:
            # Remove oldest half (set doesn't maintain order, so random removal)
            to_remove = len(self._seen_tx_hashes) - self._max_seen_cache // 2
            for _ in range(to_remove):
                self._seen_tx_hashes.pop()
        
        return False
    
    async def _process_transaction(self, tx: WhaleTransaction) -> None:
        """
        Process a parsed transaction - call handlers.
        
        Args:
            tx: Parsed whale transaction
        """
        for handler in self._transaction_handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(tx)
                else:
                    handler(tx)
            except Exception as e:
                logger.error(f"Transaction handler error: {e}", extra={
                    "tx_hash": tx.tx_hash[:16],
                    "error": str(e),
                })

    async def fetch_recent_transactions(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        min_value: Optional[float] = None,
        cursor: Optional[str] = None,
    ) -> List[WhaleTransaction]:
        """
        Fetch recent transactions from REST API.
        
        Args:
            start_time: Start of time range (default: 1 hour ago)
            end_time: End of time range (default: now)
            min_value: Minimum USD value (default: self.min_value_usd)
            cursor: Pagination cursor
            
        Returns:
            List of parsed transactions
        """
        # Set defaults
        if end_time is None:
            end_time = datetime.now(timezone.utc)
        if start_time is None:
            start_time = end_time - timedelta(hours=1)
        if min_value is None:
            min_value = self.min_value_usd
        
        # Build params
        params = {
            "start": int(start_time.timestamp()),
            "end": int(end_time.timestamp()),
            "min_value": int(min_value),
        }
        if cursor:
            params["cursor"] = cursor
        
        # Make request
        data = await self._make_request("/transactions", params)
        
        if not data:
            return []
        
        if data.get("result") != "success":
            logger.error(f"Whale Alert API error: {data.get('message', 'Unknown')}")
            return []
        
        # Parse transactions
        transactions = []
        for tx_data in data.get("transactions", []):
            self._transactions_received += 1
            
            # Check USD threshold
            amount_usd = float(tx_data.get("amount_usd", 0))
            if amount_usd < self.min_value_usd:
                self._transactions_filtered += 1
                continue
            
            # Check for duplicate
            tx_hash = tx_data.get("hash", "")
            if self._is_duplicate(tx_hash):
                continue
            
            # Parse
            tx = self._parse_transaction(tx_data)
            if tx:
                transactions.append(tx)
        
        logger.info(f"Fetched {len(transactions)} whale transactions", extra={
            "count": len(transactions),
            "start": start_time.isoformat(),
            "end": end_time.isoformat(),
            "min_value": min_value,
        })
        
        return transactions
    
    async def _poll_loop(self) -> None:
        """
        Main polling loop for REST API.
        
        Continuously fetches new transactions and processes them.
        """
        last_poll_time = datetime.now(timezone.utc) - timedelta(minutes=5)
        
        while self._state == ConnectionState.CONNECTED:
            try:
                # Fetch new transactions
                now = datetime.now(timezone.utc)
                transactions = await self.fetch_recent_transactions(
                    start_time=last_poll_time - timedelta(seconds=30),  # Overlap for safety
                    end_time=now,
                )
                
                # Process each transaction
                for tx in transactions:
                    # Log the whale
                    logger.info(
                        f"🐋 WHALE: {tx.symbol} ${tx.amount_usd:,.0f} | {tx.tx_type.value}",
                        extra=tx.to_dict()
                    )
                    
                    # Call handlers
                    await self._process_transaction(tx)
                
                last_poll_time = now
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._record_error(f"Poll loop error: {e}")
            
            # Wait for next poll
            await asyncio.sleep(self.poll_interval)
    
    async def run(self) -> None:
        """
        Run the client (connects and starts polling).
        
        This is a blocking call that runs until disconnect.
        """
        # Connect if needed
        if not self.is_connected:
            success = await self.connect()
            if not success:
                logger.error("Failed to connect to Whale Alert")
                return
        
        # Start polling
        logger.info("Starting Whale Alert polling loop")
        self._poll_task = asyncio.create_task(self._poll_loop())
        
        try:
            await self._poll_task
        except asyncio.CancelledError:
            pass
        finally:
            await self.disconnect()
    
    async def get_status(self) -> Dict[str, Any]:
        """Get current Whale Alert status."""
        data = await self._make_request("/status")
        return data or {"error": "Failed to fetch status"}
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get client metrics."""
        base_metrics = super().get_metrics()
        base_metrics.update({
            "transactions_received": self._transactions_received,
            "transactions_filtered": self._transactions_filtered,
            "api_calls": self._api_calls,
            "min_value_usd": self.min_value_usd,
            "poll_interval": self.poll_interval,
            "seen_cache_size": len(self._seen_tx_hashes),
        })
        return base_metrics
    
    def health_check(self) -> Dict[str, Any]:
        """Get health status."""
        base_health = super().health_check()
        base_health.update({
            "transactions_received": self._transactions_received,
            "api_calls": self._api_calls,
        })
        return base_health


# =============================================================================
# CLI TEST
# =============================================================================

if __name__ == "__main__":
    """Test Whale Alert client."""
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s'
    )
    
    async def test_client():
        # Load config
        sys.path.insert(0, str(__file__).rsplit('clients', 1)[0])
        from config import get_config
        
        try:
            config = get_config()
        except Exception as e:
            print(f"❌ Config error: {e}")
            return
        
        # Create client
        client = WhaleAlertClient.from_config(config)
        
        # Add test handler
        def print_whale(tx: WhaleTransaction):
            direction = "📥" if tx.is_exchange_inflow else "📤" if tx.is_exchange_outflow else "↔️"
            print(f"{direction} {tx.symbol} ${tx.amount_usd:,.0f} | {tx.from_owner or tx.from_address[:10]} → {tx.to_owner or tx.to_address[:10]}")
        
        client.add_transaction_handler(print_whale)
        
        print("=" * 60)
        print("WHALE ALERT CLIENT TEST")
        print("=" * 60)
        print(f"Min Value: ${config.thresholds.whale_min_usd:,.0f}")
        print()
        
        # Connect
        print("Connecting...")
        await client.connect()
        print(f"✅ Connected: {client.is_connected}")
        
        # Fetch recent
        print("\nFetching recent whale transactions...")
        txs = await client.fetch_recent_transactions()
        print(f"Found {len(txs)} transactions")
        
        # Show transactions
        for tx in txs[:10]:
            print_whale(tx)
        
        # Health check
        print("\nHealth Check:")
        health = client.health_check()
        print(f"  Healthy: {health.get('healthy')}")
        print(f"  State: {health.get('state')}")
        print(f"  API Calls: {health.get('api_calls')}")
        
        # Disconnect
        await client.disconnect()
        print("\n✅ Test complete!")
    
    asyncio.run(test_client())
