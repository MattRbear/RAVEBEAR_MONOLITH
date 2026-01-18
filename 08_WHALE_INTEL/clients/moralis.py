"""
Flint's Whale Intelligence System - Moralis Client

Purpose:
    Cross-chain portfolio intelligence for whale wallets.
    Only called for transactions >$10M to conserve API budget.
    
    Called AFTER Alchemy validation confirms whale status.

Inputs:
    - Moralis API key
    - Wallet addresses to analyze
    
Outputs:
    - Cross-chain token holdings
    - NFT holdings
    - Wallet net worth
    - Transaction history

Failure Modes:
    - Rate limit (400/day): Fails closed, logs critical
    - API error: Returns None, logs warning
    - Invalid address: Returns empty portfolio

Budget Strategy:
    - FREE tier: 400 calls/day
    - Gate: Only query wallets with >$10M transactions
    - Cache: 1 hour TTL to avoid redundant calls
    - Expected: ~50-100 mega-whale queries per day

Logging:
    - INFO: Portfolio queries
    - WARNING: Rate limits, API errors
    - CRITICAL: Budget exhausted

Usage:
    client = MoralisClient.from_config(config)
    await client.connect()
    
    # Get wallet portfolio
    portfolio = await client.get_wallet_portfolio("0x...")
    
    # Get net worth
    worth = await client.get_net_worth("0x...")
"""

import json
import asyncio
import aiohttp
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta
from enum import Enum

from .base_client import BaseClient, BackoffConfig

logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# =============================================================================

MORALIS_API_URL = "https://deep-index.moralis.io/api/v2.2"

# Chain IDs
class Chain(Enum):
    ETHEREUM = "0x1"
    POLYGON = "0x89"
    BSC = "0x38"
    ARBITRUM = "0xa4b1"
    OPTIMISM = "0xa"
    AVALANCHE = "0xa86a"
    BASE = "0x2105"
    FANTOM = "0xfa"
    CRONOS = "0x19"


CHAIN_NAMES = {
    Chain.ETHEREUM: "Ethereum",
    Chain.POLYGON: "Polygon",
    Chain.BSC: "BSC",
    Chain.ARBITRUM: "Arbitrum",
    Chain.OPTIMISM: "Optimism",
    Chain.AVALANCHE: "Avalanche",
    Chain.BASE: "Base",
    Chain.FANTOM: "Fantom",
    Chain.CRONOS: "Cronos",
}

# Default chains to query (most common for whales)
DEFAULT_CHAINS = [
    Chain.ETHEREUM,
    Chain.POLYGON,
    Chain.BSC,
    Chain.ARBITRUM,
]


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class TokenHolding:
    """Single token holding."""
    chain: str
    token_address: Optional[str]  # None for native
    symbol: str
    name: str
    decimals: int
    balance_raw: str
    balance_decimal: float
    usd_value: Optional[float] = None
    usd_price: Optional[float] = None
    logo: Optional[str] = None
    
    @property
    def is_native(self) -> bool:
        return self.token_address is None


@dataclass
class NFTHolding:
    """Single NFT holding."""
    chain: str
    contract_address: str
    token_id: str
    name: Optional[str] = None
    symbol: Optional[str] = None
    collection_name: Optional[str] = None
    metadata: Optional[Dict] = None


@dataclass
class WalletPortfolio:
    """Complete wallet portfolio."""
    address: str
    tokens: List[TokenHolding] = field(default_factory=list)
    nfts: List[NFTHolding] = field(default_factory=list)
    total_usd_value: float = 0.0
    chains_queried: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    @property
    def token_count(self) -> int:
        return len(self.tokens)
    
    @property
    def nft_count(self) -> int:
        return len(self.nfts)
    
    def get_top_holdings(self, n: int = 10) -> List[TokenHolding]:
        """Get top N holdings by USD value."""
        valued = [t for t in self.tokens if t.usd_value and t.usd_value > 0]
        return sorted(valued, key=lambda x: x.usd_value or 0, reverse=True)[:n]


@dataclass
class CacheEntry:
    """Cached result with TTL."""
    data: Any
    timestamp: datetime
    ttl_seconds: int = 3600  # 1 hour default
    
    @property
    def is_expired(self) -> bool:
        age = (datetime.now(timezone.utc) - self.timestamp).total_seconds()
        return age > self.ttl_seconds


# =============================================================================
# MORALIS CLIENT
# =============================================================================

class MoralisClient(BaseClient):
    """
    Moralis API client for cross-chain wallet intelligence.
    
    Features:
    - Multi-chain token balances
    - NFT holdings
    - Wallet net worth
    - 400/day budget tracking
    - Result caching (1hr TTL)
    """
    
    def __init__(
        self,
        api_key: str,
        min_value_usd: float = 10_000_000,  # $10M gate
        cache_ttl: int = 3600,  # 1 hour
        config: Any = None,
    ):
        """
        Initialize Moralis client.
        
        Args:
            api_key: Moralis API key
            min_value_usd: Minimum transaction USD to query (gate)
            cache_ttl: Cache TTL in seconds
            config: Configuration object
        """
        super().__init__(
            api_name="moralis",
            config=config,
            backoff=BackoffConfig(
                initial_delay=1.0,
                max_delay=30.0,
                max_attempts=3,
            ),
        )
        
        self.api_key = api_key
        self.min_value_usd = min_value_usd
        self.cache_ttl = cache_ttl
        
        # HTTP session
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Result cache
        self._cache: Dict[str, CacheEntry] = {}
        
        # Stats
        self._api_calls = 0
        self._cache_hits = 0
        
        logger.info(f"MoralisClient initialized: gate=${min_value_usd:,.0f}")
    
    @classmethod
    def from_config(cls, config) -> "MoralisClient":
        """Create client from Config object."""
        return cls(
            api_key=config.moralis.api_key,
            min_value_usd=config.thresholds.mega_whale_min_usd if hasattr(config, 'thresholds') else 10_000_000,
            config=config,
        )
    
    async def _connect(self) -> None:
        """Create HTTP session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={
                    "X-API-Key": self.api_key,
                    "Accept": "application/json",
                },
            )
    
    async def _disconnect(self) -> None:
        """Close HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
    
    def _get_cached(self, key: str) -> Optional[Any]:
        """Get cached result if not expired."""
        if key in self._cache:
            entry = self._cache[key]
            if not entry.is_expired:
                self._cache_hits += 1
                return entry.data
            else:
                del self._cache[key]
        return None
    
    def _set_cached(self, key: str, data: Any) -> None:
        """Cache result with TTL."""
        self._cache[key] = CacheEntry(
            data=data,
            timestamp=datetime.now(timezone.utc),
            ttl_seconds=self.cache_ttl,
        )
    
    async def _api_call(
        self,
        endpoint: str,
        params: Dict = None,
    ) -> Optional[Any]:
        """
        Make Moralis API call with rate limiting.
        
        Args:
            endpoint: API endpoint (without base URL)
            params: Query parameters
            
        Returns:
            API result or None on failure
        """
        if not self._acquire_rate_limit():
            logger.critical("Moralis daily budget exhausted!")
            return None
        
        url = f"{MORALIS_API_URL}/{endpoint}"
        
        try:
            async with self._session.get(url, params=params) as resp:
                self._record_activity()
                self._api_calls += 1
                
                if resp.status == 429:
                    logger.warning("Moralis rate limited")
                    return None
                
                if resp.status == 401:
                    logger.error("Moralis API key invalid")
                    return None
                
                if resp.status != 200:
                    logger.error(f"Moralis HTTP error: {resp.status}")
                    return None
                
                return await resp.json()
                
        except asyncio.TimeoutError:
            self._record_error("Request timeout")
            return None
        except Exception as e:
            self._record_error(f"API call failed: {e}")
            return None

    # =========================================================================
    # WALLET PORTFOLIO
    # =========================================================================
    
    async def get_wallet_tokens(
        self,
        address: str,
        chain: Chain = Chain.ETHEREUM,
    ) -> List[TokenHolding]:
        """
        Get all token balances for a wallet on a specific chain.
        
        Args:
            address: Wallet address
            chain: Chain to query
            
        Returns:
            List of TokenHolding
        """
        cache_key = f"tokens:{address.lower()}:{chain.value}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        
        result = await self._api_call(
            f"{address}/erc20",
            params={"chain": chain.value}
        )
        
        holdings = []
        if result and isinstance(result, list):
            for token in result:
                try:
                    holdings.append(TokenHolding(
                        chain=CHAIN_NAMES.get(chain, chain.value),
                        token_address=token.get("token_address"),
                        symbol=token.get("symbol", "???"),
                        name=token.get("name", "Unknown"),
                        decimals=int(token.get("decimals", 18)),
                        balance_raw=token.get("balance", "0"),
                        balance_decimal=float(token.get("balance", 0)) / (10 ** int(token.get("decimals", 18))),
                        usd_value=float(token.get("usd_value")) if token.get("usd_value") else None,
                        usd_price=float(token.get("usd_price")) if token.get("usd_price") else None,
                        logo=token.get("logo"),
                    ))
                except Exception as e:
                    logger.debug(f"Failed to parse token: {e}")
        
        self._set_cached(cache_key, holdings)
        return holdings
    
    async def get_wallet_native_balance(
        self,
        address: str,
        chain: Chain = Chain.ETHEREUM,
    ) -> Optional[TokenHolding]:
        """
        Get native token balance (ETH, MATIC, BNB, etc.).
        
        Args:
            address: Wallet address
            chain: Chain to query
            
        Returns:
            TokenHolding for native token
        """
        cache_key = f"native:{address.lower()}:{chain.value}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        
        result = await self._api_call(
            f"{address}/balance",
            params={"chain": chain.value}
        )
        
        if result and "balance" in result:
            native_symbols = {
                Chain.ETHEREUM: "ETH",
                Chain.POLYGON: "MATIC",
                Chain.BSC: "BNB",
                Chain.ARBITRUM: "ETH",
                Chain.OPTIMISM: "ETH",
                Chain.AVALANCHE: "AVAX",
                Chain.BASE: "ETH",
                Chain.FANTOM: "FTM",
                Chain.CRONOS: "CRO",
            }
            
            balance_raw = result["balance"]
            balance_decimal = int(balance_raw) / 10**18
            
            holding = TokenHolding(
                chain=CHAIN_NAMES.get(chain, chain.value),
                token_address=None,
                symbol=native_symbols.get(chain, "???"),
                name=f"{native_symbols.get(chain, 'Native')} ({CHAIN_NAMES.get(chain, 'Unknown')})",
                decimals=18,
                balance_raw=balance_raw,
                balance_decimal=balance_decimal,
            )
            
            self._set_cached(cache_key, holding)
            return holding
        
        return None
    
    async def get_wallet_portfolio(
        self,
        address: str,
        chains: List[Chain] = None,
        include_nfts: bool = False,
    ) -> WalletPortfolio:
        """
        Get complete wallet portfolio across multiple chains.
        
        Args:
            address: Wallet address
            chains: Chains to query (default: ETH, Polygon, BSC, Arbitrum)
            include_nfts: Whether to include NFT holdings
            
        Returns:
            WalletPortfolio with all holdings
        """
        if chains is None:
            chains = DEFAULT_CHAINS
        
        portfolio = WalletPortfolio(address=address.lower())
        
        for chain in chains:
            chain_name = CHAIN_NAMES.get(chain, chain.value)
            portfolio.chains_queried.append(chain_name)
            
            # Get native balance
            native = await self.get_wallet_native_balance(address, chain)
            if native:
                portfolio.tokens.append(native)
            
            # Get ERC20 tokens
            tokens = await self.get_wallet_tokens(address, chain)
            portfolio.tokens.extend(tokens)
            
            # Get NFTs if requested
            if include_nfts:
                nfts = await self.get_wallet_nfts(address, chain)
                portfolio.nfts.extend(nfts)
        
        # Calculate total USD value
        portfolio.total_usd_value = sum(
            t.usd_value for t in portfolio.tokens if t.usd_value
        )
        
        return portfolio
    
    async def get_net_worth(
        self,
        address: str,
        chains: List[Chain] = None,
    ) -> Optional[float]:
        """
        Get wallet net worth in USD.
        
        Uses Moralis net worth endpoint for accurate pricing.
        
        Args:
            address: Wallet address
            chains: Chains to include
            
        Returns:
            Net worth in USD or None
        """
        cache_key = f"networth:{address.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        
        # Use the wallet net worth endpoint
        result = await self._api_call(
            f"wallets/{address}/net-worth",
            params={"chains": ",".join(c.value for c in (chains or DEFAULT_CHAINS))}
        )
        
        if result and "total_networth_usd" in result:
            net_worth = float(result["total_networth_usd"])
            self._set_cached(cache_key, net_worth)
            return net_worth
        
        return None
    
    # =========================================================================
    # NFT HOLDINGS
    # =========================================================================
    
    async def get_wallet_nfts(
        self,
        address: str,
        chain: Chain = Chain.ETHEREUM,
        limit: int = 100,
    ) -> List[NFTHolding]:
        """
        Get NFT holdings for a wallet.
        
        Args:
            address: Wallet address
            chain: Chain to query
            limit: Max NFTs to return
            
        Returns:
            List of NFTHolding
        """
        cache_key = f"nfts:{address.lower()}:{chain.value}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        
        result = await self._api_call(
            f"{address}/nft",
            params={"chain": chain.value, "limit": limit}
        )
        
        holdings = []
        if result and "result" in result:
            for nft in result["result"]:
                try:
                    metadata = None
                    if nft.get("metadata"):
                        try:
                            metadata = json.loads(nft["metadata"]) if isinstance(nft["metadata"], str) else nft["metadata"]
                        except json.JSONDecodeError:
                            pass
                    
                    holdings.append(NFTHolding(
                        chain=CHAIN_NAMES.get(chain, chain.value),
                        contract_address=nft.get("token_address", ""),
                        token_id=nft.get("token_id", ""),
                        name=nft.get("name"),
                        symbol=nft.get("symbol"),
                        collection_name=nft.get("name"),
                        metadata=metadata,
                    ))
                except Exception as e:
                    logger.debug(f"Failed to parse NFT: {e}")
        
        self._set_cached(cache_key, holdings)
        return holdings

    # =========================================================================
    # TRANSACTION HISTORY
    # =========================================================================
    
    async def get_wallet_transactions(
        self,
        address: str,
        chain: Chain = Chain.ETHEREUM,
        limit: int = 100,
    ) -> List[Dict]:
        """
        Get transaction history for a wallet.
        
        Args:
            address: Wallet address
            chain: Chain to query
            limit: Max transactions to return
            
        Returns:
            List of transaction dicts
        """
        result = await self._api_call(
            f"{address}",
            params={"chain": chain.value, "limit": limit}
        )
        
        if result and "result" in result:
            return result["result"]
        return []
    
    # =========================================================================
    # GATE CHECK
    # =========================================================================
    
    def should_query(self, usd_value: float) -> bool:
        """
        Check if a transaction meets the threshold for Moralis query.
        
        Gate: Only query for mega-whales to conserve 400/day budget.
        
        Args:
            usd_value: Transaction value in USD
            
        Returns:
            True if should query Moralis
        """
        return usd_value >= self.min_value_usd
    
    # =========================================================================
    # HEALTH & METRICS
    # =========================================================================
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get client metrics."""
        base_metrics = super().get_metrics()
        base_metrics.update({
            "api_calls": self._api_calls,
            "cache_hits": self._cache_hits,
            "cache_size": len(self._cache),
            "min_value_gate": self.min_value_usd,
        })
        return base_metrics
    
    def health_check(self) -> Dict[str, Any]:
        """Get health status."""
        base_health = super().health_check()
        base_health.update({
            "api_calls": self._api_calls,
            "cache_hits": self._cache_hits,
            "gate_usd": f"${self.min_value_usd:,.0f}",
        })
        return base_health
    
    def clear_cache(self) -> int:
        """Clear expired cache entries. Returns count cleared."""
        expired = [k for k, v in self._cache.items() if v.is_expired]
        for k in expired:
            del self._cache[k]
        return len(expired)


# =============================================================================
# CLI TEST
# =============================================================================

if __name__ == "__main__":
    """Test Moralis client."""
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
        client = MoralisClient.from_config(config)
        
        print("=" * 60)
        print("MORALIS CLIENT TEST")
        print("=" * 60)
        
        # Connect
        print("\nConnecting...")
        await client.connect()
        print(f"✅ Connected: {client.is_connected}")
        print(f"   Gate: ${client.min_value_usd:,.0f}")
        
        # Test with Vitalik's address
        vitalik = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
        
        # Get native balance
        print(f"\nGetting ETH balance for Vitalik...")
        native = await client.get_wallet_native_balance(vitalik, Chain.ETHEREUM)
        if native:
            print(f"✅ Native Balance: {native.balance_decimal:,.4f} {native.symbol}")
        
        # Get ERC20 tokens
        print(f"\nGetting ERC20 tokens...")
        tokens = await client.get_wallet_tokens(vitalik, Chain.ETHEREUM)
        print(f"✅ Found {len(tokens)} tokens")
        
        if tokens:
            print("   Top tokens by value:")
            valued = sorted([t for t in tokens if t.usd_value], key=lambda x: x.usd_value or 0, reverse=True)[:5]
            for t in valued:
                print(f"   - {t.symbol}: ${t.usd_value:,.2f}" if t.usd_value else f"   - {t.symbol}: {t.balance_decimal:,.4f}")
        
        # Get net worth (if endpoint available)
        print(f"\nGetting net worth...")
        net_worth = await client.get_net_worth(vitalik)
        if net_worth:
            print(f"✅ Net Worth: ${net_worth:,.2f}")
        else:
            print("⚠️ Net worth endpoint not available (may need paid tier)")
        
        # Metrics
        print("\nMetrics:")
        metrics = client.get_metrics()
        print(f"   API Calls: {metrics['api_calls']}")
        print(f"   Cache Hits: {metrics['cache_hits']}")
        print(f"   Cache Size: {metrics['cache_size']}")
        
        # Gate check
        print("\nGate Check:")
        print(f"   $1M transaction: {'QUERY' if client.should_query(1_000_000) else 'SKIP'}")
        print(f"   $10M transaction: {'QUERY' if client.should_query(10_000_000) else 'SKIP'}")
        print(f"   $50M transaction: {'QUERY' if client.should_query(50_000_000) else 'SKIP'}")
        
        # Disconnect
        await client.disconnect()
        print("\n✅ Test complete!")
    
    asyncio.run(test_client())
