"""
Flint's Whale Intelligence System - Etherscan Client

Purpose:
    Metadata layer - fetches contract ABIs, token info, verification status.
    Uses aggressive caching to achieve 99% hit rate.

    Called AFTER Alchemy validation when we need to decode transactions.

Inputs:
    - Etherscan API key
    - Contract addresses to query

Outputs:
    - Contract ABIs (for decoding)
    - Token metadata (name, symbol, decimals)
    - Verification status
    - Gas prices

Failure Modes:
    - Rate limit (5 RPS): Token bucket enforcement
    - API error: Returns None, logs warning
    - Unverified contract: Returns None for ABI
    - Cache corruption: Falls back to API

Caching Strategy:
    1. In-memory dict (fastest)
    2. Disk cache (persistent across restarts)
    3. API fetch (last resort)
    Target: 99% cache hit rate

Logging:
    - INFO: Cache hits/misses
    - WARNING: Rate limits, API errors
    - DEBUG: All API calls

Usage:
    client = EtherscanClient.from_config(config)

    # Get ABI (cached)
    abi = await client.get_abi("0x...")

    # Get token info
    info = await client.get_token_info("0x...")
"""

import json
import asyncio
import aiohttp
import hashlib
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

from .base_client import BaseClient, BackoffConfig

logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# =============================================================================

ETHERSCAN_API_URL = "https://api.etherscan.io/v2/api"

# Common token ABIs (pre-cached to avoid API calls)
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "totalSupply",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": False,
        "inputs": [
            {"name": "_to", "type": "address"},
            {"name": "_value", "type": "uint256"},
        ],
        "name": "transfer",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function",
    },
]


# =============================================================================
# DATA CLASSES
# =============================================================================


@dataclass
class ContractInfo:
    """Contract metadata."""

    address: str
    name: Optional[str] = None
    symbol: Optional[str] = None
    decimals: Optional[int] = None
    is_verified: bool = False
    is_proxy: bool = False
    implementation: Optional[str] = None
    abi: Optional[List] = None
    source_code: Optional[str] = None
    compiler_version: Optional[str] = None


@dataclass
class TokenInfo:
    """Token metadata."""

    address: str
    name: str
    symbol: str
    decimals: int
    total_supply: Optional[int] = None


@dataclass
class CacheStats:
    """Cache statistics."""

    memory_hits: int = 0
    disk_hits: int = 0
    api_fetches: int = 0

    @property
    def total_requests(self) -> int:
        return self.memory_hits + self.disk_hits + self.api_fetches

    @property
    def hit_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return (self.memory_hits + self.disk_hits) / self.total_requests


# =============================================================================
# ETHERSCAN CLIENT
# =============================================================================


class EtherscanClient(BaseClient):
    """
    Etherscan API client with aggressive caching.

    Features:
    - Two-tier cache (memory + disk)
    - 5 RPS rate limiting
    - Proxy contract detection
    - Token info queries
    """

    def __init__(
        self,
        api_key: str,
        cache_dir: Optional[Path] = None,
        config: Any = None,
    ):
        """
        Initialize Etherscan client.

        Args:
            api_key: Etherscan API key
            cache_dir: Directory for disk cache (None = no disk cache)
            config: Configuration object
        """
        super().__init__(
            api_name="etherscan",
            config=config,
            backoff=BackoffConfig(
                initial_delay=1.0,
                max_delay=30.0,
                max_attempts=5,
            ),
        )

        self.api_key = api_key
        self.cache_dir = cache_dir

        # HTTP session
        self._session: Optional[aiohttp.ClientSession] = None

        # In-memory cache
        self._abi_cache: Dict[str, List] = {}
        self._token_cache: Dict[str, TokenInfo] = {}
        self._contract_cache: Dict[str, ContractInfo] = {}

        # Cache stats
        self._cache_stats = CacheStats()

        # Ensure cache directory exists
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"EtherscanClient initialized, cache_dir={cache_dir}")

    @classmethod
    def from_config(cls, config) -> "EtherscanClient":
        """Create client from Config object."""
        cache_dir = (
            Path(config.system.data_dir) / "abi_cache"
            if hasattr(config, "system")
            else None
        )
        return cls(
            api_key=config.etherscan.api_key,
            cache_dir=cache_dir,
            config=config,
        )

    async def _connect(self) -> None:
        """Create HTTP session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
            )

        # Load disk cache into memory
        await self._load_disk_cache()

    async def _disconnect(self) -> None:
        """Close HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _load_disk_cache(self) -> None:
        """Load cached ABIs from disk into memory."""
        if not self.cache_dir:
            return

        try:
            abi_dir = self.cache_dir / "abis"
            if abi_dir.exists():
                count = 0
                for abi_file in abi_dir.glob("*.json"):
                    try:
                        address = abi_file.stem.lower()
                        with open(abi_file, "r") as f:
                            self._abi_cache[address] = json.load(f)
                        count += 1
                    except Exception:
                        pass

                logger.info(f"Loaded {count} ABIs from disk cache")
        except Exception as e:
            logger.warning(f"Failed to load disk cache: {e}")

    def _save_abi_to_disk(self, address: str, abi: List) -> None:
        """Save ABI to disk cache."""
        if not self.cache_dir:
            return

        try:
            abi_dir = self.cache_dir / "abis"
            abi_dir.mkdir(parents=True, exist_ok=True)

            abi_file = abi_dir / f"{address.lower()}.json"
            with open(abi_file, "w") as f:
                json.dump(abi, f)
        except Exception as e:
            logger.warning(f"Failed to save ABI to disk: {e}")

    async def _api_call(
        self,
        module: str,
        action: str,
        params: Dict = None,
    ) -> Optional[Any]:
        """
        Make Etherscan API call with rate limiting.

        Args:
            module: API module (contract, account, stats, etc.)
            action: API action
            params: Additional parameters

        Returns:
            API result or None on failure
        """
        if not self._acquire_rate_limit():
            logger.warning("Etherscan rate limit exhausted")
            return None

        all_params = {
            "chainid": 1,  # Ethereum Mainnet (V2 API requirement)
            "module": module,
            "action": action,
            "apikey": self.api_key,
            **(params or {}),
        }

        try:
            async with self._session.get(ETHERSCAN_API_URL, params=all_params) as resp:
                self._record_activity()

                if resp.status == 429:
                    logger.warning("Etherscan rate limited")
                    await asyncio.sleep(1)
                    return None

                if resp.status != 200:
                    logger.error(f"Etherscan HTTP error: {resp.status}")
                    return None

                data = await resp.json()

                # Check for API error
                if data.get("status") == "0":
                    message = data.get("message", "")
                    result = data.get("result", "")

                    # "Contract source code not verified" is not an error
                    if "not verified" in str(result).lower():
                        return None

                    # Rate limit message
                    if "rate limit" in message.lower():
                        logger.warning("Etherscan rate limit hit")
                        await asyncio.sleep(1)
                        return None

                    logger.debug(f"Etherscan API error: {message} - {result}")
                    return None

                return data.get("result")

        except asyncio.TimeoutError:
            self._record_error("Request timeout")
            return None
        except Exception as e:
            self._record_error(f"API call failed: {e}")
            return None

    # =========================================================================
    # ABI METHODS
    # =========================================================================

    async def get_abi(self, address: str) -> Optional[List]:
        """
        Get contract ABI with caching.

        Cache hierarchy:
        1. In-memory cache
        2. Disk cache
        3. API fetch

        Args:
            address: Contract address

        Returns:
            ABI as list or None if not verified
        """
        address = address.lower()

        # Check memory cache
        if address in self._abi_cache:
            self._cache_stats.memory_hits += 1
            return self._abi_cache[address]

        # Check disk cache (already loaded to memory in _connect)
        # This branch handles case where we check after disk load
        if address in self._abi_cache:
            self._cache_stats.disk_hits += 1
            return self._abi_cache[address]

        # Fetch from API
        self._cache_stats.api_fetches += 1

        result = await self._api_call("contract", "getabi", {"address": address})

        if result and isinstance(result, str):
            try:
                abi = json.loads(result)

                # Cache in memory
                self._abi_cache[address] = abi

                # Cache to disk
                self._save_abi_to_disk(address, abi)

                logger.info(
                    f"Fetched ABI for {address[:10]}...",
                    extra={
                        "address": address,
                        "functions": len(
                            [x for x in abi if x.get("type") == "function"]
                        ),
                    },
                )

                return abi
            except json.JSONDecodeError:
                logger.warning(f"Invalid ABI JSON for {address}")

        return None

    async def get_abi_batch(self, addresses: List[str]) -> Dict[str, Optional[List]]:
        """
        Get ABIs for multiple addresses.

        Args:
            addresses: List of contract addresses

        Returns:
            Dict of address -> ABI
        """
        results = {}

        for address in addresses:
            results[address] = await self.get_abi(address)

        return results

    # =========================================================================
    # CONTRACT INFO METHODS
    # =========================================================================

    async def get_contract_info(self, address: str) -> Optional[ContractInfo]:
        """
        Get comprehensive contract information.

        Args:
            address: Contract address

        Returns:
            ContractInfo or None
        """
        address = address.lower()

        # Check cache
        if address in self._contract_cache:
            return self._contract_cache[address]

        # Get source code (includes verification info)
        result = await self._api_call("contract", "getsourcecode", {"address": address})

        if not result or not isinstance(result, list) or len(result) == 0:
            return None

        data = result[0]

        # Parse contract info
        info = ContractInfo(
            address=address,
            name=data.get("ContractName") or None,
            is_verified=bool(data.get("SourceCode")),
            is_proxy=bool(data.get("Implementation")),
            implementation=data.get("Implementation") or None,
            source_code=data.get("SourceCode") or None,
            compiler_version=data.get("CompilerVersion") or None,
        )

        # Get ABI if verified
        if info.is_verified:
            abi_str = data.get("ABI")
            if abi_str and abi_str != "Contract source code not verified":
                try:
                    info.abi = json.loads(abi_str)
                    # Also cache the ABI
                    self._abi_cache[address] = info.abi
                    self._save_abi_to_disk(address, info.abi)
                except json.JSONDecodeError:
                    pass

        # Cache contract info
        self._contract_cache[address] = info

        return info

    async def is_contract_verified(self, address: str) -> bool:
        """Check if contract is verified."""
        info = await self.get_contract_info(address)
        return info.is_verified if info else False

    async def get_implementation_address(self, proxy_address: str) -> Optional[str]:
        """
        Get implementation address for a proxy contract.

        Args:
            proxy_address: Proxy contract address

        Returns:
            Implementation address or None
        """
        info = await self.get_contract_info(proxy_address)
        return info.implementation if info else None

    # =========================================================================
    # TOKEN METHODS
    # =========================================================================

    async def get_token_info(self, address: str) -> Optional[TokenInfo]:
        """
        Get ERC20 token information.

        Args:
            address: Token contract address

        Returns:
            TokenInfo or None
        """
        address = address.lower()

        # Check cache
        if address in self._token_cache:
            return self._token_cache[address]

        # Fetch token info
        result = await self._api_call(
            "token", "tokeninfo", {"contractaddress": address}
        )

        if result and isinstance(result, list) and len(result) > 0:
            data = result[0]

            info = TokenInfo(
                address=address,
                name=data.get("name", "Unknown"),
                symbol=data.get("symbol", "???"),
                decimals=int(data.get("decimals", 18)),
                total_supply=int(data.get("totalSupply", 0))
                if data.get("totalSupply")
                else None,
            )

            self._token_cache[address] = info
            return info

        return None

    async def get_token_supply(self, address: str) -> Optional[int]:
        """Get total supply of a token."""
        result = await self._api_call(
            "stats", "tokensupply", {"contractaddress": address}
        )
        if result:
            try:
                return int(result)
            except ValueError:
                pass
        return None

    # =========================================================================
    # GAS METHODS
    # =========================================================================

    async def get_gas_price(self) -> Optional[Dict[str, float]]:
        """
        Get current gas prices (Gwei).

        Returns:
            Dict with 'low', 'average', 'high' keys
        """
        result = await self._api_call("gastracker", "gasoracle")

        if result and isinstance(result, dict):
            try:
                return {
                    "low": float(result.get("SafeGasPrice", 0)),
                    "average": float(result.get("ProposeGasPrice", 0)),
                    "high": float(result.get("FastGasPrice", 0)),
                    "base_fee": float(result.get("suggestBaseFee", 0))
                    if result.get("suggestBaseFee")
                    else None,
                }
            except (ValueError, TypeError):
                return None

        return None

    # =========================================================================
    # ACCOUNT METHODS
    # =========================================================================

    async def get_transactions(
        self,
        address: str,
        start_block: int = 0,
        end_block: int = 99999999,
        page: int = 1,
        offset: int = 100,
    ) -> Optional[List[Dict]]:
        """
        Get transaction list for an address.

        Args:
            address: Account address
            start_block: Start block number
            end_block: End block number
            page: Page number
            offset: Results per page (max 10000)

        Returns:
            List of transaction dicts
        """
        result = await self._api_call(
            "account",
            "txlist",
            {
                "address": address,
                "startblock": start_block,
                "endblock": end_block,
                "page": page,
                "offset": offset,
                "sort": "desc",
            },
        )

        if result and isinstance(result, list):
            return result
        return None

    async def get_token_transfers(
        self,
        address: str,
        contract_address: Optional[str] = None,
        start_block: int = 0,
        end_block: int = 99999999,
        page: int = 1,
        offset: int = 100,
    ) -> Optional[List[Dict]]:
        """
        Get ERC20 token transfers for an address.

        Args:
            address: Account address
            contract_address: Optional specific token contract
            start_block: Start block number
            end_block: End block number
            page: Page number
            offset: Results per page

        Returns:
            List of transfer dicts
        """
        params = {
            "address": address,
            "startblock": start_block,
            "endblock": end_block,
            "page": page,
            "offset": offset,
            "sort": "desc",
        }

        if contract_address:
            params["contractaddress"] = contract_address

        result = await self._api_call("account", "tokentx", params)

        if result and isinstance(result, list):
            return result
        return None

    # =========================================================================
    # HEALTH & METRICS
    # =========================================================================

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            "memory_hits": self._cache_stats.memory_hits,
            "disk_hits": self._cache_stats.disk_hits,
            "api_fetches": self._cache_stats.api_fetches,
            "total_requests": self._cache_stats.total_requests,
            "hit_rate": f"{self._cache_stats.hit_rate * 100:.1f}%",
            "cached_abis": len(self._abi_cache),
            "cached_tokens": len(self._token_cache),
            "cached_contracts": len(self._contract_cache),
        }

    def get_metrics(self) -> Dict[str, Any]:
        """Get client metrics."""
        base_metrics = super().get_metrics()
        base_metrics.update(self.get_cache_stats())
        return base_metrics

    def health_check(self) -> Dict[str, Any]:
        """Get health status."""
        base_health = super().health_check()
        base_health.update(
            {
                "cache_hit_rate": f"{self._cache_stats.hit_rate * 100:.1f}%",
                "cached_abis": len(self._abi_cache),
            }
        )
        return base_health


# =============================================================================
# CLI TEST
# =============================================================================

if __name__ == "__main__":
    """Test Etherscan client."""
    import sys

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
    )

    async def test_client():
        # Load config
        sys.path.insert(0, str(__file__).rsplit("clients", 1)[0])
        from config import get_config

        try:
            config = get_config()
        except Exception as e:
            print(f"❌ Config error: {e}")
            return

        # Create client with temp cache
        cache_dir = Path("./test_cache")
        client = EtherscanClient(
            api_key=config.etherscan.api_key,
            cache_dir=cache_dir,
            config=config,
        )

        print("=" * 60)
        print("ETHERSCAN CLIENT TEST")
        print("=" * 60)

        # Connect
        print("\nConnecting...")
        await client.connect()
        print(f"✅ Connected: {client.is_connected}")

        # Test ABI fetch - USDT
        usdt = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
        print(f"\nFetching USDT ABI...")
        abi = await client.get_abi(usdt)
        if abi:
            functions = [x["name"] for x in abi if x.get("type") == "function"]
            print(f"✅ Got ABI with {len(functions)} functions")
            print(f"   Functions: {', '.join(functions[:5])}...")
        else:
            print("❌ Failed to get ABI")

        # Fetch same ABI again (should be cached)
        print("\nFetching USDT ABI again (should be cached)...")
        abi2 = await client.get_abi(usdt)
        print(f"✅ Got ABI (cached): {abi2 is not None}")

        # Get contract info
        print(f"\nGetting contract info for USDT...")
        info = await client.get_contract_info(usdt)
        if info:
            print(f"✅ Contract Info:")
            print(f"   Name: {info.name}")
            print(f"   Verified: {info.is_verified}")
            print(f"   Is Proxy: {info.is_proxy}")

        # Get gas prices
        print("\nGetting gas prices...")
        gas = await client.get_gas_price()
        if gas:
            print(f"✅ Gas Prices (Gwei):")
            print(f"   Low: {gas['low']}")
            print(f"   Average: {gas['average']}")
            print(f"   High: {gas['high']}")

        # Cache stats
        print("\nCache Stats:")
        stats = client.get_cache_stats()
        print(f"   Memory Hits: {stats['memory_hits']}")
        print(f"   API Fetches: {stats['api_fetches']}")
        print(f"   Hit Rate: {stats['hit_rate']}")
        print(f"   Cached ABIs: {stats['cached_abis']}")

        # Disconnect
        await client.disconnect()

        # Cleanup test cache
        import shutil

        if cache_dir.exists():
            shutil.rmtree(cache_dir)

        print("\n✅ Test complete!")

    asyncio.run(test_client())
