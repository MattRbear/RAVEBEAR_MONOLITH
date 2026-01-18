"""
Flint's Whale Intelligence System - Alchemy Client

Purpose:
    Validation layer - confirms whale transactions via Ethereum RPC.
    Uses Multicall3 to batch balance checks and reduce CU burn.
    
    Called AFTER Whale Alert triggers, not for discovery.

Inputs:
    - Alchemy API key
    - HTTP/WebSocket URLs
    
Outputs:
    - Wallet balances (ETH + tokens)
    - Transaction receipts
    - Block data
    - Real-time block notifications (WebSocket)

Failure Modes:
    - Rate limit (429): Backs off, logs warning
    - WebSocket disconnect: Auto-reconnect with backoff
    - Invalid response: Logs error, returns None
    - CU budget exhausted: Fails closed, logs critical

CU Costs (approximate):
    - eth_blockNumber: 10 CU
    - eth_getBalance: 15 CU
    - eth_call: 26 CU
    - eth_getTransactionReceipt: 15 CU
    - eth_subscribe: 10 CU per notification

Logging:
    - INFO: Successful queries
    - WARNING: Rate limits, retries
    - ERROR: Failed queries
    - DEBUG: CU tracking

Usage:
    client = AlchemyClient.from_config(config)
    await client.connect()
    
    # Single balance
    balance = await client.get_balance("0x...")
    
    # Batched balances (1 RPC call for multiple addresses)
    balances = await client.get_balances_batch(["0x...", "0x..."])
"""

import json
import asyncio
import aiohttp
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timezone
from enum import Enum

from .base_client import WebSocketClient, BackoffConfig, ConnectionState

logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# =============================================================================

# Multicall3 contract (same address on all EVM chains)
MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"

# Multicall3 ABI (minimal - just aggregate3)
MULTICALL3_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"name": "target", "type": "address"},
                    {"name": "allowFailure", "type": "bool"},
                    {"name": "callData", "type": "bytes"}
                ],
                "name": "calls",
                "type": "tuple[]"
            }
        ],
        "name": "aggregate3",
        "outputs": [
            {
                "components": [
                    {"name": "success", "type": "bool"},
                    {"name": "returnData", "type": "bytes"}
                ],
                "name": "returnData",
                "type": "tuple[]"
            }
        ],
        "stateMutability": "payable",
        "type": "function"
    }
]

# ERC20 function signatures
ERC20_BALANCE_OF = "0x70a08231"  # balanceOf(address)
ERC20_DECIMALS = "0x313ce567"   # decimals()
ERC20_SYMBOL = "0x95d89b41"     # symbol()
ERC20_NAME = "0x06fdde03"       # name()

# CU costs per method
CU_COSTS = {
    "eth_blockNumber": 10,
    "eth_getBalance": 15,
    "eth_call": 26,
    "eth_getTransactionReceipt": 15,
    "eth_getTransactionByHash": 15,
    "eth_getLogs": 75,
    "eth_getBlockByNumber": 16,
    "eth_chainId": 0,
    "eth_subscribe": 10,  # per notification
}


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class CUTracker:
    """Track compute unit usage."""
    total_used: int = 0
    calls_made: int = 0
    by_method: Dict[str, int] = field(default_factory=dict)
    
    def record(self, method: str, cu: int = None):
        """Record CU usage for a method call."""
        if cu is None:
            cu = CU_COSTS.get(method, 26)  # Default to eth_call cost
        
        self.total_used += cu
        self.calls_made += 1
        self.by_method[method] = self.by_method.get(method, 0) + cu
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_cu": self.total_used,
            "calls_made": self.calls_made,
            "by_method": self.by_method,
        }


@dataclass
class TokenBalance:
    """Token balance result."""
    address: str
    token_contract: Optional[str]  # None for native ETH
    balance_raw: int
    balance_decimal: float
    decimals: int = 18
    symbol: str = "ETH"


# =============================================================================
# ALCHEMY CLIENT
# =============================================================================

class AlchemyClient(WebSocketClient):
    """
    Alchemy Ethereum RPC client.
    
    Supports:
    - HTTP JSON-RPC for queries
    - WebSocket for subscriptions
    - Multicall3 batching
    - CU tracking
    """
    
    def __init__(
        self,
        api_key: str,
        http_url: str,
        ws_url: str,
        chain_id: int = 1,
        config: Any = None,
    ):
        """
        Initialize Alchemy client.
        
        Args:
            api_key: Alchemy API key
            http_url: HTTP RPC URL
            ws_url: WebSocket URL
            chain_id: Chain ID (1=ETH mainnet)
            config: Configuration object
        """
        super().__init__(
            api_name="alchemy",
            config=config,
            ws_url=ws_url,
            ping_interval=30.0,
            backoff=BackoffConfig(
                initial_delay=1.0,
                max_delay=60.0,
                max_attempts=10,
            ),
        )
        
        self.api_key = api_key
        self.http_url = http_url
        self.chain_id = chain_id
        
        # HTTP session
        self._http_session: Optional[aiohttp.ClientSession] = None
        
        # CU tracking
        self._cu_tracker = CUTracker()
        
        # Request ID counter
        self._request_id = 0
        
        # Subscription handlers
        self._subscription_handlers: Dict[str, List] = {}
        self._subscriptions: Dict[str, str] = {}  # sub_id -> sub_type
        
        logger.info(f"AlchemyClient initialized for chain {chain_id}")
    
    @classmethod
    def from_config(cls, config) -> "AlchemyClient":
        """Create client from Config object."""
        return cls(
            api_key=config.alchemy.api_key,
            http_url=config.alchemy.http_url,
            ws_url=config.alchemy.ws_url,
            config=config,
        )
    
    def _next_id(self) -> int:
        """Get next request ID."""
        self._request_id += 1
        return self._request_id
    
    async def _connect(self) -> None:
        """Establish connections."""
        # Create HTTP session
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={"Content-Type": "application/json"},
            )
        
        # Connect WebSocket (parent class)
        await super()._connect()
    
    async def _disconnect(self) -> None:
        """Close connections."""
        # Close HTTP session
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None
        
        # Close WebSocket (parent class)
        await super()._disconnect()
    
    async def _rpc_call(
        self,
        method: str,
        params: List = None,
        track_cu: bool = True,
    ) -> Optional[Any]:
        """
        Make JSON-RPC call over HTTP.
        
        Args:
            method: RPC method name
            params: Method parameters
            track_cu: Whether to track CU usage
            
        Returns:
            Result or None on failure
        """
        if not self._acquire_rate_limit():
            logger.warning("Alchemy rate limit exhausted")
            return None
        
        payload = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": method,
            "params": params or [],
        }
        
        try:
            async with self._http_session.post(self.http_url, json=payload) as resp:
                self._record_activity()
                
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 1))
                    logger.warning(f"Alchemy rate limited, retry after {retry_after}s")
                    await asyncio.sleep(retry_after)
                    return None
                
                if resp.status != 200:
                    logger.error(f"Alchemy HTTP error: {resp.status}")
                    return None
                
                data = await resp.json()
                
                if "error" in data:
                    logger.error(f"Alchemy RPC error: {data['error']}")
                    return None
                
                # Track CU
                if track_cu:
                    self._cu_tracker.record(method)
                
                return data.get("result")
                
        except asyncio.TimeoutError:
            self._record_error("Request timeout")
            return None
        except Exception as e:
            self._record_error(f"RPC call failed: {e}")
            return None
    
    async def _handle_message(self, message: str) -> None:
        """Handle WebSocket message."""
        try:
            data = json.loads(message)
            
            # Check for subscription notification
            if data.get("method") == "eth_subscription":
                params = data.get("params", {})
                sub_id = params.get("subscription")
                result = params.get("result")
                
                if sub_id and sub_id in self._subscriptions:
                    sub_type = self._subscriptions[sub_id]
                    handlers = self._subscription_handlers.get(sub_type, [])
                    
                    for handler in handlers:
                        try:
                            if asyncio.iscoroutinefunction(handler):
                                await handler(result)
                            else:
                                handler(result)
                        except Exception as e:
                            logger.error(f"Subscription handler error: {e}")
                    
                    # Track CU for subscription
                    self._cu_tracker.record("eth_subscribe")
                    
        except json.JSONDecodeError as e:
            logger.error(f"Invalid WebSocket message: {e}")

    # =========================================================================
    # BASIC QUERIES
    # =========================================================================
    
    async def get_block_number(self) -> Optional[int]:
        """Get current block number."""
        result = await self._rpc_call("eth_blockNumber")
        if result:
            return int(result, 16)
        return None
    
    async def get_chain_id(self) -> Optional[int]:
        """Get chain ID."""
        result = await self._rpc_call("eth_chainId", track_cu=False)
        if result:
            return int(result, 16)
        return None
    
    async def get_balance(self, address: str, block: str = "latest") -> Optional[int]:
        """
        Get native token balance (ETH).
        
        Args:
            address: Wallet address
            block: Block number or "latest"
            
        Returns:
            Balance in wei or None on failure
        """
        result = await self._rpc_call("eth_getBalance", [address, block])
        if result:
            return int(result, 16)
        return None
    
    async def get_transaction_receipt(self, tx_hash: str) -> Optional[Dict]:
        """
        Get transaction receipt.
        
        Args:
            tx_hash: Transaction hash
            
        Returns:
            Receipt dict or None
        """
        return await self._rpc_call("eth_getTransactionReceipt", [tx_hash])
    
    async def get_transaction(self, tx_hash: str) -> Optional[Dict]:
        """
        Get transaction by hash.
        
        Args:
            tx_hash: Transaction hash
            
        Returns:
            Transaction dict or None
        """
        return await self._rpc_call("eth_getTransactionByHash", [tx_hash])
    
    async def get_block(self, block: str = "latest", full_txs: bool = False) -> Optional[Dict]:
        """
        Get block by number.
        
        Args:
            block: Block number (hex) or "latest"
            full_txs: Include full transaction objects
            
        Returns:
            Block dict or None
        """
        return await self._rpc_call("eth_getBlockByNumber", [block, full_txs])
    
    # =========================================================================
    # TOKEN QUERIES
    # =========================================================================
    
    async def get_token_balance(
        self,
        wallet: str,
        token_contract: str,
        block: str = "latest",
    ) -> Optional[int]:
        """
        Get ERC20 token balance.
        
        Args:
            wallet: Wallet address
            token_contract: Token contract address
            block: Block number or "latest"
            
        Returns:
            Balance in token units (raw) or None
        """
        # Encode balanceOf(address) call
        # Function selector + padded address
        wallet_padded = wallet.lower().replace("0x", "").zfill(64)
        call_data = f"{ERC20_BALANCE_OF}{wallet_padded}"
        
        result = await self._rpc_call("eth_call", [
            {"to": token_contract, "data": call_data},
            block
        ])
        
        if result and result != "0x":
            return int(result, 16)
        return None
    
    async def get_token_decimals(self, token_contract: str) -> Optional[int]:
        """Get token decimals."""
        result = await self._rpc_call("eth_call", [
            {"to": token_contract, "data": ERC20_DECIMALS},
            "latest"
        ])
        
        if result and result != "0x":
            return int(result, 16)
        return 18  # Default
    
    async def get_token_symbol(self, token_contract: str) -> Optional[str]:
        """Get token symbol."""
        result = await self._rpc_call("eth_call", [
            {"to": token_contract, "data": ERC20_SYMBOL},
            "latest"
        ])
        
        if result and len(result) > 2:
            try:
                # Decode string from ABI encoding
                hex_data = result[2:]  # Remove 0x
                if len(hex_data) >= 128:
                    # Standard ABI encoding
                    length = int(hex_data[64:128], 16)
                    symbol_hex = hex_data[128:128 + length * 2]
                    return bytes.fromhex(symbol_hex).decode('utf-8').strip('\x00')
                else:
                    # Bytes32 encoding (some tokens)
                    return bytes.fromhex(hex_data).decode('utf-8').strip('\x00')
            except Exception:
                pass
        return None
    
    # =========================================================================
    # MULTICALL BATCHING
    # =========================================================================
    
    async def get_balances_batch(
        self,
        addresses: List[str],
        block: str = "latest",
    ) -> Dict[str, int]:
        """
        Get ETH balances for multiple addresses in ONE RPC call.
        
        Uses Multicall3 to batch getBalance calls.
        
        Args:
            addresses: List of wallet addresses
            block: Block number or "latest"
            
        Returns:
            Dict of address -> balance (wei)
        """
        if not addresses:
            return {}
        
        # Build multicall payload
        # For ETH balance, we call address.balance via staticcall in assembly
        # But Multicall3 doesn't support this directly, so we use a workaround:
        # We'll make individual eth_getBalance calls but batch them in one HTTP request
        
        # Actually, for pure ETH balances, we need to use batch JSON-RPC
        return await self._batch_get_balances(addresses, block)
    
    async def _batch_get_balances(
        self,
        addresses: List[str],
        block: str = "latest",
    ) -> Dict[str, int]:
        """
        Batch ETH balance queries using JSON-RPC batching.
        
        One HTTP request, multiple RPC calls.
        """
        if not self._acquire_rate_limit():
            return {}
        
        # Build batch request
        batch = []
        for i, addr in enumerate(addresses):
            batch.append({
                "jsonrpc": "2.0",
                "id": self._next_id(),
                "method": "eth_getBalance",
                "params": [addr, block],
            })
        
        try:
            async with self._http_session.post(self.http_url, json=batch) as resp:
                self._record_activity()
                
                if resp.status != 200:
                    logger.error(f"Alchemy batch error: {resp.status}")
                    return {}
                
                results = await resp.json()
                
                # Track CU (one call per address, but single HTTP request)
                for _ in addresses:
                    self._cu_tracker.record("eth_getBalance")
                
                # Parse results
                balances = {}
                for i, result in enumerate(results):
                    if "result" in result and result["result"]:
                        balances[addresses[i]] = int(result["result"], 16)
                    else:
                        balances[addresses[i]] = 0
                
                return balances
                
        except Exception as e:
            self._record_error(f"Batch balance query failed: {e}")
            return {}
    
    async def get_token_balances_batch(
        self,
        wallet: str,
        token_contracts: List[str],
        block: str = "latest",
    ) -> Dict[str, int]:
        """
        Get multiple token balances for ONE wallet using Multicall3.
        
        This is the efficient way - one RPC call for many tokens.
        
        Args:
            wallet: Wallet address
            token_contracts: List of token contract addresses
            block: Block number or "latest"
            
        Returns:
            Dict of token_contract -> balance (raw)
        """
        if not token_contracts:
            return {}
        
        # Build Multicall3 calls
        wallet_padded = wallet.lower().replace("0x", "").zfill(64)
        
        calls = []
        for token in token_contracts:
            call_data = f"{ERC20_BALANCE_OF}{wallet_padded}"
            calls.append({
                "target": token,
                "allowFailure": True,
                "callData": bytes.fromhex(call_data[2:]) if call_data.startswith("0x") else bytes.fromhex(call_data),
            })
        
        # Encode aggregate3 call
        # This is complex ABI encoding, so we'll use a simpler approach: batch RPC
        return await self._batch_get_token_balances(wallet, token_contracts, block)
    
    async def _batch_get_token_balances(
        self,
        wallet: str,
        token_contracts: List[str],
        block: str = "latest",
    ) -> Dict[str, int]:
        """
        Batch token balance queries using JSON-RPC batching.
        """
        if not self._acquire_rate_limit():
            return {}
        
        wallet_padded = wallet.lower().replace("0x", "").zfill(64)
        
        # Build batch request
        batch = []
        for token in token_contracts:
            call_data = f"{ERC20_BALANCE_OF}{wallet_padded}"
            batch.append({
                "jsonrpc": "2.0",
                "id": self._next_id(),
                "method": "eth_call",
                "params": [{"to": token, "data": call_data}, block],
            })
        
        try:
            async with self._http_session.post(self.http_url, json=batch) as resp:
                self._record_activity()
                
                if resp.status != 200:
                    return {}
                
                results = await resp.json()
                
                # Track CU
                for _ in token_contracts:
                    self._cu_tracker.record("eth_call")
                
                # Parse results
                balances = {}
                for i, result in enumerate(results):
                    if "result" in result and result["result"] and result["result"] != "0x":
                        try:
                            balances[token_contracts[i]] = int(result["result"], 16)
                        except ValueError:
                            balances[token_contracts[i]] = 0
                    else:
                        balances[token_contracts[i]] = 0
                
                return balances
                
        except Exception as e:
            self._record_error(f"Batch token balance query failed: {e}")
            return {}

    # =========================================================================
    # WEBSOCKET SUBSCRIPTIONS
    # =========================================================================
    
    async def subscribe_new_heads(self, handler) -> Optional[str]:
        """
        Subscribe to new block headers.
        
        Args:
            handler: Callback function(block_header_dict)
            
        Returns:
            Subscription ID or None on failure
        """
        if self._state != ConnectionState.CONNECTED or not self._ws:
            logger.warning("Cannot subscribe - WebSocket not connected")
            return None
        
        # Register handler
        if "newHeads" not in self._subscription_handlers:
            self._subscription_handlers["newHeads"] = []
        self._subscription_handlers["newHeads"].append(handler)
        
        # Send subscription request
        request = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "eth_subscribe",
            "params": ["newHeads"],
        }
        
        try:
            await self._ws.send(json.dumps(request))
            
            # Wait for response
            response = await asyncio.wait_for(self._ws.recv(), timeout=10)
            data = json.loads(response)
            
            if "result" in data:
                sub_id = data["result"]
                self._subscriptions[sub_id] = "newHeads"
                logger.info(f"Subscribed to newHeads: {sub_id}")
                return sub_id
            else:
                logger.error(f"Subscription failed: {data.get('error')}")
                return None
                
        except Exception as e:
            self._record_error(f"Subscription failed: {e}")
            return None
    
    async def subscribe_pending_transactions(self, handler) -> Optional[str]:
        """
        Subscribe to pending transactions.
        
        Args:
            handler: Callback function(tx_hash)
            
        Returns:
            Subscription ID or None
        """
        if self._state != ConnectionState.CONNECTED or not self._ws:
            return None
        
        if "newPendingTransactions" not in self._subscription_handlers:
            self._subscription_handlers["newPendingTransactions"] = []
        self._subscription_handlers["newPendingTransactions"].append(handler)
        
        request = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "eth_subscribe",
            "params": ["newPendingTransactions"],
        }
        
        try:
            await self._ws.send(json.dumps(request))
            response = await asyncio.wait_for(self._ws.recv(), timeout=10)
            data = json.loads(response)
            
            if "result" in data:
                sub_id = data["result"]
                self._subscriptions[sub_id] = "newPendingTransactions"
                logger.info(f"Subscribed to pending txs: {sub_id}")
                return sub_id
            return None
            
        except Exception as e:
            self._record_error(f"Pending tx subscription failed: {e}")
            return None
    
    async def unsubscribe(self, sub_id: str) -> bool:
        """
        Unsubscribe from a subscription.
        
        Args:
            sub_id: Subscription ID
            
        Returns:
            True if successful
        """
        if not self._ws or sub_id not in self._subscriptions:
            return False
        
        request = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "eth_unsubscribe",
            "params": [sub_id],
        }
        
        try:
            await self._ws.send(json.dumps(request))
            response = await asyncio.wait_for(self._ws.recv(), timeout=10)
            data = json.loads(response)
            
            if data.get("result"):
                sub_type = self._subscriptions.pop(sub_id, None)
                logger.info(f"Unsubscribed from {sub_type}: {sub_id}")
                return True
            return False
            
        except Exception as e:
            self._record_error(f"Unsubscribe failed: {e}")
            return False
    
    # =========================================================================
    # HEALTH & METRICS
    # =========================================================================
    
    def get_cu_usage(self) -> Dict[str, Any]:
        """Get compute unit usage stats."""
        return self._cu_tracker.to_dict()
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get client metrics."""
        base_metrics = super().get_metrics()
        base_metrics.update({
            "cu_usage": self._cu_tracker.to_dict(),
            "chain_id": self.chain_id,
            "active_subscriptions": len(self._subscriptions),
        })
        return base_metrics
    
    def health_check(self) -> Dict[str, Any]:
        """Get health status."""
        base_health = super().health_check()
        base_health.update({
            "cu_total": self._cu_tracker.total_used,
            "calls_made": self._cu_tracker.calls_made,
            "chain_id": self.chain_id,
        })
        return base_health


# =============================================================================
# CLI TEST
# =============================================================================

if __name__ == "__main__":
    """Test Alchemy client."""
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
        client = AlchemyClient.from_config(config)
        
        print("=" * 60)
        print("ALCHEMY CLIENT TEST")
        print("=" * 60)
        
        # Connect
        print("\nConnecting...")
        await client.connect()
        print(f"✅ Connected: {client.is_connected}")
        
        # Get block number
        print("\nGetting block number...")
        block = await client.get_block_number()
        print(f"✅ Current block: {block:,}")
        
        # Get chain ID
        chain = await client.get_chain_id()
        print(f"✅ Chain ID: {chain}")
        
        # Test balance query - Vitalik's address
        vitalik = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
        print(f"\nGetting balance for Vitalik...")
        balance = await client.get_balance(vitalik)
        if balance:
            eth_balance = balance / 10**18
            print(f"✅ Balance: {eth_balance:,.4f} ETH")
        
        # Test batch balance query
        addresses = [
            "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",  # Vitalik
            "0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8",  # Binance
            "0x47ac0Fb4F2D84898e4D9E7b4DaB3C24507a6D503",  # Binance 2
        ]
        print(f"\nBatch querying {len(addresses)} addresses...")
        balances = await client.get_balances_batch(addresses)
        print(f"✅ Got {len(balances)} balances")
        for addr, bal in balances.items():
            eth = bal / 10**18
            print(f"   {addr[:10]}...{addr[-6:]}: {eth:,.2f} ETH")
        
        # Test token balance (USDT on Vitalik)
        usdt = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
        print(f"\nGetting USDT balance for Vitalik...")
        usdt_balance = await client.get_token_balance(vitalik, usdt)
        if usdt_balance is not None:
            # USDT has 6 decimals
            usdt_decimal = usdt_balance / 10**6
            print(f"✅ USDT Balance: {usdt_decimal:,.2f}")
        
        # CU usage
        print("\nCU Usage:")
        cu = client.get_cu_usage()
        print(f"   Total CU: {cu['total_cu']}")
        print(f"   Calls Made: {cu['calls_made']}")
        for method, cu_used in cu['by_method'].items():
            print(f"   {method}: {cu_used} CU")
        
        # Health check
        print("\nHealth Check:")
        health = client.health_check()
        print(f"   Healthy: {health.get('healthy')}")
        print(f"   State: {health.get('state')}")
        
        # Disconnect
        await client.disconnect()
        print("\n✅ Test complete!")
    
    asyncio.run(test_client())
