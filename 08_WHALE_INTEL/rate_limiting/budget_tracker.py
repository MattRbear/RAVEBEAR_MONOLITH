"""
Flint's Whale Intelligence System - API Budget Tracker

Purpose:
    Centralized management of all API rate limits and budgets.
    Provides unified interface for acquiring API credits.
    Tracks usage across all providers.

Inputs:
    - Config object with rate limit settings
    
Outputs:
    - can_call(api): Check if API call is allowed
    - acquire(api): Acquire token for API call
    - get_all_status(): Get status of all buckets

Failure Modes:
    - Unknown API: Raises KeyError
    - All budgets exhausted: Logs critical, returns False

Logging:
    - INFO: Periodic status report
    - WARNING: Any API below 20% budget
    - CRITICAL: Any API exhausted

Usage:
    tracker = BudgetTracker.from_config(config)
    
    if tracker.acquire("etherscan"):
        make_etherscan_call()
"""

import logging
import threading
from dataclasses import dataclass
from typing import Dict, Optional, Any, List
from datetime import datetime, timezone

from .token_bucket import TokenBucket, BudgetPeriod, BucketStatus

logger = logging.getLogger(__name__)


# =============================================================================
# API DEFINITIONS
# =============================================================================

@dataclass
class APILimits:
    """Rate limit configuration for an API."""
    name: str
    requests_per_second: float
    burst_capacity: int
    daily_limit: Optional[int] = None
    monthly_limit: Optional[int] = None
    compute_units_per_call: int = 1  # For APIs that use CU (Alchemy, Moralis)


# Default API configurations (2025/2026 free tiers)
DEFAULT_API_LIMITS: Dict[str, APILimits] = {
    "whale_alert": APILimits(
        name="whale_alert",
        requests_per_second=30.0,  # Premium tier
        burst_capacity=30,
    ),
    "alchemy": APILimits(
        name="alchemy",
        requests_per_second=12.0,  # 330 CU/sec / ~26 CU per eth_call
        burst_capacity=20,
        monthly_limit=30_000_000,  # 30M CU/month
        compute_units_per_call=26,  # Average for eth_call
    ),
    "etherscan": APILimits(
        name="etherscan",
        requests_per_second=5.0,
        burst_capacity=5,
        daily_limit=100_000,
    ),
    "moralis": APILimits(
        name="moralis",
        requests_per_second=2.0,
        burst_capacity=5,
        daily_limit=400,  # 40k CU / 100 CU per call
        compute_units_per_call=100,
    ),
    "dune": APILimits(
        name="dune",
        requests_per_second=0.5,  # Conservative
        burst_capacity=3,
        monthly_limit=250,  # 2500 credits / ~10 per query
        compute_units_per_call=10,
    ),
    "token_metrics": APILimits(
        name="token_metrics",
        requests_per_second=0.08,  # 5 per minute
        burst_capacity=5,
        monthly_limit=500,
    ),
    "coingecko": APILimits(
        name="coingecko",
        requests_per_second=0.5,  # 30/min = 0.5/sec
        burst_capacity=10,
    ),
    "discord": APILimits(
        name="discord",
        requests_per_second=1.0,  # Webhook rate limit
        burst_capacity=5,
    ),
}


# =============================================================================
# BUDGET TRACKER
# =============================================================================

class BudgetTracker:
    """
    Centralized API budget management.
    
    Thread-safe tracker for all API rate limits and budgets.
    Provides health checks, status reporting, and fail-closed behavior.
    """
    
    def __init__(self, api_limits: Optional[Dict[str, APILimits]] = None):
        """
        Initialize budget tracker.
        
        Args:
            api_limits: Custom API limit configurations (uses defaults if None)
        """
        self._limits = api_limits or DEFAULT_API_LIMITS
        self._buckets: Dict[str, TokenBucket] = {}
        self._daily_buckets: Dict[str, TokenBucket] = {}
        self._monthly_buckets: Dict[str, TokenBucket] = {}
        self._lock = threading.RLock()
        
        # Initialize buckets for each API
        for name, limits in self._limits.items():
            # Per-second rate bucket
            self._buckets[name] = TokenBucket(
                name=f"{name}_rps",
                capacity=limits.burst_capacity,
                refill_rate=limits.requests_per_second,
            )
            
            # Daily budget bucket (if applicable)
            if limits.daily_limit:
                self._daily_buckets[name] = TokenBucket(
                    name=f"{name}_daily",
                    capacity=limits.daily_limit,
                    refill_rate=limits.daily_limit / 86400,  # Spread over day
                    budget_limit=limits.daily_limit,
                    budget_period=BudgetPeriod.DAY,
                )
            
            # Monthly budget bucket (if applicable)
            if limits.monthly_limit:
                self._monthly_buckets[name] = TokenBucket(
                    name=f"{name}_monthly",
                    capacity=limits.monthly_limit,
                    refill_rate=limits.monthly_limit / (30 * 86400),  # Spread over month
                    budget_limit=limits.monthly_limit,
                    budget_period=BudgetPeriod.MONTH,
                )
        
        logger.info(f"BudgetTracker initialized with {len(self._limits)} APIs")
    
    @classmethod
    def from_config(cls, config) -> "BudgetTracker":
        """
        Create BudgetTracker from Config object.
        
        Args:
            config: Config object with API settings
            
        Returns:
            Configured BudgetTracker instance
        """
        # Override defaults with config values
        limits = DEFAULT_API_LIMITS.copy()
        
        # Update Alchemy limits from config
        if hasattr(config, 'alchemy'):
            limits["alchemy"] = APILimits(
                name="alchemy",
                requests_per_second=config.alchemy.cu_per_sec / 26,
                burst_capacity=20,
                monthly_limit=config.alchemy.cu_monthly,
                compute_units_per_call=26,
            )
        
        # Update Etherscan limits from config
        if hasattr(config, 'etherscan'):
            limits["etherscan"] = APILimits(
                name="etherscan",
                requests_per_second=config.etherscan.rps,
                burst_capacity=config.etherscan.rps,
                daily_limit=config.etherscan.daily_limit,
            )
        
        # Update Moralis limits from config
        if hasattr(config, 'moralis'):
            daily_calls = config.moralis.cu_daily // 100  # 100 CU per call
            limits["moralis"] = APILimits(
                name="moralis",
                requests_per_second=2.0,
                burst_capacity=5,
                daily_limit=daily_calls,
                compute_units_per_call=100,
            )
        
        # Update Dune limits from config
        if hasattr(config, 'dune'):
            monthly_queries = config.dune.credits_monthly // 10  # ~10 credits per query
            limits["dune"] = APILimits(
                name="dune",
                requests_per_second=0.5,
                burst_capacity=3,
                monthly_limit=monthly_queries,
                compute_units_per_call=10,
            )
        
        # Update Token Metrics limits from config
        if hasattr(config, 'token_metrics'):
            limits["token_metrics"] = APILimits(
                name="token_metrics",
                requests_per_second=0.08,
                burst_capacity=5,
                monthly_limit=config.token_metrics.monthly_limit,
            )
        
        return cls(api_limits=limits)
    
    def can_call(self, api: str) -> bool:
        """
        Check if API call is allowed without consuming a token.
        
        Args:
            api: API name (e.g., "etherscan", "moralis")
            
        Returns:
            True if call would be allowed
        """
        with self._lock:
            if api not in self._buckets:
                logger.error(f"Unknown API: {api}")
                return False
            
            status = self._buckets[api].get_status()
            if status.tokens < 1:
                return False
            
            # Check daily budget
            if api in self._daily_buckets:
                daily_status = self._daily_buckets[api].get_status()
                if daily_status.is_exhausted:
                    return False
            
            # Check monthly budget
            if api in self._monthly_buckets:
                monthly_status = self._monthly_buckets[api].get_status()
                if monthly_status.is_exhausted:
                    return False
            
            return True
    
    def acquire(self, api: str, tokens: float = 1.0, blocking: bool = False) -> bool:
        """
        Acquire token for API call.
        
        Args:
            api: API name
            tokens: Number of tokens (default 1)
            blocking: Wait for token if not available
            
        Returns:
            True if token acquired, False otherwise
        """
        with self._lock:
            if api not in self._buckets:
                logger.error(f"Unknown API: {api}", extra={"api": api})
                return False
            
            # Check monthly budget first (fail-closed)
            if api in self._monthly_buckets:
                if not self._monthly_buckets[api].acquire(tokens, blocking=False):
                    logger.warning(f"API '{api}' monthly budget exhausted", extra={
                        "api": api,
                        "type": "monthly_exhausted",
                    })
                    return False
            
            # Check daily budget (fail-closed)
            if api in self._daily_buckets:
                if not self._daily_buckets[api].acquire(tokens, blocking=False):
                    logger.warning(f"API '{api}' daily budget exhausted", extra={
                        "api": api,
                        "type": "daily_exhausted",
                    })
                    # Refund monthly if we already deducted
                    if api in self._monthly_buckets:
                        self._monthly_buckets[api]._budget_used -= 1
                    return False
            
            # Check rate limit
            if not self._buckets[api].acquire(tokens, blocking=blocking):
                logger.debug(f"API '{api}' rate limited", extra={
                    "api": api,
                    "type": "rate_limited",
                })
                # Refund budgets if we already deducted
                if api in self._daily_buckets:
                    self._daily_buckets[api]._budget_used -= 1
                if api in self._monthly_buckets:
                    self._monthly_buckets[api]._budget_used -= 1
                return False
            
            return True
    
    def wait(self, api: str, timeout: float = 30.0) -> bool:
        """
        Wait for API token to become available.
        
        Args:
            api: API name
            timeout: Max seconds to wait
            
        Returns:
            True if token acquired, False if timeout or budget exhausted
        """
        return self.acquire(api, blocking=True)
    
    def get_status(self, api: str) -> Dict[str, Any]:
        """Get status for a specific API."""
        with self._lock:
            if api not in self._buckets:
                return {"error": f"Unknown API: {api}"}
            
            result = {
                "api": api,
                "rate_limit": self._buckets[api].get_metrics(),
            }
            
            if api in self._daily_buckets:
                result["daily_budget"] = self._daily_buckets[api].get_metrics()
            
            if api in self._monthly_buckets:
                result["monthly_budget"] = self._monthly_buckets[api].get_metrics()
            
            return result
    
    def get_all_status(self) -> Dict[str, Any]:
        """Get status for all APIs."""
        with self._lock:
            return {api: self.get_status(api) for api in self._buckets}
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on all API budgets.
        
        Returns:
            Dict with overall health status and per-API details
        """
        with self._lock:
            healthy = True
            warnings = []
            critical = []
            
            for api in self._buckets:
                # Check daily budget
                if api in self._daily_buckets:
                    status = self._daily_buckets[api].get_status()
                    if status.is_exhausted:
                        healthy = False
                        critical.append(f"{api}: daily budget exhausted")
                    elif status.utilization_pct >= 80:
                        warnings.append(f"{api}: daily budget at {status.utilization_pct:.0f}%")
                
                # Check monthly budget
                if api in self._monthly_buckets:
                    status = self._monthly_buckets[api].get_status()
                    if status.is_exhausted:
                        healthy = False
                        critical.append(f"{api}: monthly budget exhausted")
                    elif status.utilization_pct >= 80:
                        warnings.append(f"{api}: monthly budget at {status.utilization_pct:.0f}%")
            
            return {
                "healthy": healthy,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "warnings": warnings,
                "critical": critical,
                "apis": list(self._buckets.keys()),
            }
    
    def log_status_report(self) -> None:
        """Log current status of all API budgets."""
        health = self.health_check()
        
        if not health["healthy"]:
            logger.critical("API Budget Health Check FAILED", extra=health)
        elif health["warnings"]:
            logger.warning("API Budget Health Check has warnings", extra=health)
        else:
            logger.info("API Budget Health Check passed", extra=health)


# =============================================================================
# MODULE-LEVEL SINGLETON
# =============================================================================

_tracker: Optional[BudgetTracker] = None


def get_budget_tracker() -> BudgetTracker:
    """Get the global budget tracker singleton."""
    global _tracker
    if _tracker is None:
        _tracker = BudgetTracker()
    return _tracker


def init_budget_tracker(config) -> BudgetTracker:
    """Initialize global budget tracker from config."""
    global _tracker
    _tracker = BudgetTracker.from_config(config)
    return _tracker
