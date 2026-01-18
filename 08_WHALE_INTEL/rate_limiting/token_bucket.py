"""
Flint's Whale Intelligence System - Token Bucket Rate Limiter

Purpose:
    Implement token bucket algorithm for API rate limiting.
    Supports per-second, per-minute, per-day, and per-month budgets.
    Fail-closed: blocks requests when budget exhausted.

Inputs:
    - capacity: Maximum tokens in bucket
    - refill_rate: Tokens added per second
    - budget_limit: Optional hard limit (daily/monthly)

Outputs:
    - acquire(): Returns True if token available, False otherwise
    - wait(): Blocks until token available
    - get_status(): Returns current bucket state

Failure Modes:
    - Budget exhausted: Returns False, logs warning
    - Negative capacity: Raises ValueError
    - Clock drift: Handles gracefully with monotonic time

Logging:
    - WARNING: Budget below 10%
    - ERROR: Budget exhausted
    - DEBUG: Token acquisition details

Usage:
    limiter = TokenBucket(capacity=5, refill_rate=5.0)  # 5 RPS
    if limiter.acquire():
        make_api_call()
    else:
        handle_rate_limit()
"""

import time
import threading
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# EXCEPTIONS
# =============================================================================

class RateLimitExhausted(Exception):
    """Raised when rate limit budget is exhausted."""
    pass


class BudgetExhausted(Exception):
    """Raised when periodic budget (daily/monthly) is exhausted."""
    pass


# =============================================================================
# ENUMS
# =============================================================================

class BudgetPeriod(Enum):
    """Budget reset periods."""
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    MONTH = "month"


# =============================================================================
# TOKEN BUCKET IMPLEMENTATION
# =============================================================================

@dataclass
class BucketStatus:
    """Current state of a token bucket."""
    name: str
    tokens: float
    capacity: float
    refill_rate: float
    budget_used: int
    budget_limit: Optional[int]
    budget_period: Optional[BudgetPeriod]
    budget_resets_at: Optional[datetime]
    is_exhausted: bool
    utilization_pct: float


class TokenBucket:
    """
    Thread-safe token bucket rate limiter.
    
    Implements the token bucket algorithm with optional periodic budgets.
    Tokens are added at a constant rate up to capacity.
    Requests consume tokens; if no tokens available, request is denied.
    """
    
    def __init__(
        self,
        name: str,
        capacity: float,
        refill_rate: float,
        budget_limit: Optional[int] = None,
        budget_period: Optional[BudgetPeriod] = None,
    ):
        """
        Initialize token bucket.
        
        Args:
            name: Identifier for this bucket (for logging)
            capacity: Maximum tokens in bucket
            refill_rate: Tokens added per second
            budget_limit: Optional hard limit per period
            budget_period: Period for budget reset (day, month, etc.)
        """
        if capacity <= 0:
            raise ValueError(f"Capacity must be positive, got {capacity}")
        if refill_rate <= 0:
            raise ValueError(f"Refill rate must be positive, got {refill_rate}")
        
        self.name = name
        self.capacity = float(capacity)
        self.refill_rate = float(refill_rate)
        self.budget_limit = budget_limit
        self.budget_period = budget_period
        
        # State
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._budget_used = 0
        self._budget_reset_time = self._calculate_next_reset()
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Metrics
        self._total_acquired = 0
        self._total_denied = 0
        self._total_waited = 0
        
        logger.debug(f"TokenBucket '{name}' initialized: capacity={capacity}, rate={refill_rate}/s")
    
    def _calculate_next_reset(self) -> Optional[datetime]:
        """Calculate next budget reset time."""
        if not self.budget_period:
            return None
        
        now = datetime.now(timezone.utc)
        
        if self.budget_period == BudgetPeriod.SECOND:
            return now.replace(microsecond=0) + timedelta(seconds=1)
        elif self.budget_period == BudgetPeriod.MINUTE:
            return now.replace(second=0, microsecond=0) + timedelta(minutes=1)
        elif self.budget_period == BudgetPeriod.HOUR:
            return now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        elif self.budget_period == BudgetPeriod.DAY:
            return now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        elif self.budget_period == BudgetPeriod.MONTH:
            # First of next month
            if now.month == 12:
                return now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            else:
                return now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
        
        return None
    
    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        
        if elapsed > 0:
            new_tokens = elapsed * self.refill_rate
            self._tokens = min(self.capacity, self._tokens + new_tokens)
            self._last_refill = now
    
    def _check_budget_reset(self) -> None:
        """Check and reset budget if period has passed."""
        if not self.budget_period or not self._budget_reset_time:
            return
        
        now = datetime.now(timezone.utc)
        if now >= self._budget_reset_time:
            old_used = self._budget_used
            self._budget_used = 0
            self._budget_reset_time = self._calculate_next_reset()
            logger.info(f"TokenBucket '{self.name}' budget reset: {old_used} used last period")
    
    def _is_budget_exhausted(self) -> bool:
        """Check if periodic budget is exhausted."""
        if self.budget_limit is None:
            return False
        return self._budget_used >= self.budget_limit
    
    def acquire(self, tokens: float = 1.0, blocking: bool = False, timeout: Optional[float] = None) -> bool:
        """
        Attempt to acquire tokens from the bucket.
        
        Args:
            tokens: Number of tokens to acquire (default 1)
            blocking: If True, wait for tokens instead of returning False
            timeout: Max seconds to wait if blocking (None = forever)
            
        Returns:
            True if tokens acquired, False otherwise
            
        Raises:
            BudgetExhausted: If periodic budget is exhausted and blocking=True
        """
        if tokens <= 0:
            raise ValueError(f"Tokens must be positive, got {tokens}")
        
        start_time = time.monotonic()
        
        while True:
            with self._lock:
                self._check_budget_reset()
                
                # Check periodic budget
                if self._is_budget_exhausted():
                    if blocking:
                        raise BudgetExhausted(
                            f"TokenBucket '{self.name}' budget exhausted: "
                            f"{self._budget_used}/{self.budget_limit} used"
                        )
                    self._total_denied += 1
                    logger.warning(f"TokenBucket '{self.name}' budget exhausted", extra={
                        "bucket": self.name,
                        "budget_used": self._budget_used,
                        "budget_limit": self.budget_limit,
                    })
                    return False
                
                # Refill based on elapsed time
                self._refill()
                
                # Try to acquire
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    self._budget_used += 1
                    self._total_acquired += 1
                    
                    # Warn if budget low
                    if self.budget_limit and self._budget_used >= self.budget_limit * 0.9:
                        remaining = self.budget_limit - self._budget_used
                        logger.warning(f"TokenBucket '{self.name}' budget low: {remaining} remaining", extra={
                            "bucket": self.name,
                            "budget_remaining": remaining,
                            "budget_limit": self.budget_limit,
                        })
                    
                    return True
                
                # Not enough tokens
                if not blocking:
                    self._total_denied += 1
                    return False
            
            # Blocking mode: wait for tokens
            if timeout is not None:
                elapsed = time.monotonic() - start_time
                if elapsed >= timeout:
                    self._total_denied += 1
                    return False
            
            # Calculate wait time for next token
            wait_time = (tokens - self._tokens) / self.refill_rate
            wait_time = min(wait_time, 0.1)  # Max 100ms per iteration
            
            if timeout is not None:
                remaining = timeout - (time.monotonic() - start_time)
                wait_time = min(wait_time, remaining)
            
            self._total_waited += 1
            time.sleep(wait_time)
    
    def wait(self, tokens: float = 1.0, timeout: Optional[float] = None) -> bool:
        """
        Wait for tokens to become available.
        
        Args:
            tokens: Number of tokens to acquire
            timeout: Max seconds to wait (None = forever)
            
        Returns:
            True if tokens acquired, False if timeout
        """
        return self.acquire(tokens=tokens, blocking=True, timeout=timeout)
    
    def get_status(self) -> BucketStatus:
        """Get current bucket status."""
        with self._lock:
            self._refill()
            self._check_budget_reset()
            
            return BucketStatus(
                name=self.name,
                tokens=self._tokens,
                capacity=self.capacity,
                refill_rate=self.refill_rate,
                budget_used=self._budget_used,
                budget_limit=self.budget_limit,
                budget_period=self.budget_period,
                budget_resets_at=self._budget_reset_time,
                is_exhausted=self._is_budget_exhausted(),
                utilization_pct=(self._budget_used / self.budget_limit * 100) if self.budget_limit else 0,
            )
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get bucket metrics for monitoring."""
        status = self.get_status()
        return {
            "name": self.name,
            "tokens_available": status.tokens,
            "capacity": status.capacity,
            "refill_rate": status.refill_rate,
            "budget_used": status.budget_used,
            "budget_limit": status.budget_limit,
            "budget_utilization_pct": status.utilization_pct,
            "is_exhausted": status.is_exhausted,
            "total_acquired": self._total_acquired,
            "total_denied": self._total_denied,
            "total_waited": self._total_waited,
        }


# =============================================================================
# IMPORTS FOR DATETIME (fix missing import)
# =============================================================================

from datetime import timedelta
