"""Central async rate limiter with token bucket algorithm.

Provides:
- AsyncTokenBucket: Token bucket rate limiter with on-demand refill
- BudgetRegistry: Named bucket management
"""

import asyncio
import time
from typing import Callable

from ravebear_monolith.util.errors import BudgetNotFoundError, RateLimitError


class AsyncTokenBucket:
    """Async token bucket rate limiter.

    Tokens are refilled on-demand using monotonic time calculation.
    No background tasks required.

    Args:
        name: Identifier for this bucket.
        rate_per_sec: Token refill rate per second.
        burst: Maximum tokens (bucket capacity).
        time_func: Time function for testing (default: time.monotonic).
    """

    def __init__(
        self,
        name: str,
        rate_per_sec: float,
        burst: int,
        *,
        time_func: Callable[[], float] | None = None,
    ) -> None:
        if rate_per_sec <= 0:
            raise ValueError("rate_per_sec must be positive")
        if burst <= 0:
            raise ValueError("burst must be positive")

        self.name = name
        self.rate_per_sec = rate_per_sec
        self.burst = burst
        self._time_func = time_func or time.monotonic

        # Start with full bucket
        self._tokens = float(burst)
        self._last_refill = self._time_func()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = self._time_func()
        elapsed = now - self._last_refill
        self._tokens = min(self.burst, self._tokens + elapsed * self.rate_per_sec)
        self._last_refill = now

    async def acquire(self, cost: int = 1) -> None:
        """Acquire tokens from the bucket.

        Args:
            cost: Number of tokens to acquire.

        Raises:
            RateLimitError: If cost exceeds burst capacity.
        """
        if cost > self.burst:
            raise RateLimitError(
                f"Request cost {cost} exceeds burst capacity {self.burst} for bucket '{self.name}'"
            )

        async with self._lock:
            self._refill()

            while self._tokens < cost:
                # Calculate wait time for enough tokens
                needed = cost - self._tokens
                wait_time = needed / self.rate_per_sec
                await asyncio.sleep(wait_time)
                self._refill()

            self._tokens -= cost

    @property
    def available_tokens(self) -> float:
        """Current available tokens (read-only snapshot)."""
        # Note: This is approximate without holding the lock
        now = self._time_func()
        elapsed = now - self._last_refill
        return min(self.burst, self._tokens + elapsed * self.rate_per_sec)


class BudgetRegistry:
    """Registry of named token buckets.

    Provides centralized management of rate limit budgets.
    """

    def __init__(self) -> None:
        self._buckets: dict[str, AsyncTokenBucket] = {}

    def register(
        self,
        name: str,
        rate_per_sec: float,
        burst: int,
        *,
        time_func: Callable[[], float] | None = None,
    ) -> AsyncTokenBucket:
        """Register a new budget bucket.

        Args:
            name: Unique identifier for the bucket.
            rate_per_sec: Token refill rate per second.
            burst: Maximum tokens (bucket capacity).
            time_func: Optional time function for testing.

        Returns:
            The created AsyncTokenBucket.

        Raises:
            ValueError: If bucket with name already exists.
        """
        if name in self._buckets:
            raise ValueError(f"Bucket '{name}' already registered")

        bucket = AsyncTokenBucket(
            name=name,
            rate_per_sec=rate_per_sec,
            burst=burst,
            time_func=time_func,
        )
        self._buckets[name] = bucket
        return bucket

    def get(self, name: str) -> AsyncTokenBucket:
        """Get a registered bucket by name.

        Args:
            name: Bucket identifier.

        Returns:
            The AsyncTokenBucket instance.

        Raises:
            BudgetNotFoundError: If bucket is not registered.
        """
        if name not in self._buckets:
            raise BudgetNotFoundError(f"Unknown budget bucket: '{name}'")
        return self._buckets[name]

    def __contains__(self, name: str) -> bool:
        """Check if bucket is registered."""
        return name in self._buckets

    @property
    def bucket_names(self) -> list[str]:
        """List of registered bucket names."""
        return list(self._buckets.keys())
