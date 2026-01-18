"""
Global Rate Limiter with Exponential Backoff
Handles all external API calls with automatic 429 retry
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)
import aiohttp

logger = logging.getLogger(__name__)


class RateLimitExceeded(Exception):
    """Raised when rate limit is exceeded"""
    pass


class RateLimiter:
    """
    Global rate limiter using token bucket algorithm
    Automatically handles 429 errors with exponential backoff
    """
    
    def __init__(
        self,
        requests_per_second: int = 10,
        burst_size: int = 20,
        backoff_multiplier: float = 2.0,
        max_backoff_seconds: int = 60
    ):
        self.rate = requests_per_second
        self.burst = burst_size
        self.backoff_multiplier = backoff_multiplier
        self.max_backoff = max_backoff_seconds
        
        # Token bucket
        self.tokens = float(burst_size)
        self.last_update = datetime.now()
        self.lock = asyncio.Lock()
        
        # Per-endpoint tracking
        self.endpoint_stats: Dict[str, Dict] = {}
        
        logger.info(
            f"RateLimiter initialized: {requests_per_second} req/s, "
            f"burst={burst_size}"
        )
    
    async def _refill_tokens(self):
        """Refill tokens based on elapsed time"""
        now = datetime.now()
        elapsed = (now - self.last_update).total_seconds()
        
        # Add tokens based on rate
        tokens_to_add = elapsed * self.rate
        self.tokens = min(self.burst, self.tokens + tokens_to_add)
        self.last_update = now
    
    async def acquire(self, endpoint: str = "default", tokens: int = 1):
        """
        Acquire tokens before making request
        Blocks if insufficient tokens available
        """
        async with self.lock:
            await self._refill_tokens()
            
            # Wait until we have enough tokens
            while self.tokens < tokens:
                wait_time = (tokens - self.tokens) / self.rate
                logger.debug(
                    f"Rate limit: waiting {wait_time:.2f}s for {endpoint}"
                )
                await asyncio.sleep(wait_time)
                await self._refill_tokens()
            
            # Consume tokens
            self.tokens -= tokens
            
            # Track endpoint
            if endpoint not in self.endpoint_stats:
                self.endpoint_stats[endpoint] = {
                    'requests': 0,
                    'last_request': None,
                    'errors': 0
                }
            
            self.endpoint_stats[endpoint]['requests'] += 1
            self.endpoint_stats[endpoint]['last_request'] = datetime.now()
    
    def record_error(self, endpoint: str):
        """Record error for endpoint"""
        if endpoint in self.endpoint_stats:
            self.endpoint_stats[endpoint]['errors'] += 1
    
    def get_stats(self) -> Dict:
        """Get rate limiter statistics"""
        return {
            'current_tokens': self.tokens,
            'max_tokens': self.burst,
            'rate_per_second': self.rate,
            'endpoints': self.endpoint_stats.copy()
        }


# Global rate limiter instance
_global_limiter: Optional[RateLimiter] = None


def init_global_limiter(
    requests_per_second: int = 10,
    burst_size: int = 20
) -> RateLimiter:
    """Initialize global rate limiter"""
    global _global_limiter
    _global_limiter = RateLimiter(
        requests_per_second=requests_per_second,
        burst_size=burst_size
    )
    return _global_limiter


def get_global_limiter() -> RateLimiter:
    """Get global rate limiter instance"""
    if _global_limiter is None:
        raise RuntimeError("Rate limiter not initialized. Call init_global_limiter() first")
    return _global_limiter


@retry(
    retry=retry_if_exception_type((aiohttp.ClientError, RateLimitExceeded)),
    wait=wait_exponential(multiplier=2, min=1, max=60),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
async def rate_limited_request(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    endpoint_name: str = "default",
    **kwargs
) -> aiohttp.ClientResponse:
    """
    Make rate-limited HTTP request with automatic retry on 429
    
    Args:
        session: aiohttp session
        method: HTTP method (GET, POST, etc.)
        url: Request URL
        endpoint_name: Endpoint identifier for tracking
        **kwargs: Additional arguments for aiohttp request
    
    Returns:
        Response object
    
    Raises:
        RateLimitExceeded: If rate limit exceeded after retries
        aiohttp.ClientError: On other HTTP errors
    """
    limiter = get_global_limiter()
    
    # Acquire token
    await limiter.acquire(endpoint_name)
    
    try:
        async with session.request(method, url, **kwargs) as response:
            if response.status == 429:
                limiter.record_error(endpoint_name)
                logger.warning(f"Rate limit 429 for {endpoint_name}: {url}")
                raise RateLimitExceeded(f"Rate limit exceeded for {endpoint_name}")
            
            response.raise_for_status()
            return response
    
    except aiohttp.ClientError as e:
        limiter.record_error(endpoint_name)
        logger.error(f"Request error for {endpoint_name}: {e}")
        raise
