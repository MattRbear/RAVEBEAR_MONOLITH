"""
HTTP retry utilities with bounded loops, exponential backoff, and rate limit handling.

NO recursion - all retries use bounded loops.
"""
import asyncio
import logging
import random
from typing import Optional, Callable, Any
from aiohttp import ClientResponse, ClientError


class RetryBudgetExhausted(Exception):
    """Raised when retry budget is exhausted."""
    pass


async def retry_with_backoff(
    request_func: Callable,
    max_attempts: int = 3,
    max_retry_after_seconds: int = 300,
    logger: Optional[logging.Logger] = None
) -> Any:
    """
    Execute HTTP request with bounded retry loop.
    
    Handles:
    - 429: Respect Retry-After header (capped to max_retry_after_seconds)
    - 5xx: Exponential backoff with jitter
    - Network errors: Exponential backoff with jitter
    
    Args:
        request_func: Async function that makes the HTTP request
        max_attempts: Maximum number of attempts (default 3)
        max_retry_after_seconds: Maximum wait time for Retry-After (default 300s)
        logger: Logger instance for structured logging
        
    Returns:
        Response from successful request
        
    Raises:
        RetryBudgetExhausted: If all attempts fail
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    last_exception = None
    
    for attempt in range(1, max_attempts + 1):
        try:
            response = await request_func()
            
            # Check status code
            if response.status == 429:
                # Rate limited - respect Retry-After
                retry_after = _get_retry_after(response, max_retry_after_seconds)
                
                if attempt < max_attempts:
                    logger.warning(
                        "event=rate_limited status=429 attempt=%d/%d retry_after=%ds",
                        attempt, max_attempts, retry_after
                    )
                    await asyncio.sleep(retry_after)
                    continue
                else:
                    logger.error(
                        "event=retry_budget_exhausted status=429 attempt=%d/%d",
                        attempt, max_attempts
                    )
                    raise RetryBudgetExhausted(
                        f"Rate limited after {max_attempts} attempts"
                    )
            
            elif 500 <= response.status < 600:
                # Server error - exponential backoff
                if attempt < max_attempts:
                    wait_seconds = _exponential_backoff_with_jitter(attempt)
                    logger.warning(
                        "event=server_error status=%d attempt=%d/%d wait=%ds",
                        response.status, attempt, max_attempts, wait_seconds
                    )
                    await asyncio.sleep(wait_seconds)
                    continue
                else:
                    logger.error(
                        "event=retry_budget_exhausted status=%d attempt=%d/%d",
                        response.status, attempt, max_attempts
                    )
                    raise RetryBudgetExhausted(
                        f"Server error {response.status} after {max_attempts} attempts"
                    )
            
            elif 200 <= response.status < 300:
                # Success
                if attempt > 1:
                    logger.info(
                        "event=retry_success status=%d attempt=%d/%d",
                        response.status, attempt, max_attempts
                    )
                return response
            
            else:
                # Client error (4xx except 429) - don't retry
                logger.error(
                    "event=client_error status=%d attempt=%d/%d",
                    response.status, attempt, max_attempts
                )
                return response
        
        except ClientError as exc:
            # Network error - exponential backoff
            last_exception = exc
            
            if attempt < max_attempts:
                wait_seconds = _exponential_backoff_with_jitter(attempt)
                logger.warning(
                    "event=network_error error=%s attempt=%d/%d wait=%ds",
                    type(exc).__name__, attempt, max_attempts, wait_seconds
                )
                await asyncio.sleep(wait_seconds)
                continue
            else:
                logger.error(
                    "event=retry_budget_exhausted error=%s attempt=%d/%d",
                    type(exc).__name__, attempt, max_attempts
                )
                raise RetryBudgetExhausted(
                    f"Network error after {max_attempts} attempts"
                ) from exc
    
    # Should never reach here, but just in case
    if last_exception:
        raise RetryBudgetExhausted(
            f"Failed after {max_attempts} attempts"
        ) from last_exception
    else:
        raise RetryBudgetExhausted(f"Failed after {max_attempts} attempts")


def _get_retry_after(response: ClientResponse, max_seconds: int) -> int:
    """
    Extract Retry-After header value, capped to max_seconds.
    
    Returns:
        Retry-After value in seconds, or default exponential backoff if not present
    """
    retry_after_header = response.headers.get("Retry-After")
    
    if retry_after_header:
        try:
            # Retry-After can be seconds or HTTP date
            retry_after = int(retry_after_header)
            # Cap to max_seconds
            return min(retry_after, max_seconds)
        except ValueError:
            # HTTP date format - not implemented, use default
            pass
    
    # No valid Retry-After header, use exponential backoff
    return min(_exponential_backoff_with_jitter(1), max_seconds)


def _exponential_backoff_with_jitter(attempt: int, base: float = 2.0) -> int:
    """
    Calculate exponential backoff with jitter.
    
    Formula: min(base^attempt + jitter, 60)
    Jitter: random [0, 1] seconds
    
    Args:
        attempt: Attempt number (1-indexed)
        base: Base for exponential calculation
        
    Returns:
        Wait time in seconds (capped at 60)
    """
    exponential = base ** attempt
    jitter = random.uniform(0, 1)
    wait_seconds = exponential + jitter
    
    # Cap at 60 seconds
    return int(min(wait_seconds, 60))
