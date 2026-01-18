"""Retry and backoff policy with error classification.

Provides:
- RetryPolicy: Configuration for retry behavior
- classify_error: Categorize exceptions as transient/rate_limited/fatal
- retry_async: Async retry wrapper with exponential backoff
"""

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal, TypeVar

from ravebear_monolith.util.errors import ConfigError, RateLimitError

T = TypeVar("T")

ErrorClassification = Literal["transient", "rate_limited", "fatal"]


@dataclass(frozen=True)
class RetryPolicy:
    """Configuration for retry behavior.

    Attributes:
        max_attempts: Maximum number of attempts (including initial).
        base_delay_s: Initial delay between retries.
        max_delay_s: Maximum delay cap.
        jitter: Proportional jitter (+/- percentage).
        backoff: Backoff strategy (currently only exponential).
    """

    max_attempts: int = 5
    base_delay_s: float = 0.25
    max_delay_s: float = 5.0
    jitter: float = 0.10
    backoff: Literal["exponential"] = "exponential"

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay_s <= 0:
            raise ValueError("base_delay_s must be positive")
        if self.max_delay_s < self.base_delay_s:
            raise ValueError("max_delay_s must be >= base_delay_s")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be between 0 and 1")


def classify_error(exc: Exception) -> ErrorClassification:
    """Classify an exception for retry decisions.

    Args:
        exc: The exception to classify.

    Returns:
        "rate_limited" for RateLimitError
        "fatal" for ConfigError, ValueError, TypeError
        "transient" for everything else (including TimeoutError)
    """
    if isinstance(exc, RateLimitError):
        return "rate_limited"
    if isinstance(exc, (ConfigError, ValueError, TypeError)):
        return "fatal"
    if isinstance(exc, asyncio.TimeoutError):
        return "transient"
    # Default to transient (network errors, etc.)
    return "transient"


def _calculate_delay(
    attempt: int,
    policy: RetryPolicy,
    rng: random.Random | None = None,
) -> float:
    """Calculate delay with exponential backoff and jitter.

    Args:
        attempt: Current attempt number (1-indexed).
        policy: Retry policy configuration.
        rng: Optional random instance for testing.

    Returns:
        Delay in seconds.
    """
    if rng is None:
        rng = random.Random()

    # Exponential backoff: base * 2^(attempt-1)
    delay = policy.base_delay_s * (2 ** (attempt - 1))

    # Apply jitter: delay * (1 +/- jitter)
    jitter_factor = 1 + rng.uniform(-policy.jitter, policy.jitter)
    delay *= jitter_factor

    # Clamp to max_delay_s
    return min(delay, policy.max_delay_s)


# Type for on_attempt callback
OnAttemptCallback = Callable[[int, ErrorClassification, float, Exception], None]


async def retry_async(
    fn: Callable[[], Awaitable[T]],
    *,
    policy: RetryPolicy | None = None,
    on_attempt: OnAttemptCallback | None = None,
    rng: random.Random | None = None,
) -> T:
    """Execute async function with retry on transient/rate_limited errors.

    Args:
        fn: Async function to call (no arguments).
        policy: Retry policy. Defaults to RetryPolicy().
        on_attempt: Optional callback for logging attempts.
            Called with (attempt, classification, delay_s, exc).
        rng: Optional random instance for deterministic testing.

    Returns:
        Result of fn() on success.

    Raises:
        Exception: The last exception if all retries exhausted,
            or immediately on fatal classification.
    """
    if policy is None:
        policy = RetryPolicy()

    last_exception: Exception | None = None

    for attempt in range(1, policy.max_attempts + 1):
        try:
            return await fn()
        except Exception as exc:
            last_exception = exc
            classification = classify_error(exc)

            # Fatal errors: raise immediately, no retry
            if classification == "fatal":
                if on_attempt:
                    on_attempt(attempt, classification, 0.0, exc)
                raise

            # Last attempt: don't sleep, just raise
            if attempt >= policy.max_attempts:
                if on_attempt:
                    on_attempt(attempt, classification, 0.0, exc)
                raise

            # Calculate delay and notify
            delay = _calculate_delay(attempt, policy, rng)
            if on_attempt:
                on_attempt(attempt, classification, delay, exc)

            await asyncio.sleep(delay)

    # Should never reach here, but satisfy type checker
    assert last_exception is not None
    raise last_exception
