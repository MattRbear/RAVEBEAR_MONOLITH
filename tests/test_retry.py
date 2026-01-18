"""Tests for retry and backoff utilities."""

import asyncio
import random

import pytest

from ravebear_monolith.util.errors import ConfigError, RateLimitError
from ravebear_monolith.util.retry import (
    RetryPolicy,
    _calculate_delay,
    classify_error,
    retry_async,
)


class TestRetryPolicy:
    """Tests for RetryPolicy validation."""

    def test_default_values(self) -> None:
        """Default policy has expected values."""
        policy = RetryPolicy()
        assert policy.max_attempts == 5
        assert policy.base_delay_s == 0.25
        assert policy.max_delay_s == 5.0
        assert policy.jitter == 0.10
        assert policy.backoff == "exponential"

    def test_invalid_max_attempts(self) -> None:
        """max_attempts < 1 raises ValueError."""
        with pytest.raises(ValueError, match="max_attempts"):
            RetryPolicy(max_attempts=0)

    def test_invalid_base_delay(self) -> None:
        """base_delay_s <= 0 raises ValueError."""
        with pytest.raises(ValueError, match="base_delay_s"):
            RetryPolicy(base_delay_s=0)

    def test_invalid_max_delay(self) -> None:
        """max_delay_s < base_delay_s raises ValueError."""
        with pytest.raises(ValueError, match="max_delay_s"):
            RetryPolicy(base_delay_s=1.0, max_delay_s=0.5)

    def test_invalid_jitter(self) -> None:
        """jitter outside [0, 1] raises ValueError."""
        with pytest.raises(ValueError, match="jitter"):
            RetryPolicy(jitter=1.5)


class TestClassifyError:
    """Tests for error classification."""

    def test_rate_limit_error(self) -> None:
        """RateLimitError -> rate_limited."""
        assert classify_error(RateLimitError("too fast")) == "rate_limited"

    def test_config_error_fatal(self) -> None:
        """ConfigError -> fatal."""
        assert classify_error(ConfigError("bad config")) == "fatal"

    def test_value_error_fatal(self) -> None:
        """ValueError -> fatal."""
        assert classify_error(ValueError("bad value")) == "fatal"

    def test_type_error_fatal(self) -> None:
        """TypeError -> fatal."""
        assert classify_error(TypeError("bad type")) == "fatal"

    def test_timeout_error_transient(self) -> None:
        """asyncio.TimeoutError -> transient."""
        assert classify_error(asyncio.TimeoutError()) == "transient"

    def test_runtime_error_transient(self) -> None:
        """Unknown errors default to transient."""
        assert classify_error(RuntimeError("something")) == "transient"

    def test_connection_error_transient(self) -> None:
        """Connection errors are transient."""
        assert classify_error(ConnectionError("network")) == "transient"


class TestCalculateDelay:
    """Tests for delay calculation."""

    def test_exponential_backoff(self) -> None:
        """Delay grows exponentially with attempts."""
        policy = RetryPolicy(base_delay_s=1.0, jitter=0.0, max_delay_s=100.0)
        rng = random.Random(42)

        delays = [_calculate_delay(i, policy, rng) for i in range(1, 5)]
        # With jitter=0: 1, 2, 4, 8
        assert delays == pytest.approx([1.0, 2.0, 4.0, 8.0], rel=0.01)

    def test_delay_capped_at_max(self) -> None:
        """Delay is clamped to max_delay_s."""
        policy = RetryPolicy(base_delay_s=1.0, max_delay_s=3.0, jitter=0.0)
        rng = random.Random(42)

        # Attempt 4 would be 8s, but capped at 3s
        delay = _calculate_delay(4, policy, rng)
        assert delay == 3.0

    def test_jitter_bounds(self) -> None:
        """Jitter stays within bounds."""
        policy = RetryPolicy(base_delay_s=1.0, jitter=0.10, max_delay_s=100.0)

        # Test many samples
        for seed in range(100):
            rng = random.Random(seed)
            delay = _calculate_delay(1, policy, rng)
            # With jitter=0.10, delay should be 1.0 * (0.9 to 1.1)
            assert 0.89 <= delay <= 1.11


class TestRetryAsync:
    """Tests for retry_async function."""

    @pytest.mark.asyncio
    async def test_success_first_try(self) -> None:
        """Successful call returns immediately."""
        call_count = 0

        async def fn() -> str:
            nonlocal call_count
            call_count += 1
            return "success"

        result = await retry_async(fn)
        assert result == "success"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_transient_retry_then_success(self) -> None:
        """Transient failures are retried until success."""
        call_count = 0

        async def fn() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("network error")
            return "success"

        policy = RetryPolicy(base_delay_s=0.01, jitter=0.0)
        result = await retry_async(fn, policy=policy)
        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_fatal_error_no_retry(self) -> None:
        """Fatal errors raise immediately without retry."""
        call_count = 0

        async def fn() -> str:
            nonlocal call_count
            call_count += 1
            raise ConfigError("bad config")

        policy = RetryPolicy(max_attempts=5)
        with pytest.raises(ConfigError):
            await retry_async(fn, policy=policy)

        assert call_count == 1

    @pytest.mark.asyncio
    async def test_rate_limited_retried(self) -> None:
        """Rate limited errors are retried."""
        call_count = 0

        async def fn() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise RateLimitError("too fast")
            return "success"

        policy = RetryPolicy(base_delay_s=0.01, jitter=0.0)
        result = await retry_async(fn, policy=policy)
        assert result == "success"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_max_attempts_enforced(self) -> None:
        """Raises after max_attempts exhausted."""
        call_count = 0

        async def fn() -> str:
            nonlocal call_count
            call_count += 1
            raise ConnectionError("always fails")

        policy = RetryPolicy(max_attempts=3, base_delay_s=0.01, jitter=0.0)
        with pytest.raises(ConnectionError):
            await retry_async(fn, policy=policy)

        assert call_count == 3

    @pytest.mark.asyncio
    async def test_on_attempt_callback(self) -> None:
        """on_attempt callback is called with correct args."""
        attempts: list[tuple[int, str, float, Exception]] = []

        def on_attempt(attempt: int, classification: str, delay: float, exc: Exception) -> None:
            attempts.append((attempt, classification, delay, exc))

        call_count = 0

        async def fn() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("network")
            return "ok"

        policy = RetryPolicy(base_delay_s=0.01, jitter=0.0)
        await retry_async(fn, policy=policy, on_attempt=on_attempt)

        assert len(attempts) == 2
        assert attempts[0][0] == 1
        assert attempts[0][1] == "transient"
        assert attempts[1][0] == 2

    @pytest.mark.asyncio
    async def test_deterministic_with_seeded_rng(self) -> None:
        """Delays are deterministic with seeded RNG."""
        delays: list[float] = []

        def on_attempt(attempt: int, classification: str, delay: float, exc: Exception) -> None:
            delays.append(delay)

        async def fn() -> str:
            raise ConnectionError("fail")

        policy = RetryPolicy(max_attempts=3, base_delay_s=1.0, jitter=0.10)
        rng = random.Random(12345)

        with pytest.raises(ConnectionError):
            await retry_async(fn, policy=policy, on_attempt=on_attempt, rng=rng)

        # Run again with same seed
        delays2: list[float] = []

        def on_attempt2(attempt: int, classification: str, delay: float, exc: Exception) -> None:
            delays2.append(delay)

        rng2 = random.Random(12345)
        with pytest.raises(ConnectionError):
            await retry_async(fn, policy=policy, on_attempt=on_attempt2, rng=rng2)

        # Should be identical
        assert delays == delays2
