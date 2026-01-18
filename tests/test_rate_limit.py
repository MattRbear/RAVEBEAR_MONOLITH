"""Tests for async rate limiter."""

import asyncio

import pytest

from ravebear_monolith.util.errors import BudgetNotFoundError, RateLimitError
from ravebear_monolith.util.rate_limit import AsyncTokenBucket, BudgetRegistry


class MockTime:
    """Mock time source for deterministic testing."""

    def __init__(self, start: float = 0.0) -> None:
        self._time = start

    def __call__(self) -> float:
        return self._time

    def advance(self, seconds: float) -> None:
        """Advance mock time by given seconds."""
        self._time += seconds


class TestAsyncTokenBucket:
    """Tests for AsyncTokenBucket."""

    @pytest.mark.asyncio
    async def test_acquire_under_rate_succeeds(self) -> None:
        """Acquire within available tokens succeeds immediately."""
        bucket = AsyncTokenBucket(name="test", rate_per_sec=10.0, burst=5)
        # Bucket starts full with 5 tokens
        await bucket.acquire(1)
        await bucket.acquire(1)
        await bucket.acquire(1)
        # Should have 2 tokens left
        assert bucket.available_tokens >= 1.9

    @pytest.mark.asyncio
    async def test_burst_respected(self) -> None:
        """Can acquire up to burst capacity at once."""
        bucket = AsyncTokenBucket(name="test", rate_per_sec=10.0, burst=5)
        # Full bucket, acquire all 5 tokens
        await bucket.acquire(5)
        # Should be near 0
        assert bucket.available_tokens < 1

    @pytest.mark.asyncio
    async def test_over_burst_raises_rate_limit_error(self) -> None:
        """Acquiring more than burst raises RateLimitError."""
        bucket = AsyncTokenBucket(name="test", rate_per_sec=10.0, burst=5)
        with pytest.raises(RateLimitError, match="exceeds burst capacity"):
            await bucket.acquire(10)

    @pytest.mark.asyncio
    async def test_refill_with_mock_time(self) -> None:
        """Tokens refill based on elapsed time."""
        mock_time = MockTime(start=0.0)
        bucket = AsyncTokenBucket(
            name="test",
            rate_per_sec=10.0,
            burst=5,
            time_func=mock_time,
        )

        # Consume all tokens
        await bucket.acquire(5)
        assert bucket.available_tokens < 1

        # Advance time by 0.5 seconds -> should refill 5 tokens (10/sec * 0.5s)
        mock_time.advance(0.5)
        assert bucket.available_tokens >= 4.9

    @pytest.mark.asyncio
    async def test_tokens_capped_at_burst(self) -> None:
        """Token refill is capped at burst capacity."""
        mock_time = MockTime(start=0.0)
        bucket = AsyncTokenBucket(
            name="test",
            rate_per_sec=100.0,
            burst=5,
            time_func=mock_time,
        )

        # Advance time significantly
        mock_time.advance(10.0)
        # Should still be capped at burst=5
        assert bucket.available_tokens <= 5.0

    @pytest.mark.asyncio
    async def test_wait_for_tokens(self) -> None:
        """Acquire waits for token refill when bucket is empty."""
        bucket = AsyncTokenBucket(name="test", rate_per_sec=100.0, burst=5)

        # Drain bucket
        await bucket.acquire(5)

        # Next acquire should wait and complete
        start = asyncio.get_event_loop().time()
        await asyncio.wait_for(bucket.acquire(1), timeout=1.0)
        elapsed = asyncio.get_event_loop().time() - start

        # Should have waited ~0.01 seconds for 1 token at 100/sec
        assert elapsed >= 0.005  # Some margin for timing


class TestBudgetRegistry:
    """Tests for BudgetRegistry."""

    def test_register_and_get(self) -> None:
        """Can register and retrieve buckets."""
        registry = BudgetRegistry()
        bucket = registry.register("api", rate_per_sec=10.0, burst=100)
        assert bucket.name == "api"
        assert registry.get("api") is bucket

    def test_unknown_bucket_raises(self) -> None:
        """Getting unknown bucket raises BudgetNotFoundError."""
        registry = BudgetRegistry()
        with pytest.raises(BudgetNotFoundError, match="Unknown budget"):
            registry.get("nonexistent")

    def test_duplicate_registration_raises(self) -> None:
        """Registering same name twice raises ValueError."""
        registry = BudgetRegistry()
        registry.register("api", rate_per_sec=10.0, burst=100)
        with pytest.raises(ValueError, match="already registered"):
            registry.register("api", rate_per_sec=20.0, burst=50)

    def test_contains_check(self) -> None:
        """Can check if bucket is registered."""
        registry = BudgetRegistry()
        registry.register("api", rate_per_sec=10.0, burst=100)
        assert "api" in registry
        assert "other" not in registry

    def test_bucket_names(self) -> None:
        """Can list registered bucket names."""
        registry = BudgetRegistry()
        registry.register("api", rate_per_sec=10.0, burst=100)
        registry.register("ws", rate_per_sec=5.0, burst=10)
        assert set(registry.bucket_names) == {"api", "ws"}


class TestTwoBucketsIndependent:
    """Test that multiple buckets are independent."""

    @pytest.mark.asyncio
    async def test_buckets_independent(self) -> None:
        """Two buckets have separate token pools."""
        mock_time = MockTime(start=0.0)

        bucket_a = AsyncTokenBucket(
            name="a",
            rate_per_sec=10.0,
            burst=5,
            time_func=mock_time,
        )
        bucket_b = AsyncTokenBucket(
            name="b",
            rate_per_sec=10.0,
            burst=5,
            time_func=mock_time,
        )

        # Drain bucket A
        await bucket_a.acquire(5)
        assert bucket_a.available_tokens < 1

        # Bucket B should still be full
        assert bucket_b.available_tokens >= 4.9

        # Can acquire from B without waiting
        await bucket_b.acquire(3)
        assert bucket_b.available_tokens >= 1.9


class TestDeterministicBehavior:
    """Tests for deterministic behavior under mocked time."""

    @pytest.mark.asyncio
    async def test_exact_refill_calculation(self) -> None:
        """Refill calculation is exact with mocked time."""
        mock_time = MockTime(start=100.0)
        bucket = AsyncTokenBucket(
            name="test",
            rate_per_sec=2.0,  # 2 tokens per second
            burst=10,
            time_func=mock_time,
        )

        # Consume 6 tokens
        await bucket.acquire(6)
        # Should have 4 left
        assert 3.9 <= bucket.available_tokens <= 4.1

        # Advance 1.5 seconds -> 3 tokens refilled
        mock_time.advance(1.5)
        # 4 + 3 = 7 tokens
        assert 6.9 <= bucket.available_tokens <= 7.1

        # Advance another 5 seconds -> should cap at 10
        mock_time.advance(5.0)
        assert bucket.available_tokens <= 10.0
        assert bucket.available_tokens >= 9.9
