import asyncio
import time


class TokenBucket:
    def __init__(self, rate_per_sec, capacity=None):
        self.rate = float(rate_per_sec)
        self.capacity = float(capacity) if capacity is not None else float(rate_per_sec)
        self.tokens = self.capacity
        self.updated = time.monotonic()

    async def acquire(self, tokens=1.0):
        tokens = float(tokens)
        while True:
            now = time.monotonic()
            elapsed = now - self.updated
            self.updated = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            if self.tokens >= tokens:
                self.tokens -= tokens
                return
            await asyncio.sleep(0.05)
