import random


class Backoff:
    def __init__(self, base=1.0, factor=2.0, max_delay=60.0, jitter=0.2):
        self.base = float(base)
        self.factor = float(factor)
        self.max_delay = float(max_delay)
        self.jitter = float(jitter)
        self.attempt = 0

    def next_delay(self):
        self.attempt += 1
        delay = min(self.base * (self.factor ** (self.attempt - 1)), self.max_delay)
        return delay + (delay * self.jitter * random.random())

    def reset(self):
        self.attempt = 0
