import logging

from ..utils.time import timeframe_to_ms, floor_time, now_ms


class BackfillManager:
    def __init__(self, adapter, storage, validator, rate_limiter, process_candle, config):
        self.adapter = adapter
        self.storage = storage
        self.validator = validator
        self.rate_limiter = rate_limiter
        self.process_candle = process_candle
        self.config = config
        self.logger = logging.getLogger(__name__)

    async def backfill_all(self):
        now = now_ms()
        direct_timeframes = [
            tf for tf in self.config.timeframes if tf not in self.config.derive_timeframes
        ]
        for symbol in self.config.symbols:
            for timeframe in direct_timeframes:
                await self.backfill_symbol_timeframe(symbol, timeframe, now)

    async def backfill_symbol_timeframe(self, symbol, timeframe, now_ms_val):
        duration = timeframe_to_ms(timeframe)
        last_open = self.storage.get_last_open_time(
            self.config.exchange.name, symbol, timeframe
        )
        if last_open is None:
            start = now_ms_val - (self.config.backfill.lookback_days * 86400000)
        else:
            start = last_open + duration

        end = now_ms_val - duration
        start = floor_time(start, duration)
        end = floor_time(end, duration)
        if start > end:
            return

        self.logger.info(
            "event=backfill_start symbol=%s timeframe=%s start=%s end=%s",
            symbol,
            timeframe,
            start,
            end,
        )

        current = start
        limit = self.config.backfill.max_limit
        while current <= end:
            await self.rate_limiter.acquire()
            chunk_end = min(end, current + duration * (limit - 1))
            candles = await self.adapter.fetch_klines(
                symbol=symbol,
                timeframe=timeframe,
                start_ms=current,
                end_ms=chunk_end,
                limit=limit,
            )
            if not candles:
                current += duration
                continue

            existing = self.storage.existing_open_times(
                self.config.exchange.name, symbol, timeframe, current, chunk_end
            )

            for candle in candles:
                if candle["open_time_ms"] in existing:
                    continue
                await self.process_candle(candle)

            current = candles[-1]["open_time_ms"] + duration

        self.logger.info(
            "event=backfill_complete symbol=%s timeframe=%s",
            symbol,
            timeframe,
        )
