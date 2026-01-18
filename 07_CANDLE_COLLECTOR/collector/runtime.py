import asyncio
import json
import logging

import aiohttp

from .adapters.binance import BinanceAdapter
from .aggregation.aggregator import TimeframeAggregator
from .backfill.backfill import BackfillManager
from .storage.parquet_writer import ParquetStorage
from .utils.rate_limiter import TokenBucket
from .utils.time import now_ms
from .validation.validators import CandleValidator


class CollectorRuntime:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.session = aiohttp.ClientSession()

        if config.exchange.name.lower() != "binance":
            raise ValueError("Only binance adapter is implemented")

        self.adapter = BinanceAdapter(config.exchange, self.session)
        self.storage = ParquetStorage(config.storage)
        self.validator = CandleValidator(config.validation.out_of_order_window)

        self.aggregator = None
        if config.aggregation.enabled and config.derive_timeframes:
            self.aggregator = TimeframeAggregator(
                base_timeframe=config.aggregation.base_timeframe,
                target_timeframes=config.derive_timeframes,
            )

        self.rate_limiter = TokenBucket(
            rate_per_sec=config.backfill.rate_limit_per_sec
        )
        self.backfill = BackfillManager(
            adapter=self.adapter,
            storage=self.storage,
            validator=self.validator,
            rate_limiter=self.rate_limiter,
            process_candle=self.process_candle,
            config=config,
        )

    async def close(self):
        await self.session.close()

    async def process_candle(self, candle):
        candle["ingest_time_ms"] = now_ms()
        for ready in self.validator.add(candle):
            self.storage.write_candles(ready)

        if self.aggregator is None:
            return

        for agg in self.aggregator.update(candle):
            agg["ingest_time_ms"] = now_ms()
            for ready in self.validator.add(agg):
                self.storage.write_candles(ready)

    async def flush(self):
        for ready in self.validator.flush_all():
            self.storage.write_candles(ready)

    async def run(self, stop_event):
        await self.backfill.backfill_all()
        await self._run_ws_loop(stop_event)

    async def _run_ws_loop(self, stop_event):
        ws = await self.adapter.connect_ws()
        await self.adapter.subscribe(ws, self.config.symbols, self.config.ws_timeframes)
        self.logger.info("event=ws_connected exchange=%s", self.config.exchange.name)
        try:
            async for msg in ws:
                if stop_event.is_set():
                    break
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    candle = self.adapter.parse_ws_message(data)
                    if candle is None:
                        continue
                    if candle.get("is_closed") is False:
                        continue
                    await self.process_candle(candle)
                elif msg.type == aiohttp.WSMsgType.PING:
                    await ws.pong()
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                    break
        finally:
            await ws.close()
            await self.flush()

        if stop_event.is_set():
            self.logger.info("event=ws_shutdown")
            return

        raise RuntimeError("ws_disconnected")
