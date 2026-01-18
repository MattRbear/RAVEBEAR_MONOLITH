"""
Multi-venue runtime orchestrator.
Manages WebSocket connections, backfill, and data flow for all venues.
"""
import asyncio
import json
import logging
from typing import Dict, List

import aiohttp

from .adapters.coinbase import CoinbaseAdapter
from .adapters.kraken import KrakenAdapter
from .adapters.okx import OKXAdapter
from .aggregation.aggregator import TimeframeAggregator
from .backfill.gap_detector import GapDetector
from .core.health import HealthMonitor
from .storage.parquet_writer import ParquetStorage
from .utils.rate_limiter import TokenBucket
from .utils.time import now_ms
from .validation.validators import CandleValidator


class MultiVenueRuntime:
    """Orchestrate data collection across multiple venues."""
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None)
        )
        
        # Initialize core components
        self.storage = ParquetStorage(config.storage)
        self.validator = CandleValidator(config.validation.out_of_order_window)
        self.health = HealthMonitor()
        self.gap_detector = GapDetector(
            self.storage,
            lookback_days=config.gap_detection.lookback_days
        )
        
        # Initialize venue adapters
        self.adapters = {}
        if "coinbase" in config.venues and config.venues["coinbase"].enabled:
            self.adapters["coinbase"] = CoinbaseAdapter(self.session)
            self.health.register_venue("coinbase")
        
        if "kraken" in config.venues and config.venues["kraken"].enabled:
            self.adapters["kraken"] = KrakenAdapter(self.session)
            self.health.register_venue("kraken")
        
        if "okx" in config.venues and config.venues["okx"].enabled:
            self.adapters["okx"] = OKXAdapter(self.session)
            self.health.register_venue("okx")
        
        # Initialize aggregator if enabled
        self.aggregator = None
        if config.aggregation.enabled and config.derive_timeframes:
            self.aggregator = TimeframeAggregator(
                base_timeframe=config.aggregation.base_timeframe,
                target_timeframes=config.derive_timeframes,
            )
        
        # Per-venue rate limiters
        self.rate_limiters = {}
        for venue_name, venue_config in config.venues.items():
            if venue_config.enabled:
                rate_limit = venue_config.rate_limit_per_sec
                self.rate_limiters[venue_name] = TokenBucket(rate_limit)
        
        self.logger.info("event=runtime_init venues=%s", list(self.adapters.keys()))
    
    async def close(self):
        """Close all connections and flush data."""
        await self.flush()
        await self.session.close()
        self.logger.info("event=runtime_closed")
    
    async def process_candle(self, candle: dict):
        """Process a single candle through validation and storage."""
        venue = candle.get("venue")
        
        # Update health metrics
        self.health.update_candle_received(venue, candle["open_time_ms"])
        
        # Validate and write
        for ready in self.validator.add(candle):
            self.storage.write_candles(ready)
            self.health.update_candle_written(venue, len(ready))
        
        # Aggregate to higher timeframes if enabled
        if self.aggregator and candle.get("timeframe") == self.config.aggregation.base_timeframe:
            for agg in self.aggregator.update(candle):
                agg["ingest_time_ms"] = now_ms()
                for ready in self.validator.add(agg):
                    self.storage.write_candles(ready)
                    self.health.update_candle_written(venue, len(ready))
    
    async def flush(self):
        """Flush all pending candles from validator."""
        for ready in self.validator.flush_all():
            self.storage.write_candles(ready)
        self.logger.info("event=flush_complete")
    
    async def backfill_venue(self, venue_name: str):
        """Backfill missing data for a single venue."""
        adapter = self.adapters.get(venue_name)
        if not adapter:
            return
        
        venue_config = self.config.venues.get(venue_name)
        if not venue_config:
            return
        symbols = venue_config.symbols
        timeframes = self.config.timeframes
        
        self.logger.info("event=backfill_start venue=%s symbols=%d timeframes=%d",
                        venue_name, len(symbols), len(timeframes))
        
        for symbol in symbols:
            for timeframe in timeframes:
                # Skip if this timeframe is derived (will be aggregated)
                if timeframe in self.config.derive_timeframes:
                    continue
                
                # Detect gaps
                gaps = self.gap_detector.detect_gaps(venue_name, symbol, timeframe)
                
                if not gaps:
                    continue
                
                # Backfill each gap
                for start_ms, end_ms in gaps:
                    # Chunk the gap manually
                    from collector.utils.time import timeframe_to_ms
                    tf_ms = timeframe_to_ms(timeframe)
                    max_candles = self.config.gap_detection.backfill_chunk_size
                    chunk_duration_ms = max_candles * tf_ms
                    
                    chunks = []
                    current_start = start_ms
                    while current_start < end_ms:
                        current_end = min(current_start + chunk_duration_ms, end_ms)
                        chunks.append((current_start, current_end))
                        current_start = current_end
                    
                    total_chunks = len(chunks)
                    for idx, (chunk_start, chunk_end) in enumerate(chunks):
                        progress = f"{idx+1}/{total_chunks}"
                        self.health.update_backfill_status(venue_name, True, progress)
                        
                        # Rate limit
                        await self.rate_limiters[venue_name].acquire()
                        
                        # Fetch candles
                        candles = await adapter.fetch_candles(
                            symbol, timeframe, chunk_start, chunk_end
                        )
                        
                        # Process each candle
                        for candle in candles:
                            await self.process_candle(candle)
                        
                        self.health.update_rest_request(venue_name, success=True)
        
        self.health.update_backfill_status(venue_name, False)
        self.logger.info("event=backfill_complete venue=%s", venue_name)
    
    async def run_ws_venue(self, venue_name: str, stop_event: asyncio.Event):
        """Run WebSocket connection for a single venue."""
        adapter = self.adapters.get(venue_name)
        if not adapter:
            return
        
        venue_config = self.config.venues.get(venue_name)
        if not venue_config:
            return
        symbols = venue_config.symbols
        ws_timeframes = self.config.ws_timeframes
        
        try:
            ws = await adapter.connect_ws()
            self.health.update_ws_connected(venue_name, True)
            
            await adapter.subscribe(ws, symbols, ws_timeframes)
            
            async for msg in ws:
                if stop_event.is_set():
                    break
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    candle = adapter.parse_ws_message(data)
                    
                    if candle is None:
                        continue
                    
                    self.health.update_ws_message(venue_name)
                    await self.process_candle(candle)
                    
                elif msg.type == aiohttp.WSMsgType.PING:
                    await ws.pong()
                    
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                    break
            
            await ws.close()
            
        except Exception as exc:
            self.logger.error("event=ws_error venue=%s error=%s", venue_name, exc)
        finally:
            self.health.update_ws_connected(venue_name, False)
        
        if not stop_event.is_set():
            raise RuntimeError(f"ws_disconnected venue={venue_name}")
    
    async def run(self, stop_event: asyncio.Event):
        """Run the multi-venue collector."""
        # Log startup
        self.logger.info("=" * 80)
        self.logger.info("MULTI-VENUE CANDLE COLLECTOR STARTING")
        self.logger.info("Venues: %s", list(self.adapters.keys()))
        self.logger.info("=" * 80)
        
        # Backfill all venues
        backfill_tasks = [
            self.backfill_venue(venue_name)
            for venue_name in self.adapters.keys()
        ]
        await asyncio.gather(*backfill_tasks)
        
        # Log initial health check
        self.health.log_status(force=True)
        
        # Start WebSocket tasks for all venues
        ws_tasks = [
            asyncio.create_task(self.run_ws_venue(venue_name, stop_event))
            for venue_name in self.adapters.keys()
        ]
        
        # Health monitoring task
        async def health_monitor():
            while not stop_event.is_set():
                await asyncio.sleep(30)  # Log every 30 seconds
                self.health.log_status()
        
        monitor_task = asyncio.create_task(health_monitor())
        
        # Wait for all tasks
        try:
            await asyncio.gather(*ws_tasks, monitor_task)
        except Exception as exc:
            self.logger.error("event=runtime_error error=%s", exc)
            raise
        finally:
            await self.flush()
            self.health.log_status(force=True)
