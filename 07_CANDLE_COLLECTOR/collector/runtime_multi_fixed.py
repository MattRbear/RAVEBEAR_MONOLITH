"""
Multi-venue runtime - WITH CRITICAL METRICS.
"""
import asyncio
import json
import logging
import random
from typing import Dict, List, Optional

import aiohttp

from .adapters.coinbase import CoinbaseAdapter
from .adapters.kraken import KrakenAdapter
from .adapters.okx import OKXAdapter
from .aggregation.aggregator import TimeframeAggregator
from .core.health import HealthMonitor
from .storage.parquet_writer import ParquetStorage
from .utils.rate_limiter import TokenBucket
from .utils.time import now_ms
from .validation.validators import CandleValidator


class MultiVenueRuntime:
    """Multi-venue candle collector with critical metrics."""
    
    MAX_RECONNECT_ATTEMPTS = 100
    INITIAL_RECONNECT_DELAY = 2.0
    MAX_RECONNECT_DELAY = 60.0
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        logging.getLogger("aiohttp").setLevel(logging.ERROR)
        logging.getLogger("asyncio").setLevel(logging.ERROR)
        
        self.session: Optional[aiohttp.ClientSession] = None
        self.storage = ParquetStorage(config.storage)
        self.health = HealthMonitor()
        
        # Wire validator with dup callback to health monitor
        self.validator = CandleValidator(
            config.validation.out_of_order_window,
            on_dup_callback=self._on_dup_dropped
        )
        
        self.adapters: Dict[str, object] = {}
        self.enabled_venues: List[str] = []
        
        for venue_name in ["coinbase", "kraken", "okx"]:
            if venue_name in config.venues and config.venues[venue_name].enabled:
                self.enabled_venues.append(venue_name)
                self.health.register_venue(venue_name)
        
        self.aggregator = None
        if config.aggregation.enabled and config.derive_timeframes:
            self.aggregator = TimeframeAggregator(
                base_timeframe=config.aggregation.base_timeframe,
                target_timeframes=config.derive_timeframes,
            )
        
        self.rate_limiters = {}
        for venue_name, venue_config in config.venues.items():
            if venue_config.enabled:
                self.rate_limiters[venue_name] = TokenBucket(venue_config.rate_limit_per_sec)
    
    def _on_dup_dropped(self, venue: str, count: int):
        """Callback when validator drops duplicates."""
        self.health.update_dup_dropped(venue, count)
    
    def _update_queue_depths(self):
        """Update queue depth metrics in health monitor."""
        depths = self.validator.get_queue_depths_by_venue()
        for venue, depth in depths.items():
            self.health.update_queue_depth(venue, depth)
    
    def _init_adapters(self):
        """Initialize adapters."""
        if not self.session:
            raise RuntimeError("Session not initialized")
        
        if "coinbase" in self.enabled_venues:
            self.adapters["coinbase"] = CoinbaseAdapter(self.session)
        if "kraken" in self.enabled_venues:
            self.adapters["kraken"] = KrakenAdapter(self.session)
        if "okx" in self.enabled_venues:
            self.adapters["okx"] = OKXAdapter(self.session)
    
    async def close(self):
        """Close connections."""
        if "okx" in self.adapters and hasattr(self.adapters["okx"], 'stop_ping'):
            self.adapters["okx"].stop_ping()
        
        await self.flush()
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def process_candle(self, candle: dict):
        """Process candle."""
        venue = candle.get("venue")
        self.health.update_candle_received(venue, candle["open_time_ms"])
        
        for ready in self.validator.add(candle):
            self.storage.write_candles(ready)
            self.health.update_candle_written(venue, len(ready))
        
        if self.aggregator and candle.get("timeframe") == self.config.aggregation.base_timeframe:
            for agg in self.aggregator.update(candle):
                agg["ingest_time_ms"] = now_ms()
                for ready in self.validator.add(agg):
                    self.storage.write_candles(ready)
                    self.health.update_candle_written(venue, len(ready))
    
    async def flush(self):
        """Flush pending."""
        for ready in self.validator.flush_all():
            self.storage.write_candles(ready)
    
    async def run_ws_venue(self, venue_name: str, stop_event: asyncio.Event):
        """Run WebSocket for venue."""
        adapter = self.adapters.get(venue_name)
        venue_config = self.config.venues.get(venue_name)
        if not adapter or not venue_config:
            return
        
        symbols = venue_config.symbols
        ws_timeframes = self.config.ws_timeframes
        
        reconnect_delay = self.INITIAL_RECONNECT_DELAY
        consecutive_failures = 0
        
        while not stop_event.is_set() and consecutive_failures < self.MAX_RECONNECT_ATTEMPTS:
            ws = None
            try:
                ws = await adapter.connect_ws()
                self.health.update_ws_connected(venue_name, True)
                consecutive_failures = 0
                reconnect_delay = self.INITIAL_RECONNECT_DELAY
                
                await adapter.subscribe(ws, symbols, ws_timeframes)
                
                async for msg in ws:
                    if stop_event.is_set():
                        break
                    
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                        except json.JSONDecodeError:
                            candle = adapter.parse_ws_message(msg.data)
                            if candle:
                                await self.process_candle(candle)
                            continue
                        
                        candle = adapter.parse_ws_message(data)
                        if candle:
                            await self.process_candle(candle)
                    
                    elif msg.type == aiohttp.WSMsgType.PING:
                        await ws.pong(msg.data)
                    
                    elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, 
                                     aiohttp.WSMsgType.ERROR):
                        break
                
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if "Cannot connect" not in str(exc) and "Connection" not in str(exc):
                    self.logger.warning("WS %s: %s", venue_name, type(exc).__name__)
            finally:
                self.health.update_ws_connected(venue_name, False)
                if ws and not ws.closed:
                    try:
                        await ws.close()
                    except:
                        pass
            
            if not stop_event.is_set():
                consecutive_failures += 1
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=reconnect_delay)
                    break
                except asyncio.TimeoutError:
                    pass
                
                jitter = random.uniform(0.8, 1.2)
                reconnect_delay = min(reconnect_delay * 1.5 * jitter, self.MAX_RECONNECT_DELAY)
    
    async def run(self, stop_event: asyncio.Event):
        """Run collector."""
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None))
        self._init_adapters()
        
        try:
            print("\n" + "=" * 80)
            print("  MULTI-VENUE CANDLE COLLECTOR")
            print(f"  Venues: {', '.join(self.enabled_venues)}")
            print("=" * 80)
            
            print("\n  Starting WebSocket streams...\n")
            self.health.log_status(force=True)
            
            ws_tasks = [
                asyncio.create_task(self.run_ws_venue(venue_name, stop_event))
                for venue_name in self.adapters.keys()
            ]
            
            async def health_monitor():
                while not stop_event.is_set():
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=30)
                        break
                    except asyncio.TimeoutError:
                        # Update queue depths before status
                        self._update_queue_depths()
                        self.health.log_status()
            
            monitor_task = asyncio.create_task(health_monitor())
            
            done, pending = await asyncio.wait(
                ws_tasks + [monitor_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            if stop_event.is_set():
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            
        except Exception as exc:
            self.logger.error("Error: %s", exc)
            raise
        finally:
            await self.close()
            print("\n  Shutting down...")
            self._update_queue_depths()
            self.health.log_status(force=True)
