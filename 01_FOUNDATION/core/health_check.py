"""
Health Check System
Writes heartbeat to log file every 60 seconds
Tracks system metrics and strategy health
"""

import asyncio
import logging
import psutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from data.models import HealthStatus, SystemMetrics

logger = logging.getLogger(__name__)


class HealthCheck:
    """
    System health monitoring with periodic heartbeat
    """
    
    def __init__(
        self,
        heartbeat_interval: int = 60,
        log_file: str = "data/heartbeat.log"
    ):
        self.interval = heartbeat_interval
        self.log_file = Path(log_file)
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        
        self.start_time = datetime.now()
        self.running = False
        self.strategy_health: Dict[str, HealthStatus] = {}
        
        logger.info(f"HealthCheck initialized: {heartbeat_interval}s interval")
    
    def register_strategy(self, name: str, status: HealthStatus):
        """Register or update strategy health"""
        self.strategy_health[name] = status
    
    def get_system_metrics(self) -> SystemMetrics:
        """Collect current system metrics"""
        uptime = (datetime.now() - self.start_time).total_seconds()
        
        return SystemMetrics(
            timestamp=datetime.now(),
            cpu_percent=psutil.cpu_percent(interval=0.1),
            ram_percent=psutil.virtual_memory().percent,
            uptime_seconds=int(uptime),
            active_strategies=len([
                s for s in self.strategy_health.values()
                if s.status.value == "running"
            ]),
            total_events=sum(
                s.events_processed for s in self.strategy_health.values()
            )
        )
    
    def write_heartbeat(self):
        """Write heartbeat to log file"""
        try:
            metrics = self.get_system_metrics()
            
            with open(self.log_file, 'a') as f:
                # System metrics
                f.write(f"\n{'='*80}\n")
                f.write(f"HEARTBEAT: {metrics.timestamp.isoformat()}\n")
                f.write(f"{'='*80}\n")
                f.write(f"Uptime: {metrics.uptime_seconds}s\n")
                f.write(f"CPU: {metrics.cpu_percent:.1f}%\n")
                f.write(f"RAM: {metrics.ram_percent:.1f}%\n")
                f.write(f"Active Strategies: {metrics.active_strategies}\n")
                f.write(f"Total Events: {metrics.total_events}\n")
                
                # Strategy health
                f.write(f"\nStrategy Health:\n")
                f.write(f"{'-'*80}\n")
                
                for name, health in self.strategy_health.items():
                    status_icon = {
                        "running": "✓",
                        "error": "✗",
                        "stopped": "○",
                        "initializing": "⋯"
                    }.get(health.status.value, "?")
                    
                    f.write(
                        f"{status_icon} {name:20s} | "
                        f"Status: {health.status.value:12s} | "
                        f"Events: {health.events_processed:6d} | "
                        f"Errors: {health.errors_count:3d}"
                    )
                    
                    if health.pnl is not None:
                        f.write(f" | PnL: {health.pnl:+.2f}")
                    
                    f.write("\n")
                    
                    if health.last_error:
                        f.write(f"  └─ Last Error: {health.last_error}\n")
                
                f.write(f"{'='*80}\n")
            
            logger.debug(f"Heartbeat written: {metrics.active_strategies} strategies active")
        
        except Exception as e:
            logger.error(f"Failed to write heartbeat: {e}")
    
    async def run(self):
        """Run periodic heartbeat"""
        self.running = True
        logger.info("HealthCheck started")
        
        while self.running:
            self.write_heartbeat()
            await asyncio.sleep(self.interval)
    
    async def stop(self):
        """Stop health check"""
        self.running = False
        self.write_heartbeat()  # Final heartbeat
        logger.info("HealthCheck stopped")
