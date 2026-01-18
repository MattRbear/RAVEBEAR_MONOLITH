"""
Self-Healing Orchestrator
Manages strategy lifecycle with automatic restart on crash
Isolates failures to prevent cascade
"""

import asyncio
import logging
from typing import Dict, List
from datetime import datetime
from strategies.base_strategy import BaseStrategy
from core.health_check import HealthCheck
from data.models import StrategyStatus

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Self-healing orchestrator for managing multiple strategies
    Automatically restarts crashed strategies without affecting others
    """
    
    def __init__(
        self,
        restart_delay: int = 5,
        max_restart_attempts: int = 3,
        health_check_interval: int = 60
    ):
        self.restart_delay = restart_delay
        self.max_attempts = max_restart_attempts
        
        self.strategies: Dict[str, BaseStrategy] = {}
        self.tasks: Dict[str, asyncio.Task] = {}
        self.restart_counts: Dict[str, int] = {}
        
        self.health_check = HealthCheck(heartbeat_interval=health_check_interval)
        self.running = False
        
        logger.info(
            f"Orchestrator initialized: restart_delay={restart_delay}s, "
            f"max_attempts={max_restart_attempts}"
        )
    
    def register_strategy(self, strategy: BaseStrategy):
        """Register a strategy for management"""
        self.strategies[strategy.name] = strategy
        self.restart_counts[strategy.name] = 0
        logger.info(f"Registered strategy: {strategy.name}")
    
    async def _run_strategy_with_restart(self, name: str):
        """
        Run strategy with automatic restart on failure
        Isolated per-strategy error handling
        """
        strategy = self.strategies[name]
        
        while self.running:
            try:
                logger.info(f"Launching strategy: {name}")
                
                # Run strategy
                await strategy.start()
                
                # If we get here, strategy stopped gracefully
                logger.info(f"Strategy '{name}' stopped gracefully")
                break
            
            except Exception as e:
                self.restart_counts[name] += 1
                attempts = self.restart_counts[name]
                
                logger.error(
                    f"Strategy '{name}' crashed (attempt {attempts}/{self.max_attempts}): {e}",
                    exc_info=True
                )
                
                # Update health check
                self.health_check.register_strategy(name, strategy.get_health_status())
                
                # Check restart limit
                if attempts >= self.max_attempts:
                    logger.critical(
                        f"Strategy '{name}' exceeded max restart attempts. "
                        f"Giving up."
                    )
                    strategy.status = StrategyStatus.ERROR
                    break
                
                # Wait before restart
                logger.info(
                    f"Restarting strategy '{name}' in {self.restart_delay}s..."
                )
                await asyncio.sleep(self.restart_delay)
    
    async def start_all(self):
        """Start all registered strategies"""
        self.running = True
        logger.info(f"Starting {len(self.strategies)} strategies")
        
        # Start health check
        health_task = asyncio.create_task(self.health_check.run())
        
        # Start each strategy in isolated task
        for name in self.strategies:
            task = asyncio.create_task(self._run_strategy_with_restart(name))
            self.tasks[name] = task
            logger.info(f"Strategy task created: {name}")
        
        # Monitor strategies
        try:
            # Wait for all tasks (or until stopped)
            await asyncio.gather(*self.tasks.values(), health_task)
        
        except asyncio.CancelledError:
            logger.info("Orchestrator cancelled")
        
        finally:
            await self.stop_all()
    
    async def stop_all(self):
        """Stop all strategies gracefully"""
        logger.info("Stopping all strategies")
        self.running = False
        
        # Stop health check
        await self.health_check.stop()
        
        # Stop each strategy
        for name, strategy in self.strategies.items():
            try:
                logger.info(f"Stopping strategy: {name}")
                await strategy.stop()
            except Exception as e:
                logger.error(f"Error stopping strategy '{name}': {e}")
        
        # Cancel all tasks
        for name, task in self.tasks.items():
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        logger.info("All strategies stopped")
    
    def get_status(self) -> Dict:
        """Get orchestrator status"""
        return {
            'running': self.running,
            'strategies': {
                name: {
                    'status': strategy.status.value,
                    'restart_count': self.restart_counts.get(name, 0),
                    'health': strategy.get_health_status().model_dump()
                }
                for name, strategy in self.strategies.items()
            },
            'system_metrics': self.health_check.get_system_metrics().model_dump()
        }
