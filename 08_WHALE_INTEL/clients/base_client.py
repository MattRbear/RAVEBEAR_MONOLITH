"""
Flint's Whale Intelligence System - Base API Client

Purpose:
    Abstract base class for all API clients providing:
    - Common reconnection logic with exponential backoff
    - Rate limiter integration
    - Structured logging
    - Health check interface
    - Metrics collection

Inputs:
    - API name for rate limiting
    - Config object
    
Outputs:
    - health_check(): Health status dict
    - get_metrics(): Client metrics dict

Failure Modes:
    - Rate limit exhausted: Logs warning, returns None
    - Connection failed: Triggers reconnect
    - Invalid response: Logs error, returns None

Usage:
    class MyClient(BaseClient):
        def __init__(self, config):
            super().__init__("my_api", config)
        
        async def _connect(self):
            # Implementation
            pass
"""

import asyncio
import time
import random
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Callable
from datetime import datetime, timezone
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# CONNECTION STATE
# =============================================================================

class ConnectionState(Enum):
    """Client connection states."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"


# =============================================================================
# BACKOFF CONFIGURATION
# =============================================================================

@dataclass
class BackoffConfig:
    """Configuration for exponential backoff."""
    initial_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0
    jitter: float = 0.25  # ±25%
    max_attempts: int = 10
    
    def get_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt number."""
        delay = self.initial_delay * (self.multiplier ** attempt)
        delay = min(delay, self.max_delay)
        
        # Add jitter
        jitter_range = delay * self.jitter
        delay = delay + random.uniform(-jitter_range, jitter_range)
        
        return max(0.1, delay)  # Minimum 100ms


# =============================================================================
# BASE CLIENT
# =============================================================================

class BaseClient(ABC):
    """
    Abstract base class for API clients.
    
    Provides common functionality:
    - Reconnection with exponential backoff
    - Rate limiter integration
    - Health monitoring
    - Structured logging
    """
    
    def __init__(
        self,
        api_name: str,
        config: Any,
        backoff: Optional[BackoffConfig] = None,
    ):
        """
        Initialize base client.
        
        Args:
            api_name: Name for rate limiting (e.g., "whale_alert", "etherscan")
            config: Configuration object
            backoff: Backoff configuration (uses defaults if None)
        """
        self.api_name = api_name
        self.config = config
        self.backoff = backoff or BackoffConfig()
        
        # State
        self._state = ConnectionState.DISCONNECTED
        self._last_error: Optional[str] = None
        self._last_error_time: Optional[datetime] = None
        
        # Metrics
        self._connect_count = 0
        self._reconnect_count = 0
        self._error_count = 0
        self._last_activity: Optional[datetime] = None
        self._messages_received = 0
        self._messages_processed = 0
        
        # Rate limiter (lazy loaded)
        self._budget_tracker = None
        
        logger.info(f"BaseClient initialized for {api_name}")
    
    @property
    def state(self) -> ConnectionState:
        """Get current connection state."""
        return self._state
    
    @property
    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self._state == ConnectionState.CONNECTED
    
    def _get_budget_tracker(self):
        """Lazy load budget tracker."""
        if self._budget_tracker is None:
            from rate_limiting import get_budget_tracker
            self._budget_tracker = get_budget_tracker()
        return self._budget_tracker
    
    def _can_make_request(self) -> bool:
        """Check if rate limit allows a request."""
        try:
            tracker = self._get_budget_tracker()
            return tracker.can_call(self.api_name)
        except Exception as e:
            logger.warning(f"Rate limit check failed: {e}")
            return True  # Fail open for rate limit check
    
    def _acquire_rate_limit(self) -> bool:
        """Acquire rate limit token."""
        try:
            tracker = self._get_budget_tracker()
            return tracker.acquire(self.api_name)
        except Exception as e:
            logger.warning(f"Rate limit acquire failed: {e}")
            return True  # Fail open
    
    def _record_error(self, error: str) -> None:
        """Record an error occurrence."""
        self._error_count += 1
        self._last_error = error
        self._last_error_time = datetime.now(timezone.utc)
        
        logger.error(f"{self.api_name} error: {error}", extra={
            "api": self.api_name,
            "error": error,
            "error_count": self._error_count,
        })
    
    def _record_activity(self) -> None:
        """Record activity timestamp."""
        self._last_activity = datetime.now(timezone.utc)
    
    async def _reconnect_loop(self) -> bool:
        """
        Reconnection loop with exponential backoff.
        
        Returns:
            True if reconnected successfully, False if max attempts exceeded
        """
        self._state = ConnectionState.RECONNECTING
        
        for attempt in range(self.backoff.max_attempts):
            delay = self.backoff.get_delay(attempt)
            
            logger.info(f"{self.api_name} reconnecting in {delay:.1f}s (attempt {attempt + 1}/{self.backoff.max_attempts})")
            
            await asyncio.sleep(delay)
            
            try:
                await self._connect()
                self._state = ConnectionState.CONNECTED
                self._reconnect_count += 1
                
                logger.info(f"{self.api_name} reconnected successfully", extra={
                    "api": self.api_name,
                    "attempt": attempt + 1,
                    "reconnect_count": self._reconnect_count,
                })
                
                return True
                
            except Exception as e:
                self._record_error(str(e))
                continue
        
        self._state = ConnectionState.FAILED
        logger.error(f"{self.api_name} failed to reconnect after {self.backoff.max_attempts} attempts")
        return False
    
    @abstractmethod
    async def _connect(self) -> None:
        """
        Establish connection. Must be implemented by subclass.
        
        Raises:
            Exception: If connection fails
        """
        pass
    
    @abstractmethod
    async def _disconnect(self) -> None:
        """
        Close connection. Must be implemented by subclass.
        """
        pass
    
    async def connect(self) -> bool:
        """
        Connect to the API.
        
        Returns:
            True if connected successfully
        """
        if self._state == ConnectionState.CONNECTED:
            return True
        
        self._state = ConnectionState.CONNECTING
        
        try:
            await self._connect()
            self._state = ConnectionState.CONNECTED
            self._connect_count += 1
            
            logger.info(f"{self.api_name} connected", extra={
                "api": self.api_name,
                "connect_count": self._connect_count,
            })
            
            return True
            
        except Exception as e:
            self._record_error(str(e))
            self._state = ConnectionState.FAILED
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from the API."""
        if self._state == ConnectionState.DISCONNECTED:
            return
        
        try:
            await self._disconnect()
        except Exception as e:
            logger.warning(f"{self.api_name} disconnect error: {e}")
        finally:
            self._state = ConnectionState.DISCONNECTED
            logger.info(f"{self.api_name} disconnected")
    
    def health_check(self) -> Dict[str, Any]:
        """
        Get health status.
        
        Returns:
            Dict with health information
        """
        now = datetime.now(timezone.utc)
        
        # Check for stale connection
        is_stale = False
        if self._last_activity:
            staleness = (now - self._last_activity).total_seconds()
            is_stale = staleness > 300  # 5 minutes
        
        return {
            "api": self.api_name,
            "healthy": self._state == ConnectionState.CONNECTED and not is_stale,
            "state": self._state.value,
            "is_stale": is_stale,
            "last_activity": self._last_activity.isoformat() if self._last_activity else None,
            "last_error": self._last_error,
            "last_error_time": self._last_error_time.isoformat() if self._last_error_time else None,
            "error_count": self._error_count,
            "timestamp": now.isoformat(),
        }
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get client metrics.
        
        Returns:
            Dict with metrics
        """
        return {
            "api": self.api_name,
            "state": self._state.value,
            "connect_count": self._connect_count,
            "reconnect_count": self._reconnect_count,
            "error_count": self._error_count,
            "messages_received": self._messages_received,
            "messages_processed": self._messages_processed,
            "last_activity": self._last_activity.isoformat() if self._last_activity else None,
        }


# =============================================================================
# WEBSOCKET BASE CLIENT
# =============================================================================

class WebSocketClient(BaseClient):
    """
    Base class for WebSocket-based API clients.
    
    Adds:
    - WebSocket connection handling
    - Ping/pong keepalive
    - Message queue
    """
    
    def __init__(
        self,
        api_name: str,
        config: Any,
        ws_url: str,
        ping_interval: float = 30.0,
        ping_timeout: float = 10.0,
        backoff: Optional[BackoffConfig] = None,
    ):
        """
        Initialize WebSocket client.
        
        Args:
            api_name: Name for rate limiting
            config: Configuration object
            ws_url: WebSocket URL
            ping_interval: Seconds between pings
            ping_timeout: Seconds to wait for pong
            backoff: Backoff configuration
        """
        super().__init__(api_name, config, backoff)
        
        self.ws_url = ws_url
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        
        # WebSocket state
        self._ws = None
        self._receive_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        
        # Message handlers
        self._message_handlers: list[Callable] = []
    
    def add_message_handler(self, handler: Callable) -> None:
        """Add a message handler callback."""
        self._message_handlers.append(handler)
    
    async def _connect(self) -> None:
        """Establish WebSocket connection."""
        import websockets
        
        self._ws = await websockets.connect(
            self.ws_url,
            ping_interval=self.ping_interval,
            ping_timeout=self.ping_timeout,
        )
        
        # Start receive loop
        self._receive_task = asyncio.create_task(self._receive_loop())
    
    async def _disconnect(self) -> None:
        """Close WebSocket connection."""
        # Cancel tasks
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
            self._receive_task = None
        
        if self._ping_task:
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass
            self._ping_task = None
        
        # Close WebSocket
        if self._ws:
            await self._ws.close()
            self._ws = None
    
    async def _receive_loop(self) -> None:
        """Main receive loop for WebSocket messages."""
        import websockets
        
        try:
            async for message in self._ws:
                self._messages_received += 1
                self._record_activity()
                
                try:
                    await self._handle_message(message)
                    self._messages_processed += 1
                except Exception as e:
                    self._record_error(f"Message handling error: {e}")
                    
        except websockets.ConnectionClosed as e:
            logger.warning(f"{self.api_name} WebSocket closed: {e}")
            self._state = ConnectionState.DISCONNECTED
            
            # Trigger reconnect
            asyncio.create_task(self._reconnect_loop())
            
        except Exception as e:
            self._record_error(f"Receive loop error: {e}")
            self._state = ConnectionState.FAILED
    
    async def _handle_message(self, message: str) -> None:
        """
        Handle incoming message. Override in subclass for custom handling.
        
        Args:
            message: Raw message string
        """
        # Call registered handlers
        for handler in self._message_handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(message)
                else:
                    handler(message)
            except Exception as e:
                logger.error(f"Message handler error: {e}")
    
    async def send(self, message: str) -> bool:
        """
        Send message through WebSocket.
        
        Args:
            message: Message to send
            
        Returns:
            True if sent successfully
        """
        if not self._ws or self._state != ConnectionState.CONNECTED:
            logger.warning(f"{self.api_name} cannot send - not connected")
            return False
        
        try:
            await self._ws.send(message)
            return True
        except Exception as e:
            self._record_error(f"Send error: {e}")
            return False
