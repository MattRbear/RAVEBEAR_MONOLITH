"""
Per-venue health monitoring with state machine and failure escalation.

State transitions:
- HEALTHY → DEGRADED: >=3 failures in rolling 60m window
- DEGRADED → DOWN: >=10 failures in rolling 60m window
- DOWN → HEALTHY: 3 consecutive successes

Alerts only on state transitions (no spam).
"""
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional, Callable
from collections import deque


class HealthState(Enum):
    """Venue health states."""
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    DOWN = "DOWN"


class VenueHealthMonitor:
    """
    Monitor venue health with state machine and failure escalation.
    
    Tracks failures in rolling 60-minute window.
    Alerts only on state transitions.
    """
    
    def __init__(
        self,
        venue: str,
        degraded_threshold: int = 3,
        down_threshold: int = 10,
        recovery_consecutive_successes: int = 3,
        window_minutes: int = 60,
        alert_callback: Optional[Callable] = None,
        clock: Optional[Callable] = None  # For testing with fake clock
    ):
        self.venue = venue
        self.degraded_threshold = degraded_threshold
        self.down_threshold = down_threshold
        self.recovery_consecutive_successes = recovery_consecutive_successes
        self.window_ms = window_minutes * 60 * 1000
        self.alert_callback = alert_callback or self._default_alert
        self.clock = clock or self._utc_now_ms
        
        self.state = HealthState.HEALTHY
        self.failures: deque = deque()  # (timestamp_ms, reason)
        self.consecutive_successes = 0
        
        self.logger = logging.getLogger(__name__)
    
    def _utc_now_ms(self) -> int:
        """Get current UTC timestamp in milliseconds."""
        return int(datetime.now(timezone.utc).timestamp() * 1000)
    
    def _default_alert(self, level: str, venue: str, message: str):
        """Default alert handler (stdout)."""
        print(f"ALERT [{level}] {venue}: {message}")
    
    def record_failure(self, reason: str):
        """
        Record a failure and update state.
        
        Failures counted from:
        - RetryBudgetExhausted (REST)
        - WS disconnect/reconnect failures
        - Critical write failures
        """
        now_ms = self.clock()
        
        # Add failure to window
        self.failures.append((now_ms, reason))
        
        # Reset consecutive successes
        self.consecutive_successes = 0
        
        # Prune old failures outside window
        self._prune_old_failures(now_ms)
        
        # Check for state transition
        failure_count = len(self.failures)
        old_state = self.state
        
        if self.state == HealthState.HEALTHY:
            if failure_count >= self.degraded_threshold:
                self.state = HealthState.DEGRADED
                self._alert_state_change(old_state, self.state, failure_count, reason)
        
        elif self.state == HealthState.DEGRADED:
            if failure_count >= self.down_threshold:
                self.state = HealthState.DOWN
                self._alert_state_change(old_state, self.state, failure_count, reason)
        
        # Log failure
        self.logger.warning(
            "event=failure_recorded venue=%s state=%s failures=%d/%d reason=%s",
            self.venue, self.state.value, failure_count,
            self.down_threshold, reason
        )
    
    def record_success(self):
        """
        Record a success and update state.
        
        Recovery rule: 3 consecutive successes → HEALTHY
        """
        now_ms = self.clock()
        
        # Increment consecutive successes
        self.consecutive_successes += 1
        
        # Prune old failures
        self._prune_old_failures(now_ms)
        
        # Check for recovery
        old_state = self.state
        
        if self.state == HealthState.DOWN:
            if self.consecutive_successes >= self.recovery_consecutive_successes:
                self.state = HealthState.HEALTHY
                self.failures.clear()  # Clear failure history on recovery
                self._alert_state_change(
                    old_state, self.state,
                    0, f"{self.consecutive_successes} consecutive successes"
                )
                self.consecutive_successes = 0
        
        elif self.state == HealthState.DEGRADED:
            # Check if failures dropped below degraded threshold
            if len(self.failures) < self.degraded_threshold:
                self.state = HealthState.HEALTHY
                self._alert_state_change(
                    old_state, self.state,
                    len(self.failures), "failures below threshold"
                )
                self.consecutive_successes = 0
        
        # Log success
        self.logger.debug(
            "event=success_recorded venue=%s state=%s consecutive_successes=%d",
            self.venue, self.state.value, self.consecutive_successes
        )
    
    def _prune_old_failures(self, now_ms: int):
        """Remove failures outside rolling window."""
        cutoff_ms = now_ms - self.window_ms
        
        while self.failures and self.failures[0][0] < cutoff_ms:
            self.failures.popleft()
    
    def _alert_state_change(
        self,
        old_state: HealthState,
        new_state: HealthState,
        failure_count: int,
        reason: str
    ):
        """Send alert on state transition (single alert, no spam)."""
        # Determine alert level
        if new_state == HealthState.DOWN:
            level = "CRITICAL"
        elif new_state == HealthState.DEGRADED:
            level = "WARNING"
        else:  # HEALTHY
            level = "INFO"
        
        # Build message
        message = (
            f"State transition: {old_state.value} → {new_state.value} | "
            f"Failures: {failure_count}/{self.down_threshold} in {self.window_ms // 60000}m window | "
            f"Reason: {reason}"
        )
        
        # Send alert
        self.alert_callback(level, self.venue, message)
        
        # Log transition
        self.logger.info(
            "event=state_transition venue=%s old_state=%s new_state=%s failures=%d reason=%s",
            self.venue, old_state.value, new_state.value, failure_count, reason
        )
    
    def get_state(self) -> HealthState:
        """Get current health state."""
        return self.state
    
    def get_failure_count(self) -> int:
        """Get current failure count in window."""
        now_ms = self.clock()
        self._prune_old_failures(now_ms)
        return len(self.failures)
