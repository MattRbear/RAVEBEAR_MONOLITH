"""
Health monitoring for multi-venue collector - WITH CRITICAL METRICS.

3 metrics that catch 90% of "quiet failures":
1. save_lag_sec - Time since last actual write (not receive)
2. queue_depth - Pending candles waiting for validation
3. dup_dropped - Deduplication drops (where bugs hide)
"""
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional


@dataclass
class VenueHealth:
    """Health metrics for a single venue."""
    name: str
    ws_connected: bool = False
    last_candle_time_ms: int = 0
    last_save_time: float = 0  # NEW: When we last wrote to disk
    candles_received: int = 0
    candles_written: int = 0
    candles_rejected: int = 0
    dup_dropped: int = 0  # NEW: Duplicates dropped
    queue_depth: int = 0  # NEW: Pending in validator buffer
    errors: int = 0
    rest_requests: int = 0
    rest_failures: int = 0
    backfill_active: bool = False
    backfill_progress: str = ""
    reconnects: int = 0
    last_reconnect_time: float = 0


class HealthMonitor:
    """Monitor health across all venues with CRITICAL METRICS."""
    
    STALE_THRESHOLD_MS = 10 * 60 * 1000  # 10 minutes
    SAVE_LAG_WARN_SEC = 120  # Warn if no saves for 2 minutes
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.venues: Dict[str, VenueHealth] = {}
        self.start_time = time.time()
        self.last_status_time = 0
        self.last_error_counts: Dict[str, int] = {}
    
    def register_venue(self, venue_name: str):
        """Register a venue for monitoring."""
        self.venues[venue_name] = VenueHealth(name=venue_name)
        self.last_error_counts[venue_name] = 0
    
    def update_ws_connected(self, venue: str, connected: bool):
        """Update WebSocket connection status."""
        if venue in self.venues:
            was_connected = self.venues[venue].ws_connected
            self.venues[venue].ws_connected = connected
            
            if connected and not was_connected:
                self.logger.info("event=ws_connected venue=%s", venue)
            elif not connected and was_connected:
                self.venues[venue].reconnects += 1
                self.venues[venue].last_reconnect_time = time.time()
    
    def update_candle_received(self, venue: str, open_time_ms: int):
        """Record candle received."""
        if venue in self.venues:
            self.venues[venue].candles_received += 1
            self.venues[venue].last_candle_time_ms = max(
                self.venues[venue].last_candle_time_ms, 
                open_time_ms
            )
    
    def update_candle_written(self, venue: str, count: int = 1):
        """Record candles written to storage."""
        if venue in self.venues:
            self.venues[venue].candles_written += count
            self.venues[venue].last_save_time = time.time()  # Track actual save time
    
    def update_dup_dropped(self, venue: str, count: int = 1):
        """Record duplicate dropped (critical metric)."""
        if venue in self.venues:
            self.venues[venue].dup_dropped += count
    
    def update_queue_depth(self, venue: str, depth: int):
        """Update pending queue depth (critical metric)."""
        if venue in self.venues:
            self.venues[venue].queue_depth = depth
    
    def update_candle_rejected(self, venue: str, count: int = 1):
        """Record rejected candles."""
        if venue in self.venues:
            self.venues[venue].candles_rejected += count
    
    def update_error(self, venue: str):
        """Record an error."""
        if venue in self.venues:
            self.venues[venue].errors += 1
    
    def update_rest_request(self, venue: str, success: bool = True):
        """Record REST API request."""
        if venue in self.venues:
            self.venues[venue].rest_requests += 1
            if not success:
                self.venues[venue].rest_failures += 1
    
    def update_backfill_status(self, venue: str, active: bool, progress: str = ""):
        """Update backfill status."""
        if venue in self.venues:
            self.venues[venue].backfill_active = active
            self.venues[venue].backfill_progress = progress
    
    def get_save_lag(self, venue: str) -> float:
        """Get seconds since last save (critical metric)."""
        if venue not in self.venues:
            return 0
        v = self.venues[venue]
        if v.last_save_time == 0:
            return time.time() - self.start_time  # Never saved
        return time.time() - v.last_save_time
    
    def get_venue_status(self, venue: str) -> str:
        """Get human-readable status for a venue."""
        if venue not in self.venues:
            return "UNKNOWN"
        
        v = self.venues[venue]
        now_ms = int(time.time() * 1000)
        
        if not v.ws_connected:
            return "DISCONNECTED"
        
        if v.backfill_active:
            return f"BACKFILL {v.backfill_progress}"
        
        if v.last_candle_time_ms == 0:
            return "WAITING"
        
        age_ms = now_ms - v.last_candle_time_ms
        if age_ms > self.STALE_THRESHOLD_MS:
            age_min = age_ms // 60000
            return f"STALE ({age_min}m)"
        
        return "OK"
    
    def get_overall_status(self) -> str:
        """Get overall system status."""
        if not self.venues:
            return "NO VENUES"
        
        healthy = sum(1 for v in self.venues.values() if v.ws_connected)
        total = len(self.venues)
        
        if healthy == total:
            return "HEALTHY"
        elif healthy == 0:
            return "DOWN"
        else:
            return f"DEGRADED ({healthy}/{total})"
    
    def log_status(self, force: bool = False):
        """Print clean status display with CRITICAL METRICS."""
        now = time.time()
        
        if not force and (now - self.last_status_time) < 30:
            return
        
        self.last_status_time = now
        uptime_min = (now - self.start_time) / 60
        
        lines = []
        lines.append("")
        lines.append("=" * 80)
        lines.append(f"  STATUS: {self.get_overall_status()}  |  Uptime: {uptime_min:.0f} min  |  {datetime.now().strftime('%H:%M:%S')}")
        lines.append("=" * 80)
        lines.append(f"  {'VENUE':<10} {'STATUS':<12} {'RECV':>7} {'SAVED':>7} {'LAG':>6} {'QUEUE':>6} {'DUPS':>6} {'RECONN':>7}")
        lines.append("-" * 80)
        
        warnings = []
        
        for venue_name, v in self.venues.items():
            status = self.get_venue_status(venue_name)
            save_lag = self.get_save_lag(venue_name)
            
            # Status display
            if status == "OK":
                status_display = "[OK]"
            elif status.startswith("STALE"):
                status_display = f"[{status}]"
            elif status == "DISCONNECTED":
                status_display = "[DISC]"
            elif status == "WAITING":
                status_display = "[WAIT]"
            else:
                status_display = f"[{status[:8]}]"
            
            # Format save lag
            if save_lag > self.SAVE_LAG_WARN_SEC:
                lag_display = f"{save_lag:.0f}s!"
                warnings.append(f"{venue_name}: No saves for {save_lag:.0f}s")
            else:
                lag_display = f"{save_lag:.0f}s"
            
            lines.append(
                f"  {venue_name.upper():<10} {status_display:<12} "
                f"{v.candles_received:>7} {v.candles_written:>7} "
                f"{lag_display:>6} {v.queue_depth:>6} {v.dup_dropped:>6} {v.reconnects:>7}"
            )
        
        lines.append("=" * 80)
        
        # Show warnings
        if warnings:
            lines.append("  ⚠️  WARNINGS:")
            for w in warnings:
                lines.append(f"      {w}")
            lines.append("=" * 80)
        
        print("\n".join(lines))
        
        # Log new errors
        for venue_name, v in self.venues.items():
            if v.errors > self.last_error_counts.get(venue_name, 0):
                new_errors = v.errors - self.last_error_counts[venue_name]
                self.logger.warning("venue=%s new_errors=%d total_errors=%d", 
                                  venue_name, new_errors, v.errors)
                self.last_error_counts[venue_name] = v.errors
    
    def get_metrics_dict(self) -> dict:
        """Get all metrics as dict (for JSON export)."""
        return {
            venue_name: {
                "status": self.get_venue_status(venue_name),
                "ws_connected": v.ws_connected,
                "candles_received": v.candles_received,
                "candles_written": v.candles_written,
                "save_lag_sec": self.get_save_lag(venue_name),
                "queue_depth": v.queue_depth,
                "dup_dropped": v.dup_dropped,
                "reconnects": v.reconnects,
                "errors": v.errors,
            }
            for venue_name, v in self.venues.items()
        }
