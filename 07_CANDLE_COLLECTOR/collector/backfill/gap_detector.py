"""
Gap detection with caps, late tolerance, and persistent cooldown.

Prevents infinite backfill loops by:
1. Late tolerance: Don't backfill gaps within N intervals of "now"
2. Hard caps: Limit gaps per stream per run
3. Persistent cooldown: Track last backfill time on disk, enforce minimum interval
4. Convergence: Repeated gaps within cooldown are deferred, not retried
"""
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Tuple, Dict, Any

from ..utils.time import timeframe_to_ms


class GapDetectorState:
    """Persistent state for gap detector cooldowns."""
    
    def __init__(self, state_file: Path):
        self.state_file = Path(state_file)
        self.state: Dict[str, Any] = {}
        self._load()
    
    def _load(self):
        """Load state from disk."""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    self.state = json.load(f)
            except Exception:
                self.state = {}
    
    def _save(self):
        """Save state to disk."""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2)
    
    def get_last_backfill_time(self, stream_key: str) -> int:
        """Get last backfill timestamp (ms) for stream."""
        return self.state.get(stream_key, {}).get("last_backfill_ms", 0)
    
    def set_last_backfill_time(self, stream_key: str, timestamp_ms: int):
        """Set last backfill timestamp (ms) for stream."""
        if stream_key not in self.state:
            self.state[stream_key] = {}
        self.state[stream_key]["last_backfill_ms"] = timestamp_ms
        self._save()


class GapDetector:
    """
    Detect gaps in candle data with bounds and late tolerance.
    
    Caps:
    - late_grace_intervals: Don't backfill gaps within N intervals of now (default 3)
    - max_gaps_per_stream_per_run: Limit scheduled gaps per stream (default 10)
    - max_backfill_minutes_per_stream_per_run: Limit backfill duration (default 240)
    - cooldown_minutes: Minimum time between backfills for same stream (default 60)
    
    Convergence:
    - Gaps within cooldown are DEFERRED, not retried infinitely
    - State persisted to disk across restarts
    """
    
    def __init__(
        self,
        storage,
        state_file: Path = Path("data/.gap_detector_state.json"),
        late_grace_intervals: int = 3,
        max_gaps_per_stream_per_run: int = 10,
        max_backfill_minutes_per_stream_per_run: int = 240,
        cooldown_minutes: int = 5,  # Reduced from 60 for faster iteration
        lookback_days: int = 7  # Reduced from 30
    ):
        self.storage = storage
        self.state = GapDetectorState(state_file)
        self.late_grace_intervals = late_grace_intervals
        self.max_gaps_per_stream_per_run = max_gaps_per_stream_per_run
        self.max_backfill_minutes_per_stream_per_run = max_backfill_minutes_per_stream_per_run
        self.cooldown_minutes = cooldown_minutes
        self.lookback_days = lookback_days
        self.logger = logging.getLogger(__name__)
    
    def detect_gaps(
        self,
        venue: str,
        symbol: str,
        timeframe: str
    ) -> List[Tuple[int, int]]:
        """
        Detect gaps in candle data for a venue/symbol/timeframe.
        
        Returns list of (start_ms, end_ms) tuples representing gaps,
        subject to caps and late tolerance.
        """
        stream_key = f"{venue}/{symbol}/{timeframe}"
        
        # Check cooldown
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        last_backfill_ms = self.state.get_last_backfill_time(stream_key)
        cooldown_ms = self.cooldown_minutes * 60 * 1000
        
        if last_backfill_ms > 0 and (now_ms - last_backfill_ms) < cooldown_ms:
            remaining_ms = cooldown_ms - (now_ms - last_backfill_ms)
            remaining_min = remaining_ms // (60 * 1000)
            self.logger.info(
                "event=gap_detection_skipped_cooldown stream=%s remaining_minutes=%d",
                stream_key, remaining_min
            )
            return []
        
        # Get timeframe duration
        tf_ms = timeframe_to_ms(timeframe)
        
        # Calculate late tolerance cutoff
        late_cutoff_ms = now_ms - (self.late_grace_intervals * tf_ms)
        
        # Get last candle timestamp from storage
        last_timestamp = self.storage.read_last_timestamp(venue, symbol, timeframe)
        
        # Calculate lookback window
        lookback_ms = self.lookback_days * 24 * 60 * 60 * 1000
        earliest_ms = now_ms - lookback_ms
        
        if last_timestamp == 0:
            # No data exists, backfill from earliest to late_cutoff
            self.logger.info(
                "event=gap_detected_full stream=%s lookback_days=%d",
                stream_key, self.lookback_days
            )
            gaps = [(earliest_ms, late_cutoff_ms)]
        else:
            # Check if there's a gap between last timestamp and late_cutoff
            expected_next = last_timestamp + tf_ms
            
            if expected_next < late_cutoff_ms:
                gap_duration_ms = late_cutoff_ms - expected_next
                gap_duration_min = gap_duration_ms // (60 * 1000)
                
                self.logger.info(
                    "event=gap_detected stream=%s gap_minutes=%d",
                    stream_key, gap_duration_min
                )
                gaps = [(expected_next, late_cutoff_ms)]
            else:
                # No gap or gap is within late tolerance
                self.logger.debug(
                    "event=no_gap_detected stream=%s",
                    stream_key
                )
                return []
        
        # Apply caps
        gaps = self._apply_caps(gaps, stream_key, tf_ms)
        
        # Update state if gaps were scheduled
        if gaps:
            self.state.set_last_backfill_time(stream_key, now_ms)
        
        return gaps
    
    def _apply_caps(
        self,
        gaps: List[Tuple[int, int]],
        stream_key: str,
        tf_ms: int
    ) -> List[Tuple[int, int]]:
        """Apply hard caps to gap list."""
        if not gaps:
            return gaps
        
        # Cap 1: Max gaps per run
        if len(gaps) > self.max_gaps_per_stream_per_run:
            self.logger.warning(
                "event=gaps_capped_by_count stream=%s total=%d max=%d",
                stream_key, len(gaps), self.max_gaps_per_stream_per_run
            )
            gaps = gaps[:self.max_gaps_per_stream_per_run]
        
        # Cap 2: Max backfill duration
        max_backfill_ms = self.max_backfill_minutes_per_stream_per_run * 60 * 1000
        total_duration_ms = sum(end - start for start, end in gaps)
        
        if total_duration_ms > max_backfill_ms:
            # Truncate gaps to fit within max duration
            capped_gaps = []
            accumulated_ms = 0
            
            for start, end in gaps:
                gap_duration = end - start
                remaining_budget = max_backfill_ms - accumulated_ms
                
                if remaining_budget <= 0:
                    break
                
                if gap_duration <= remaining_budget:
                    capped_gaps.append((start, end))
                    accumulated_ms += gap_duration
                else:
                    # Partial gap to fit budget
                    capped_end = start + remaining_budget
                    capped_gaps.append((start, capped_end))
                    accumulated_ms += remaining_budget
                    break
            
            self.logger.warning(
                "event=gaps_capped_by_duration stream=%s original_minutes=%d max_minutes=%d",
                stream_key,
                total_duration_ms // (60 * 1000),
                self.max_backfill_minutes_per_stream_per_run
            )
            gaps = capped_gaps
        
        return gaps
