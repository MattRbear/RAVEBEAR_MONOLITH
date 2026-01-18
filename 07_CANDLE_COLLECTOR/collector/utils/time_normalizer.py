"""
Canonical time normalization for all venue adapters.
Ensures consistent UTC epoch milliseconds across all data sources.
"""
from datetime import datetime, timezone


def normalize_timestamp_seconds(timestamp_sec: int, timeframe: str) -> int:
    """
    Normalize Unix timestamp (seconds) to UTC epoch milliseconds.
    Ensures alignment to timeframe grid.
    
    Args:
        timestamp_sec: Unix timestamp in seconds
        timeframe: Timeframe string (1m, 5m, 15m, 1h, etc.)
        
    Returns:
        UTC epoch milliseconds, aligned to timeframe grid
    """
    # Convert to milliseconds
    timestamp_ms = timestamp_sec * 1000
    
    # Get timeframe duration in milliseconds
    from .time import timeframe_to_ms
    tf_ms = timeframe_to_ms(timeframe)
    
    # Align to grid (floor to nearest timeframe boundary)
    aligned_ms = (timestamp_ms // tf_ms) * tf_ms
    
    return aligned_ms


def normalize_iso8601(iso_string: str, timeframe: str) -> int:
    """
    Normalize ISO 8601 timestamp to UTC epoch milliseconds.
    Ensures alignment to timeframe grid.
    
    Args:
        iso_string: ISO 8601 timestamp (e.g., "2026-01-09T18:00:00.000000Z")
        timeframe: Timeframe string (1m, 5m, 15m, 1h, etc.)
        
    Returns:
        UTC epoch milliseconds, aligned to timeframe grid
    """
    # Parse ISO 8601 to datetime (handle Z suffix)
    if iso_string.endswith('Z'):
        iso_string = iso_string[:-1] + '+00:00'
    
    dt = datetime.fromisoformat(iso_string)
    
    # Ensure UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    
    # Convert to milliseconds
    timestamp_ms = int(dt.timestamp() * 1000)
    
    # Get timeframe duration in milliseconds
    from .time import timeframe_to_ms
    tf_ms = timeframe_to_ms(timeframe)
    
    # Align to grid (floor to nearest timeframe boundary)
    aligned_ms = (timestamp_ms // tf_ms) * tf_ms
    
    return aligned_ms


def validate_alignment(timestamp_ms: int, timeframe: str) -> bool:
    """
    Validate that timestamp is aligned to timeframe grid.
    
    Args:
        timestamp_ms: UTC epoch milliseconds
        timeframe: Timeframe string
        
    Returns:
        True if aligned, False otherwise
    """
    from .time import timeframe_to_ms
    tf_ms = timeframe_to_ms(timeframe)
    return timestamp_ms % tf_ms == 0
