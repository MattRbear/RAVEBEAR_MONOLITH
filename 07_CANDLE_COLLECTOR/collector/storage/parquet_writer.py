"""
Multi-venue Parquet storage with venue-based partitioning.
Supports nullable venue-specific fields (vwap, trades_count, vol_ccy, etc.)
Includes path sanitization to prevent directory traversal attacks.
"""
import datetime
import logging
import os
from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq

from ..schemas import CANDLE_SCHEMA
from ..utils.path_sanitizer import sanitize_and_resolve, PathSanitizationError


class ParquetStorage:
    """Write candles to Parquet files with venue/symbol/timeframe partitioning."""
    
    def __init__(self, config):
        self.path = Path(config.path).resolve()  # Resolve to absolute path
        self.schema_version = config.schema_version
        self.logger = logging.getLogger(__name__)
        self.logger.info("event=storage_init path=%s schema_version=%s", 
                        self.path, self.schema_version)

    def write_candles(self, candles: List[dict]):
        """Write a batch of candles, grouped by venue/symbol/timeframe."""
        if not candles:
            return

        # Group by (venue, symbol, timeframe)
        by_key = {}
        for candle in candles:
            key = (candle["venue"], candle["symbol"], candle["timeframe"])
            by_key.setdefault(key, []).append(candle)

        for (venue, symbol, timeframe), batch in by_key.items():
            self._write_batch(venue, symbol, timeframe, batch)

    def _write_batch(self, venue: str, symbol: str, timeframe: str, candles: List[dict]):
        """Write a batch of candles for a specific venue/symbol/timeframe."""
        if not candles:
            return

        # Calculate partition from first candle's timestamp
        first = candles[0]
        open_time = first["open_time_ms"]
        
        # Convert milliseconds to date components
        dt = datetime.datetime.utcfromtimestamp(open_time / 1000.0)
        year = dt.year
        month = dt.month
        day = dt.day

        # Sanitize path components and build partition directory
        try:
            partition_dir = sanitize_and_resolve(
                self.path,
                venue,
                symbol,
                timeframe,
                f"year={year}",
                f"month={month:02d}",
                f"day={day:02d}"
            )
        except PathSanitizationError as exc:
            self.logger.error(
                "event=path_sanitization_error venue=%s symbol=%s timeframe=%s error=%s",
                venue, symbol, timeframe, exc
            )
            # CRITICAL: Send alert for security violation
            self.logger.critical(
                "SECURITY ALERT: Path sanitization violation detected! "
                "venue=%s symbol=%s timeframe=%s",
                venue, symbol, timeframe
            )
            return
        
        partition_dir.mkdir(parents=True, exist_ok=True)

        # Convert to PyArrow table using unified schema
        table = pa.Table.from_pylist(candles, schema=CANDLE_SCHEMA)
        
        # Write to temp file first, then atomic rename
        temp_path = partition_dir / f"part-{open_time}.parquet.tmp"
        final_path = partition_dir / f"part-{open_time}.parquet"

        pq.write_table(table, temp_path, compression="snappy")
        
        # Use os.replace() for atomic rename that works on Windows
        # (overwrites target if it exists, unlike Path.rename())
        os.replace(str(temp_path), str(final_path))

        self.logger.debug(
            "event=parquet_write venue=%s symbol=%s timeframe=%s count=%d path=%s",
            venue,
            symbol,
            timeframe,
            len(candles),
            final_path,
        )
    
    def read_last_timestamp(self, venue: str, symbol: str, timeframe: str) -> int:
        """
        Read the last candle timestamp for a given venue/symbol/timeframe.
        Returns 0 if no data exists.
        Used for gap detection.
        """
        # Sanitize path components
        try:
            venue_path = sanitize_and_resolve(self.path, venue, symbol, timeframe)
        except PathSanitizationError as exc:
            self.logger.error("event=path_sanitization_error error=%s", exc)
            return 0
        
        if not venue_path.exists():
            return 0
        
        try:
            # Read all parquet files and find max timestamp
            dataset = pq.ParquetDataset(venue_path, use_legacy_dataset=False)
            table = dataset.read(columns=["open_time_ms"])
            if table.num_rows == 0:
                return 0
            
            max_time = table.column("open_time_ms").to_pylist()
            return max(max_time) if max_time else 0
        except Exception as exc:
            self.logger.error("event=read_last_timestamp_error venue=%s symbol=%s timeframe=%s error=%s",
                            venue, symbol, timeframe, exc)
            return 0
