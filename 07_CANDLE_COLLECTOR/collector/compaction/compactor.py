"""
Crash-safe Parquet compaction with deterministic deduplication.
Merges small append-only files into larger partitions with PK deduplication.

Race-safety: Only compacts CLOSED partitions (previous hour/day) to avoid
conflicts with active writers.
"""
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

import pyarrow as pa
import pyarrow.parquet as pq

from ..schemas import CANDLE_SCHEMA
from ..utils.path_sanitizer import sanitize_and_resolve, PathSanitizationError


class ParquetCompactor:
    """Compact and deduplicate Parquet files."""
    
    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        self.logger = logging.getLogger(__name__)
    
    def compact_partition(
        self,
        venue: str,
        symbol: str,
        timeframe: str,
        year: int,
        month: int,
        day: int,
        force: bool = False
    ) -> Dict[str, Any]:
        """
        Compact all files in a day partition.
        
        Race-safety: By default, only compacts CLOSED partitions (not current day).
        Set force=True to override (use with caution).
        
        Process:
        1. Check if partition is closed (not current day)
        2. Sanitize paths
        3. Read all .parquet files in partition
        4. Deduplicate by PK: (venue, symbol, timeframe, open_time_ms)
        5. Sort by open_time_ms (stable sort)
        6. Write to temp file
        7. Verify row count and compute hash
        8. Atomic rename
        9. Delete source files
        10. Write manifest
        
        Returns:
            Manifest dict with stats
        """
        # Race-safety check: don't compact current day unless forced
        if not force:
            now = datetime.now(timezone.utc)
            is_current_day = (
                year == now.year and
                month == now.month and
                day == now.day
            )
            
            if is_current_day:
                self.logger.info(
                    "event=skip_current_day venue=%s symbol=%s timeframe=%s partition=%04d-%02d-%02d",
                    venue, symbol, timeframe, year, month, day
                )
                return {}
        
        # Sanitize path components
        try:
            partition_path = sanitize_and_resolve(
                self.data_dir,
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
            return {}
        
        if not partition_path.exists():
            self.logger.warning(
                "event=partition_not_found path=%s",
                partition_path
            )
            return {}
        
        # Find all parquet files (exclude temp files and manifests)
        parquet_files = [
            f for f in partition_path.glob("*.parquet")
            if not f.name.endswith(".tmp") and not f.name.startswith("_")
        ]
        
        if not parquet_files:
            self.logger.info(
                "event=no_files_to_compact path=%s",
                partition_path
            )
            return {}
        
        # If only one file and it's already compacted, skip
        if len(parquet_files) == 1 and parquet_files[0].name.startswith("part-compacted-"):
            self.logger.info(
                "event=already_compacted path=%s",
                partition_path
            )
            return {}
        
        self.logger.info(
            "event=compaction_start venue=%s symbol=%s timeframe=%s partition=%04d-%02d-%02d files=%d",
            venue, symbol, timeframe, year, month, day, len(parquet_files)
        )
        
        # Step 1: Read all files
        tables = []
        for file_path in parquet_files:
            try:
                table = pq.read_table(file_path)
                tables.append(table)
            except Exception as exc:
                self.logger.error(
                    "event=read_error file=%s error=%s",
                    file_path, exc
                )
                raise
        
        # Concatenate all tables
        combined_table = pa.concat_tables(tables)
        df = combined_table.to_pandas()
        
        initial_rows = len(df)
        self.logger.info("event=read_complete rows=%d", initial_rows)
        
        # Step 2: Deduplicate by PK
        # Keep last occurrence (most recent data)
        df = df.drop_duplicates(
            subset=["venue", "symbol", "timeframe", "open_time_ms"],
            keep="last"
        )
        
        deduped_rows = len(df)
        duplicates_removed = initial_rows - deduped_rows
        
        if duplicates_removed > 0:
            self.logger.warning(
                "event=duplicates_removed count=%d",
                duplicates_removed
            )
        
        # Step 3: Sort by open_time_ms (stable sort)
        df = df.sort_values("open_time_ms", kind="stable")
        
        # Convert back to PyArrow table
        compacted_table = pa.Table.from_pandas(df, schema=CANDLE_SCHEMA)
        
        # Step 4: Write to temp file
        timestamp = int(df.iloc[0]["open_time_ms"]) if len(df) > 0 else 0
        temp_path = partition_path / f"part-compacted-{timestamp}.parquet.tmp"
        final_path = partition_path / f"part-compacted-{timestamp}.parquet"
        
        try:
            pq.write_table(compacted_table, temp_path, compression="snappy")
        except Exception as exc:
            self.logger.error("event=write_error error=%s", exc)
            if temp_path.exists():
                temp_path.unlink()
            raise
        
        # Step 5: Verify row count and compute hash
        verify_table = pq.read_table(temp_path)
        verify_rows = verify_table.num_rows
        
        if verify_rows != deduped_rows:
            self.logger.error(
                "event=verification_failed expected=%d actual=%d",
                deduped_rows, verify_rows
            )
            temp_path.unlink()
            raise ValueError(f"Row count mismatch: {deduped_rows} != {verify_rows}")
        
        # Compute SHA256 hash of file
        hasher = hashlib.sha256()
        with open(temp_path, "rb") as f:
            while chunk := f.read(8192):
                hasher.update(chunk)
        file_hash = hasher.hexdigest()
        
        # Step 6: Atomic rename
        temp_path.rename(final_path)
        
        # Step 7: Delete source files
        for file_path in parquet_files:
            try:
                file_path.unlink()
                self.logger.debug("event=source_deleted file=%s", file_path.name)
            except Exception as exc:
                self.logger.warning(
                    "event=delete_failed file=%s error=%s",
                    file_path.name, exc
                )
        
        # Step 8: Write manifest
        min_time = int(df["open_time_ms"].min()) if len(df) > 0 else 0
        max_time = int(df["open_time_ms"].max()) if len(df) > 0 else 0
        
        manifest = {
            "venue": venue,
            "symbol": symbol,
            "timeframe": timeframe,
            "partition": f"{year:04d}-{month:02d}-{day:02d}",
            "row_count": deduped_rows,
            "min_time_ms": min_time,
            "max_time_ms": max_time,
            "sha256": file_hash,
            "source_files": len(parquet_files),
            "duplicates_removed": duplicates_removed,
            "compacted_file": final_path.name
        }
        
        manifest_path = partition_path / "_manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        
        self.logger.info(
            "event=compaction_complete rows=%d duplicates=%d hash=%s",
            deduped_rows, duplicates_removed, file_hash[:16]
        )
        
        return manifest
    
    def compact_all_partitions(
        self,
        venue: str,
        symbol: str,
        timeframe: str,
        max_partitions: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Compact all partitions for a given venue/symbol/timeframe.
        
        Args:
            venue: Venue name
            symbol: Symbol name
            timeframe: Timeframe string
            max_partitions: Maximum number of partitions to compact
            
        Returns:
            List of manifests
        """
        base_path = self.data_dir / venue / symbol / timeframe
        
        if not base_path.exists():
            return []
        
        manifests = []
        partition_count = 0
        
        # Find all day partitions
        for year_dir in sorted(base_path.glob("year=*")):
            year = int(year_dir.name.split("=")[1])
            
            for month_dir in sorted(year_dir.glob("month=*")):
                month = int(month_dir.name.split("=")[1])
                
                for day_dir in sorted(month_dir.glob("day=*")):
                    day = int(day_dir.name.split("=")[1])
                    
                    if partition_count >= max_partitions:
                        self.logger.info(
                            "event=max_partitions_reached limit=%d",
                            max_partitions
                        )
                        return manifests
                    
                    try:
                        manifest = self.compact_partition(
                            venue, symbol, timeframe,
                            year, month, day
                        )
                        if manifest:
                            manifests.append(manifest)
                        partition_count += 1
                    except Exception as exc:
                        self.logger.error(
                            "event=compaction_error partition=%04d-%02d-%02d error=%s",
                            year, month, day, exc
                        )
        
        return manifests
