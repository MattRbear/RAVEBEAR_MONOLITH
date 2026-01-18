"""
Compact all partitions in the data directory.

Usage:
    python -m collector.compaction.compact_all [--force]
"""
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone
from collector.compaction.compactor import ParquetCompactor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s level=%(levelname)s msg=%(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Compact all partitions")
    parser.add_argument("--data-dir", type=str, default="data", help="Data directory")
    parser.add_argument("--force", action="store_true", help="Force compact current day")
    
    args = parser.parse_args()
    
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        return 1
    
    compactor = ParquetCompactor(data_dir)
    
    # Find all day partitions
    partitions = []
    for day_dir in data_dir.rglob("day=*"):
        if day_dir.is_dir():
            partitions.append(day_dir)
    
    logger.info(f"Found {len(partitions)} partitions")
    
    compacted = 0
    skipped = 0
    errors = 0
    
    for partition_dir in partitions:
        try:
            # Extract venue/symbol/timeframe from path
            parts = partition_dir.parts
            venue_idx = next(i for i, p in enumerate(parts) if p == "data") + 1
            
            venue = parts[venue_idx]
            symbol = parts[venue_idx + 1]
            timeframe = parts[venue_idx + 2]
            
            # Check if current day
            year = int(partition_dir.parent.parent.name.split("=")[1])
            month = int(partition_dir.parent.name.split("=")[1])
            day = int(partition_dir.name.split("=")[1])
            
            partition_date = datetime(year, month, day, tzinfo=timezone.utc).date()
            today = datetime.now(timezone.utc).date()
            
            if partition_date == today and not args.force:
                logger.info(f"Skipping current day: {venue}/{symbol}/{timeframe}/{partition_date}")
                skipped += 1
                continue
            
            # Compact
            logger.info(f"Compacting: {venue}/{symbol}/{timeframe}/{partition_date}")
            result = compactor.compact_partition(
                venue=venue,
                symbol=symbol,
                timeframe=timeframe,
                partition_path=partition_dir,
                force=args.force
            )
            
            if result:
                compacted += 1
                logger.info(f"  OK {result['source_files']} files -> 1 file ({result['row_count']} rows)")
            else:
                skipped += 1
        
        except Exception as exc:
            logger.error(f"Error compacting {partition_dir}: {exc}")
            errors += 1
    
    logger.info("=" * 80)
    logger.info(f"Compaction complete:")
    logger.info(f"  Compacted: {compacted}")
    logger.info(f"  Skipped: {skipped}")
    logger.info(f"  Errors: {errors}")
    logger.info("=" * 80)
    
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    exit(main())
