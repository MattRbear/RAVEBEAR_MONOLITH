"""
Multi-venue configuration system.
Supports Coinbase, Kraken, and OKX with per-venue settings.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import yaml

from .utils.time import ALLOWED_TIMEFRAMES


@dataclass
class VenueConfig:
    """Configuration for a single venue."""
    enabled: bool = False
    rest_url: str = ""
    ws_url: str = ""
    symbols: List[str] = field(default_factory=list)
    rate_limit_per_sec: float = 5.0
    api_key: Optional[str] = None
    api_secret: Optional[str] = None


@dataclass
class StorageConfig:
    path: str = "data"
    schema_version: str = "2"


@dataclass
class GapDetectionConfig:
    enabled: bool = True
    lookback_days: int = 30
    backfill_chunk_size: int = 1000


@dataclass
class ValidationConfig:
    out_of_order_window: int = 5


@dataclass
class LoggingConfig:
    level: str = "INFO"
    file: str = "logs/collector.log"
    max_bytes: int = 10000000
    backup_count: int = 5


@dataclass
class AggregationConfig:
    enabled: bool = True
    base_timeframe: str = "1m"


@dataclass
class AppConfig:
    """Multi-venue application configuration."""
    venues: Dict[str, VenueConfig]
    timeframes: List[str]
    ws_timeframes: List[str]
    derive_timeframes: List[str]
    storage: StorageConfig = field(default_factory=StorageConfig)
    gap_detection: GapDetectionConfig = field(default_factory=GapDetectionConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    aggregation: AggregationConfig = field(default_factory=AggregationConfig)


def _validate_timeframes(timeframes):
    unknown = [tf for tf in timeframes if tf not in ALLOWED_TIMEFRAMES]
    if unknown:
        raise ValueError(f"Unsupported timeframes: {unknown}")


def load_config(path: str) -> AppConfig:
    """Load multi-venue configuration from YAML file."""
    with open(path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    # Parse venues
    venues_raw = raw.get("venues", {})
    venues = {}
    for venue_name, venue_data in venues_raw.items():
        if not isinstance(venue_data, dict):
            continue
        venues[venue_name] = VenueConfig(
            enabled=venue_data.get("enabled", False),
            rest_url=venue_data.get("rest_url", ""),
            ws_url=venue_data.get("ws_url", ""),
            symbols=list(venue_data.get("symbols", [])),
            rate_limit_per_sec=float(venue_data.get("rate_limit_per_sec", 5.0)),
            api_key=venue_data.get("api_key"),
            api_secret=venue_data.get("api_secret"),
        )

    # Validate at least one venue is enabled
    enabled_venues = [name for name, cfg in venues.items() if cfg.enabled]
    if not enabled_venues:
        raise ValueError("At least one venue must be enabled")

    # Parse timeframes
    timeframes = list(raw.get("timeframes", []))
    ws_timeframes = raw.get("ws_timeframes")
    derive_timeframes = list(raw.get("derive_timeframes", []))

    if not timeframes:
        raise ValueError("Config must include at least one timeframe")

    _validate_timeframes(timeframes)
    
    if ws_timeframes is None:
        ws_timeframes = list(timeframes)
    else:
        ws_timeframes = list(ws_timeframes)
        _validate_timeframes(ws_timeframes)

    if derive_timeframes:
        _validate_timeframes(derive_timeframes)

    # Parse other configs
    storage = StorageConfig(**(raw.get("storage", {})))
    gap_detection = GapDetectionConfig(**(raw.get("gap_detection", {})))
    validation = ValidationConfig(**(raw.get("validation", {})))
    logging_cfg = LoggingConfig(**(raw.get("logging", {})))
    
    aggregation_raw = raw.get("aggregation", {})
    if derive_timeframes and not aggregation_raw:
        aggregation = AggregationConfig()
    else:
        aggregation = AggregationConfig(**aggregation_raw)

    if derive_timeframes and aggregation.base_timeframe not in timeframes:
        raise ValueError("Base timeframe must be in timeframes when deriving")

    return AppConfig(
        venues=venues,
        timeframes=timeframes,
        ws_timeframes=ws_timeframes,
        derive_timeframes=derive_timeframes,
        storage=storage,
        gap_detection=gap_detection,
        validation=validation,
        logging=logging_cfg,
        aggregation=aggregation,
    )
