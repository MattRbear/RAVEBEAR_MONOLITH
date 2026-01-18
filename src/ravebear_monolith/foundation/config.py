"""Application configuration contract with strict Pydantic v2 validation."""

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, PositiveInt, ValidationError

from ravebear_monolith.util.errors import ConfigError


class AppConfig(BaseModel, strict=True, extra="forbid"):
    """Application configuration with strict type validation.

    All fields have sensible defaults for development.
    """

    app_name: str = "RAVEBEAR_MONOLITH"
    environment: Literal["dev", "prod"] = "dev"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    shutdown_timeout_s: PositiveInt = 5
    heartbeat_interval_s: PositiveInt = 5

    # Health check settings
    data_dir: Path = Path("data")
    kill_switch_path: Path = Path("config/kill_switch.txt")
    min_free_disk_mb: PositiveInt = 2048
    min_python_major: PositiveInt = 3
    min_python_minor: PositiveInt = 12


def load_config(path: Path) -> AppConfig:
    """Load and validate configuration from a YAML file.

    Args:
        path: Path to the YAML configuration file.

    Returns:
        Validated AppConfig instance.

    Raises:
        ConfigError: If file is missing, unreadable, or contains invalid data.
    """
    if not path.exists():
        raise ConfigError(f"Configuration file not found: {path}")

    try:
        with open(path, encoding="utf-8") as f:
            raw_data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML in configuration file: {e}") from e
    except OSError as e:
        raise ConfigError(f"Cannot read configuration file: {e}") from e

    # Handle empty YAML files
    if raw_data is None:
        raw_data = {}

    if not isinstance(raw_data, dict):
        raise ConfigError(f"Configuration must be a mapping, got {type(raw_data).__name__}")

    try:
        return AppConfig.model_validate(raw_data)
    except ValidationError as e:
        raise ConfigError(f"Configuration validation failed: {e}") from e
