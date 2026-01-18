"""
Flint's Whale Intelligence System - Configuration Module

Purpose:
    Load and validate environment variables with fail-closed behavior.
    All API keys and thresholds are validated at startup.

Inputs:
    .env file in project root

Outputs:
    Typed Config object with validated settings

Failure Modes:
    - Missing critical API key: Raises ConfigError, system won't start
    - Invalid type: Raises ConfigError with specific field
    - Missing .env file: Raises ConfigError

Logging:
    - INFO: Successful config load with key presence (not values)
    - ERROR: Missing or invalid configuration

Usage:
    from config import config

    # Access validated settings
    api_key = config.whale_alert.api_key
    threshold = config.thresholds.whale_min_usd
"""

import os
import sys
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, List
from datetime import datetime, timezone

# =============================================================================
# EXCEPTIONS
# =============================================================================


class ConfigError(Exception):
    """Raised when configuration is invalid or missing."""

    pass


# =============================================================================
# CONFIGURATION DATACLASSES
# =============================================================================


@dataclass(frozen=True)
class WhaleAlertConfig:
    """Whale Alert API configuration."""

    api_key: str
    ws_url: str = "wss://ws.whale-alert.io"
    rest_url: str = "https://api.whale-alert.io/v1"

    def validate(self) -> List[str]:
        errors = []
        if not self.api_key:
            errors.append("WHALE_ALERT_API_KEY is required (trigger layer)")
        if len(self.api_key) < 10:
            errors.append("WHALE_ALERT_API_KEY appears invalid (too short)")
        return errors


@dataclass(frozen=True)
class AlchemyConfig:
    """Alchemy API configuration."""

    api_key: str
    ws_url: str
    http_url: str
    cu_monthly: int = 30_000_000
    cu_per_sec: int = 330

    def validate(self) -> List[str]:
        errors = []
        # Alchemy is optional until Phase 4
        if self.api_key and len(self.api_key) < 10:
            errors.append("ALCHEMY_API_KEY appears invalid")
        return errors


@dataclass(frozen=True)
class EtherscanConfig:
    """Etherscan API configuration."""

    api_key: str
    api_url: str = "https://api.etherscan.io/v2/api"
    rps: int = 5
    daily_limit: int = 100_000

    def validate(self) -> List[str]:
        errors = []
        if not self.api_key:
            errors.append("ETHERSCAN_API_KEY is required (metadata layer)")
        return errors


@dataclass(frozen=True)
class MoralisConfig:
    """Moralis API configuration."""

    api_key: str
    api_url: str = "https://deep-index.moralis.io/api/v2.2"
    cu_daily: int = 40_000

    def validate(self) -> List[str]:
        errors = []
        if not self.api_key:
            errors.append("MORALIS_API_KEY is required (cross-chain layer)")
        return errors


@dataclass(frozen=True)
class DuneConfig:
    """Dune Analytics API configuration."""

    api_key: str
    api_url: str = "https://api.dune.com/api/v1"
    credits_monthly: int = 2_500

    def validate(self) -> List[str]:
        errors = []
        if not self.api_key:
            errors.append("DUNE_API_KEY is required (forensics layer)")
        return errors


@dataclass(frozen=True)
class TokenMetricsConfig:
    """Token Metrics API configuration."""

    api_key: str
    api_url: str = "https://api.tokenmetrics.com/v1"
    monthly_limit: int = 500

    def validate(self) -> List[str]:
        errors = []
        if not self.api_key:
            errors.append("TOKEN_METRICS_API_KEY is required (sentiment layer)")
        return errors


@dataclass(frozen=True)
class CoinGeckoConfig:
    """CoinGecko API configuration."""

    api_key: str
    api_url: str = "https://api.coingecko.com/api/v3"
    rpm: int = 30

    def validate(self) -> List[str]:
        # CoinGecko works without key, just rate limited
        return []


@dataclass(frozen=True)
class DiscordConfig:
    """Discord webhook configuration."""

    webhook_whale: str
    webhook_alpha: str

    def validate(self) -> List[str]:
        errors = []
        if not self.webhook_whale:
            errors.append("DISCORD_WEBHOOK_WHALE recommended for alerts")
        return errors


@dataclass(frozen=True)
class DatabaseConfig:
    """PostgreSQL database configuration."""

    host: str
    port: int
    name: str
    user: str
    password: str
    pool_size: int = 5
    pool_overflow: int = 10

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    def validate(self) -> List[str]:
        errors = []
        if not self.host:
            errors.append("DB_HOST is required")
        if not self.name:
            errors.append("DB_NAME is required")
        return errors


@dataclass(frozen=True)
class ThresholdsConfig:
    """Trading and alerting thresholds."""

    whale_min_usd: float = 500_000
    mega_whale_usd: float = 10_000_000
    sentiment_min_usd: float = 5_000_000
    alpha_alert_threshold: float = 5.0
    alpha_log_threshold: float = 2.0

    def validate(self) -> List[str]:
        errors = []
        if self.whale_min_usd <= 0:
            errors.append("WHALE_MIN_USD must be positive")
        if self.mega_whale_usd <= self.whale_min_usd:
            errors.append("MEGA_WHALE_USD must be > WHALE_MIN_USD")
        return errors


@dataclass(frozen=True)
class SystemConfig:
    """System-wide settings."""

    log_level: str = "INFO"
    log_format: str = "json"
    data_dir: Path = field(
        default_factory=lambda: Path.home() / "Data_Vault" / "Whale_Intel"
    )

    def validate(self) -> List[str]:
        errors = []
        if self.log_level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            errors.append(f"LOG_LEVEL '{self.log_level}' is invalid")
        return errors


# =============================================================================
# MAIN CONFIGURATION CLASS
# =============================================================================


@dataclass
class Config:
    """
    Master configuration container.
    All sub-configs are validated on load.
    """

    whale_alert: WhaleAlertConfig
    alchemy: AlchemyConfig
    etherscan: EtherscanConfig
    moralis: MoralisConfig
    dune: DuneConfig
    token_metrics: TokenMetricsConfig
    coingecko: CoinGeckoConfig
    discord: DiscordConfig
    database: DatabaseConfig
    thresholds: ThresholdsConfig
    system: SystemConfig

    # Metadata
    loaded_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    env_file: Optional[Path] = None

    def validate_all(self) -> List[str]:
        """Validate all sub-configurations. Returns list of errors."""
        all_errors = []
        all_errors.extend(self.whale_alert.validate())
        all_errors.extend(self.alchemy.validate())
        all_errors.extend(self.etherscan.validate())
        all_errors.extend(self.moralis.validate())
        all_errors.extend(self.dune.validate())
        all_errors.extend(self.token_metrics.validate())
        all_errors.extend(self.coingecko.validate())
        all_errors.extend(self.discord.validate())
        all_errors.extend(self.database.validate())
        all_errors.extend(self.thresholds.validate())
        all_errors.extend(self.system.validate())
        return all_errors

    def log_status(self, logger: logging.Logger) -> None:
        """Log configuration status (keys present, not values)."""
        logger.info(
            "Configuration loaded",
            extra={
                "loaded_at": self.loaded_at.isoformat(),
                "env_file": str(self.env_file) if self.env_file else "environment",
                "whale_alert_key": "present" if self.whale_alert.api_key else "MISSING",
                "alchemy_key": "present" if self.alchemy.api_key else "not set",
                "etherscan_key": "present" if self.etherscan.api_key else "MISSING",
                "moralis_key": "present" if self.moralis.api_key else "MISSING",
                "dune_key": "present" if self.dune.api_key else "MISSING",
                "token_metrics_key": "present"
                if self.token_metrics.api_key
                else "MISSING",
                "coingecko_key": "present" if self.coingecko.api_key else "optional",
                "discord_whale": "present" if self.discord.webhook_whale else "not set",
                "discord_alpha": "present" if self.discord.webhook_alpha else "not set",
                "db_host": self.database.host,
                "db_name": self.database.name,
                "data_dir": str(self.system.data_dir),
            },
        )


# =============================================================================
# ENVIRONMENT LOADING UTILITIES
# =============================================================================


def _get_env(key: str, default: str = "") -> str:
    """Get environment variable with default."""
    return os.environ.get(key, default).strip()


def _get_env_int(key: str, default: int) -> int:
    """Get environment variable as integer."""
    val = _get_env(key, str(default))
    try:
        return int(val)
    except ValueError:
        raise ConfigError(f"{key}='{val}' is not a valid integer")


def _get_env_float(key: str, default: float) -> float:
    """Get environment variable as float."""
    val = _get_env(key, str(default))
    try:
        return float(val)
    except ValueError:
        raise ConfigError(f"{key}='{val}' is not a valid float")


def _load_dotenv(env_path: Path) -> None:
    """Load .env file into environment variables."""
    if not env_path.exists():
        raise ConfigError(f".env file not found at {env_path}")

    with open(env_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()

            # Skip comments and empty lines
            if not line or line.startswith("#"):
                continue

            # Parse KEY=VALUE
            if "=" not in line:
                continue

            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()

            # Remove quotes if present
            if value and value[0] in ('"', "'") and value[-1] == value[0]:
                value = value[1:-1]

            # Set in environment (don't override existing)
            if key and key not in os.environ:
                os.environ[key] = value


def load_config(env_path: Optional[Path] = None) -> Config:
    """
    Load configuration from .env file and environment.

    Args:
        env_path: Path to .env file. If None, searches project root.

    Returns:
        Validated Config object.

    Raises:
        ConfigError: If configuration is invalid or missing required keys.
    """
    # Find .env file
    if env_path is None:
        # Look in same directory as this file
        env_path = Path(__file__).parent / ".env"

    # Load .env file
    if env_path.exists():
        _load_dotenv(env_path)

    # Build configuration
    config = Config(
        whale_alert=WhaleAlertConfig(
            api_key=_get_env("WHALE_ALERT_API_KEY"),
            ws_url=_get_env("WHALE_ALERT_WS_URL", "wss://ws.whale-alert.io"),
        ),
        alchemy=AlchemyConfig(
            api_key=_get_env("ALCHEMY_API_KEY"),
            ws_url=_get_env("ALCHEMY_WS_URL"),
            http_url=_get_env("ALCHEMY_HTTP_URL"),
            cu_monthly=_get_env_int("ALCHEMY_CU_MONTHLY", 30_000_000),
            cu_per_sec=_get_env_int("ALCHEMY_CU_PER_SEC", 330),
        ),
        etherscan=EtherscanConfig(
            api_key=_get_env("ETHERSCAN_API_KEY"),
            api_url=_get_env("ETHERSCAN_API_URL", "https://api.etherscan.io/v2/api"),
            rps=_get_env_int("ETHERSCAN_RPS", 5),
            daily_limit=_get_env_int("ETHERSCAN_DAILY", 100_000),
        ),
        moralis=MoralisConfig(
            api_key=_get_env("MORALIS_API_KEY"),
            api_url=_get_env(
                "MORALIS_API_URL", "https://deep-index.moralis.io/api/v2.2"
            ),
            cu_daily=_get_env_int("MORALIS_CU_DAILY", 40_000),
        ),
        dune=DuneConfig(
            api_key=_get_env("DUNE_API_KEY"),
            api_url=_get_env("DUNE_API_URL", "https://api.dune.com/api/v1"),
            credits_monthly=_get_env_int("DUNE_CREDITS_MONTHLY", 2_500),
        ),
        token_metrics=TokenMetricsConfig(
            api_key=_get_env("TOKEN_METRICS_API_KEY"),
            api_url=_get_env(
                "TOKEN_METRICS_API_URL", "https://api.tokenmetrics.com/v1"
            ),
            monthly_limit=_get_env_int("TOKEN_METRICS_MONTHLY", 500),
        ),
        coingecko=CoinGeckoConfig(
            api_key=_get_env("COINGECKO_API_KEY"),
            api_url=_get_env("COINGECKO_API_URL", "https://api.coingecko.com/api/v3"),
            rpm=_get_env_int("COINGECKO_RPM", 30),
        ),
        discord=DiscordConfig(
            webhook_whale=_get_env("DISCORD_WEBHOOK_WHALE"),
            webhook_alpha=_get_env("DISCORD_WEBHOOK_ALPHA"),
        ),
        database=DatabaseConfig(
            host=_get_env("DB_HOST", "localhost"),
            port=_get_env_int("DB_PORT", 5432),
            name=_get_env("DB_NAME", "whale_intel"),
            user=_get_env("DB_USER", "postgres"),
            password=_get_env("DB_PASSWORD"),
            pool_size=_get_env_int("DB_POOL_SIZE", 5),
            pool_overflow=_get_env_int("DB_POOL_OVERFLOW", 10),
        ),
        thresholds=ThresholdsConfig(
            whale_min_usd=_get_env_float("WHALE_MIN_USD", 500_000),
            mega_whale_usd=_get_env_float("MEGA_WHALE_USD", 10_000_000),
            sentiment_min_usd=_get_env_float("SENTIMENT_MIN_USD", 5_000_000),
            alpha_alert_threshold=_get_env_float("ALPHA_ALERT_THRESHOLD", 5.0),
            alpha_log_threshold=_get_env_float("ALPHA_LOG_THRESHOLD", 2.0),
        ),
        system=SystemConfig(
            log_level=_get_env("LOG_LEVEL", "INFO"),
            log_format=_get_env("LOG_FORMAT", "json"),
            data_dir=Path(
                _get_env("DATA_DIR", str(Path.home() / "Data_Vault" / "Whale_Intel"))
            ),
        ),
        env_file=env_path if env_path.exists() else None,
    )

    # Validate
    errors = config.validate_all()
    if errors:
        # Log all errors before raising
        for err in errors:
            print(f"CONFIG ERROR: {err}", file=sys.stderr)
        raise ConfigError(
            f"Configuration validation failed with {len(errors)} error(s)"
        )

    return config


# =============================================================================
# MODULE-LEVEL SINGLETON
# =============================================================================

# Lazy-loaded singleton
_config: Optional[Config] = None


def get_config() -> Config:
    """Get the global configuration singleton."""
    global _config
    if _config is None:
        _config = load_config()
    return _config


# Convenience alias
config = property(lambda self: get_config())


# =============================================================================
# CLI TEST
# =============================================================================

if __name__ == "__main__":
    """Test configuration loading."""
    import json

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
    )
    logger = logging.getLogger(__name__)

    print("=" * 60)
    print("FLINT CONFIG LOADER TEST")
    print("=" * 60)

    try:
        cfg = load_config()
        print("\n✅ Configuration loaded successfully!\n")
        cfg.log_status(logger)

        print("\n--- Key Status ---")
        print(f"  Whale Alert:   {'✅' if cfg.whale_alert.api_key else '❌'}")
        print(
            f"  Alchemy:       {'✅' if cfg.alchemy.api_key else '⚠️  (optional until Phase 4)'}"
        )
        print(f"  Etherscan:     {'✅' if cfg.etherscan.api_key else '❌'}")
        print(f"  Moralis:       {'✅' if cfg.moralis.api_key else '❌'}")
        print(f"  Dune:          {'✅' if cfg.dune.api_key else '❌'}")
        print(f"  Token Metrics: {'✅' if cfg.token_metrics.api_key else '❌'}")
        print(
            f"  CoinGecko:     {'✅' if cfg.coingecko.api_key else '⚠️  (works without)'}"
        )
        print(
            f"  Discord:       {'✅' if cfg.discord.webhook_whale else '⚠️  (alerts disabled)'}"
        )

        print("\n--- Thresholds ---")
        print(f"  Whale Min:     ${cfg.thresholds.whale_min_usd:,.0f}")
        print(f"  Mega Whale:    ${cfg.thresholds.mega_whale_usd:,.0f}")
        print(f"  Sentiment Min: ${cfg.thresholds.sentiment_min_usd:,.0f}")
        print(f"  Alpha Alert:   {cfg.thresholds.alpha_alert_threshold}")

        print("\n--- Rate Limits ---")
        print(
            f"  Alchemy:       {cfg.alchemy.cu_monthly:,} CU/month, {cfg.alchemy.cu_per_sec} CU/sec"
        )
        print(
            f"  Etherscan:     {cfg.etherscan.rps} RPS, {cfg.etherscan.daily_limit:,}/day"
        )
        print(f"  Moralis:       {cfg.moralis.cu_daily:,} CU/day")
        print(f"  Dune:          {cfg.dune.credits_monthly:,} credits/month")
        print(f"  Token Metrics: {cfg.token_metrics.monthly_limit}/month")

        print("\n" + "=" * 60)
        print("CONFIG TEST COMPLETE")
        print("=" * 60)

    except ConfigError as e:
        print(f"\n❌ Configuration Error: {e}")
        sys.exit(1)
