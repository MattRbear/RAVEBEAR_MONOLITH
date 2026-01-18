"""
INPUT VALIDATORS - Security & Sanitization
===========================================
Validate and sanitize all user inputs.
"""

import re
from typing import List


class ValidationError(Exception):
    """Raised when input validation fails."""
    pass


def validate_symbol(symbol: str) -> str:
    """
    Validate and sanitize symbol input.
    Only allows alphanumeric characters and dashes.
    """
    if not symbol:
        raise ValidationError("Symbol cannot be empty")
    
    # Only allow alphanumeric and dash
    if not re.match(r'^[A-Z0-9-]+$', symbol.upper()):
        raise ValidationError(f"Invalid symbol: {symbol}. Only alphanumeric and dash allowed.")
    
    if len(symbol) > 20:
        raise ValidationError(f"Symbol too long: {symbol}")
    
    return symbol.upper()


def validate_threshold(value: float, min_val: float = 0.0, max_val: float = 1.0) -> float:
    """Validate threshold is in valid range."""
    if not isinstance(value, (int, float)):
        raise ValidationError(f"Threshold must be a number, got {type(value)}")
    
    if not min_val <= value <= max_val:
        raise ValidationError(f"Threshold {value} must be between {min_val} and {max_val}")
    
    return float(value)


def validate_webhook_url(url: str) -> str:
    """Validate Discord webhook URL format."""
    if not url:
        return ""  # Empty is OK (optional)
    
    if not url.startswith("https://discord.com/api/webhooks/"):
        raise ValidationError(f"Invalid Discord webhook URL format")
    
    # Basic length check
    if len(url) > 200:
        raise ValidationError("Webhook URL too long")
    
    return url


def validate_symbols(symbols: List[str]) -> List[str]:
    """Validate list of symbols."""
    if not symbols:
        raise ValidationError("At least one symbol required")
    
    if len(symbols) > 50:
        raise ValidationError("Too many symbols (max 50)")
    
    return [validate_symbol(s) for s in symbols]


def validate_cooldown(seconds: float) -> float:
    """Validate cooldown period."""
    if not isinstance(seconds, (int, float)):
        raise ValidationError(f"Cooldown must be a number, got {type(seconds)}")
    
    if seconds < 0.1:
        raise ValidationError("Cooldown too short (min 0.1s)")
    
    if seconds > 3600:
        raise ValidationError("Cooldown too long (max 1 hour)")
    
    return float(seconds)


def validate_persistence(ticks: int) -> int:
    """Validate persistence tick count."""
    if not isinstance(ticks, int):
        raise ValidationError(f"Persistence must be an integer, got {type(ticks)}")
    
    if ticks < 1:
        raise ValidationError("Persistence must be at least 1")
    
    if ticks > 100:
        raise ValidationError("Persistence too high (max 100)")
    
    return ticks
