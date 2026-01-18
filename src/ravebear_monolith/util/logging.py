"""Structured JSON logging with correlation ID and redaction.

Provides:
- correlation_id contextvar for request tracing
- correlation_id_scope context manager for scoped IDs
- redact() for masking sensitive data
- configure_logging() for JSON log output
"""

import json
import logging
import re
import sys
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

from ravebear_monolith.foundation.config import AppConfig

# Correlation ID for request tracing
correlation_id: ContextVar[str] = ContextVar("correlation_id", default="root")

# Patterns for redaction
_BEARER_PATTERN = re.compile(r"(Authorization:\s*Bearer\s+)\S+", re.IGNORECASE)
_TOKEN_PATTERN = re.compile(
    r"(token|key|secret|password|api_key|apikey)([\"']?\s*[:=]\s*[\"']?)\S+", re.IGNORECASE
)


@contextmanager
def correlation_id_scope(new_id: str) -> Generator[str, None, None]:
    """Set correlation_id for a scope.

    Args:
        new_id: The correlation ID to use within this scope.

    Yields:
        The new correlation ID.
    """
    token = correlation_id.set(new_id)
    try:
        yield new_id
    finally:
        correlation_id.reset(token)


def redact(text: str) -> str:
    """Mask sensitive data in text.

    Redacts:
    - Authorization: Bearer <token>
    - token/key/secret/password/api_key patterns

    Args:
        text: Input text potentially containing secrets.

    Returns:
        Text with sensitive values masked as [REDACTED].
    """
    result = _BEARER_PATTERN.sub(r"\1[REDACTED]", text)
    result = _TOKEN_PATTERN.sub(r"\1\2[REDACTED]", result)
    return result


class JsonFormatter(logging.Formatter):
    """JSON log formatter with required fields."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "msg": record.getMessage(),
            "logger": record.name,
            "event": getattr(record, "event", "log"),
            "correlation_id": correlation_id.get(),
        }

        # Add extra fields if present
        if hasattr(record, "extra_data"):
            log_entry.update(record.extra_data)

        return json.dumps(log_entry)


class SafeStreamHandler(logging.StreamHandler):
    """StreamHandler that silently ignores writes to closed streams.

    Prevents ValueError: I/O operation on closed file during pytest teardown.
    """

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a record, silently ignoring closed stream errors."""
        try:
            stream = self.stream
            if stream is None or getattr(stream, "closed", False):
                return
            super().emit(record)
        except ValueError:
            # Stream was closed between check and write - ignore
            pass
        except Exception:
            # Don't let logging errors crash the application
            self.handleError(record)


def configure_logging(config: AppConfig) -> None:
    """Configure structured JSON logging.

    Args:
        config: Application configuration with log_level.
    """
    root_logger = logging.getLogger()

    # Clear existing handlers
    root_logger.handlers.clear()

    # Create safe JSON handler for stdout
    handler = SafeStreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root_logger.addHandler(handler)
    root_logger.setLevel(getattr(logging, config.log_level))


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance.

    Args:
        name: Logger name (typically __name__).

    Returns:
        Configured logger.
    """
    return logging.getLogger(name)


class LogAdapter(logging.LoggerAdapter):
    """Logger adapter that injects event field."""

    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        """Add event to log record."""
        extra = kwargs.get("extra", {})
        if "event" not in extra:
            extra["event"] = "log"
        kwargs["extra"] = extra
        return msg, kwargs


def log_event(
    logger: logging.Logger,
    level: int,
    msg: str,
    event: str,
    **extra: Any,
) -> None:
    """Log with explicit event field.

    Args:
        logger: Logger instance.
        level: Log level (e.g., logging.INFO).
        msg: Log message.
        event: Event type identifier.
        **extra: Additional fields to include.
    """
    logger.log(level, msg, extra={"event": event, "extra_data": extra})
