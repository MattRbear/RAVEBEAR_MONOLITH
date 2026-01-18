"""Tests for structured JSON logging."""

import json
import logging

import pytest

from ravebear_monolith.foundation.config import AppConfig
from ravebear_monolith.util.logging import (
    JsonFormatter,
    configure_logging,
    correlation_id,
    correlation_id_scope,
    get_logger,
    log_event,
    redact,
)


class TestRedact:
    """Tests for the redact function."""

    def test_redact_bearer_token(self) -> None:
        """Redacts Authorization: Bearer tokens."""
        text = "Authorization: Bearer abc123secret"
        result = redact(text)
        assert "[REDACTED]" in result
        assert "abc123secret" not in result

    def test_redact_api_key(self) -> None:
        """Redacts api_key patterns."""
        text = "api_key=mysecretkey123"
        result = redact(text)
        assert "[REDACTED]" in result
        assert "mysecretkey123" not in result

    def test_redact_token_json(self) -> None:
        """Redacts token in JSON-like strings."""
        text = '"token": "secret_value"'
        result = redact(text)
        assert "[REDACTED]" in result
        assert "secret_value" not in result

    def test_redact_password(self) -> None:
        """Redacts password patterns."""
        text = "password=hunter2"
        result = redact(text)
        assert "[REDACTED]" in result
        assert "hunter2" not in result

    def test_redact_preserves_safe_text(self) -> None:
        """Does not modify text without secrets."""
        text = "This is a normal log message about data processing"
        result = redact(text)
        assert result == text


class TestCorrelationId:
    """Tests for correlation ID context var."""

    def test_default_is_root(self) -> None:
        """Default correlation_id is 'root'."""
        assert correlation_id.get() == "root"

    def test_scope_changes_id(self) -> None:
        """correlation_id_scope changes the ID within scope."""
        with correlation_id_scope("request-123"):
            assert correlation_id.get() == "request-123"
        assert correlation_id.get() == "root"

    def test_nested_scopes(self) -> None:
        """Nested scopes work correctly."""
        with correlation_id_scope("outer"):
            assert correlation_id.get() == "outer"
            with correlation_id_scope("inner"):
                assert correlation_id.get() == "inner"
            assert correlation_id.get() == "outer"
        assert correlation_id.get() == "root"


class TestJsonFormatter:
    """Tests for JSON log formatting."""

    def test_output_is_valid_json(self) -> None:
        """Formatter produces valid JSON."""
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert isinstance(parsed, dict)

    def test_required_fields_present(self) -> None:
        """All required fields are in output."""
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.WARNING,
            pathname="",
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)

        assert "ts" in parsed
        assert "level" in parsed
        assert "msg" in parsed
        assert "logger" in parsed
        assert "event" in parsed
        assert "correlation_id" in parsed

    def test_level_is_name_not_number(self) -> None:
        """Level field is the name, not numeric level."""
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="Error occurred",
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["level"] == "ERROR"

    def test_correlation_id_included(self) -> None:
        """Correlation ID from contextvar is included."""
        formatter = JsonFormatter()
        with correlation_id_scope("test-corr-123"):
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg="Test",
                args=(),
                exc_info=None,
            )
            output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["correlation_id"] == "test-corr-123"


class TestConfigureLogging:
    """Tests for logging configuration."""

    def test_configure_sets_level(self) -> None:
        """configure_logging sets the root logger level."""
        config = AppConfig(log_level="WARNING")
        configure_logging(config)
        root = logging.getLogger()
        assert root.level == logging.WARNING

    def test_configure_clears_handlers(self) -> None:
        """configure_logging replaces existing handlers."""
        root = logging.getLogger()
        config = AppConfig(log_level="INFO")
        configure_logging(config)
        # Should have exactly one handler after configure
        assert len(root.handlers) == 1


class TestLogEvent:
    """Tests for log_event helper."""

    def test_log_event_includes_event_field(self, capfd: pytest.CaptureFixture[str]) -> None:
        """log_event adds event field to log record."""
        config = AppConfig(log_level="DEBUG")
        configure_logging(config)
        logger = get_logger("test")

        log_event(logger, logging.INFO, "Test message", event="test_event")

        captured = capfd.readouterr()
        parsed = json.loads(captured.out.strip())
        assert parsed["event"] == "test_event"
