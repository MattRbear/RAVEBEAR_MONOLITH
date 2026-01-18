"""Regression test for SafeStreamHandler preventing closed-stream errors."""

import io
import logging

from ravebear_monolith.util.logging import SafeStreamHandler


def test_safe_stream_handler_ignores_closed_stream() -> None:
    """SafeStreamHandler silently ignores writes to closed streams."""
    # Create a StringIO stream and close it
    stream = io.StringIO()
    stream.close()

    # Create handler with closed stream
    handler = SafeStreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(message)s"))

    # Create a log record
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    # This should NOT raise ValueError
    handler.emit(record)

    # If we get here, the test passed (no exception)


def test_safe_stream_handler_works_with_open_stream() -> None:
    """SafeStreamHandler works normally with open streams."""
    stream = io.StringIO()

    handler = SafeStreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(message)s"))

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Hello World",
        args=(),
        exc_info=None,
    )

    handler.emit(record)

    # Check that message was written
    assert "Hello World" in stream.getvalue()
