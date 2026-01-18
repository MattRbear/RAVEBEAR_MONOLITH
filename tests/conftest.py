"""Shared pytest fixtures for all tests."""

import logging
import os
import warnings
from collections.abc import Generator

import pytest


def pytest_configure(config: pytest.Config) -> None:
    """Configure strict warnings if RAVEBEAR_STRICT_TESTS is set."""
    if os.environ.get("RAVEBEAR_STRICT_TESTS") == "1":
        # Make these specific warnings fatal
        warnings.filterwarnings("error", category=pytest.PytestUnhandledThreadExceptionWarning)
        warnings.filterwarnings("error", category=pytest.PytestUnraisableExceptionWarning)
        warnings.filterwarnings("error", category=ResourceWarning)


@pytest.fixture(scope="session", autouse=True)
def disable_logging_exceptions() -> Generator[None, None, None]:
    """Disable logging exception raising under pytest to prevent closed-stream errors."""
    original = logging.raiseExceptions
    logging.raiseExceptions = False
    yield
    logging.raiseExceptions = original
    # Final shutdown of logging
    logging.shutdown()


@pytest.fixture(autouse=True)
def reset_logging_handlers() -> Generator[None, None, None]:
    """Reset logging handlers after each test to prevent file handle leaks."""
    yield
    # Clean up all logger handlers after test
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        try:
            handler.close()
        except Exception:
            pass
        root_logger.removeHandler(handler)

    # Clean up named loggers
    for name in list(logging.Logger.manager.loggerDict.keys()):
        logger = logging.getLogger(name)
        for handler in logger.handlers[:]:
            try:
                handler.close()
            except Exception:
                pass
            logger.removeHandler(handler)
