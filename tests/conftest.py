"""Shared pytest fixtures for all tests."""

import logging
from collections.abc import Generator

import pytest


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
