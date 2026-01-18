import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(config):
    level = getattr(logging, config.level.upper(), logging.INFO)
    logger = logging.getLogger()
    logger.setLevel(level)

    fmt = logging.Formatter("%(asctime)s level=%(levelname)s msg=%(message)s")

    log_path = Path(config.file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    file_handler = RotatingFileHandler(
        log_path, maxBytes=config.max_bytes, backupCount=config.backup_count
    )
    file_handler.setFormatter(fmt)
    file_handler.setLevel(level)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    console_handler.setLevel(level)

    logger.handlers = [file_handler, console_handler]
