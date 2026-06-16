"""Structured logging setup.

Replaces the original ``grep -i "ERROR" $logfile`` error-detection pattern with
real Python logging. A job can inspect :class:`ErrorCaptureHandler` to decide
whether to send a success or failure notification, reproducing the ksh
``ERR_FLAG`` behaviour without scraping log text.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from typing import List, Optional


class ErrorCaptureHandler(logging.Handler):
    """Collects WARNING+ records so a job can check whether anything failed."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.records: List[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    @property
    def had_errors(self) -> bool:
        return any(r.levelno >= logging.ERROR for r in self.records)


def configure_logging(
    job_name: str,
    log_dir: Optional[str] = None,
    level: int = logging.INFO,
) -> tuple[logging.Logger, ErrorCaptureHandler, Optional[str]]:
    """Configure and return ``(logger, error_handler, logfile_path)``.

    A timestamped logfile is created under ``log_dir`` (when supplied), mirroring
    the ``ehrp2biis_preload_<ts>.log`` naming of the original scripts.
    """
    logger = logging.getLogger(job_name)
    logger.setLevel(level)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    logger.addHandler(stream)

    logfile_path: Optional[str] = None
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        stamp = datetime.now().strftime("%y%m%d%H%M%S")
        logfile_path = os.path.join(log_dir, f"{job_name}_{stamp}.log")
        file_handler = logging.FileHandler(logfile_path)
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)

    error_handler = ErrorCaptureHandler()
    logger.addHandler(error_handler)

    return logger, error_handler, logfile_path
