"""Logging configuration for the BIIS ETL jobs.

Replaces the grep-based ``ERR_FLAG`` error detection in the original shell
scripts with structured Python logging. Log files follow the existing naming
convention ``{job_name}_{YYMMDDHHmmSS}.log`` and are written to ``LOG_DIR``.
"""
import logging
import os
from datetime import datetime

from config.connections import LOG_DIR

LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def log_filename(job_name: str) -> str:
    """Build a timestamped log filename matching the legacy convention."""
    stamp = datetime.now().strftime("%y%m%d%H%M%S")
    return "%s_%s.log" % (job_name, stamp)


def configure_logging(job_name: str, log_dir: str = LOG_DIR,
                      level: int = logging.INFO) -> logging.Logger:
    """Configure file + console logging for a job and return its logger.

    A file handler writes to ``{log_dir}/{job_name}_{YYMMDDHHmmSS}.log`` and a
    console (stream) handler mirrors output to stdout.
    """
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, log_filename(job_name))

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(level)
    # Avoid duplicate handlers if configure_logging is called more than once.
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.addHandler(file_handler)
    root.addHandler(console_handler)

    logger = logging.getLogger(job_name)
    logger.info("Logging to %s", log_path)
    return logger
