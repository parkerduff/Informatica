"""
Logging configuration for BIISINT PySpark ETL pipelines.

Replaces:
  - Timestamped log files: $logdir/script_name_YYMMDDHHMMSS.log
  - echo / spool patterns from KSH and SQL scripts
"""

import logging
import os
from datetime import datetime

from pyspark.utils.config import PathConfig


def setup_logging(
    process_name: str,
    paths: PathConfig,
    level: int = logging.INFO,
) -> str:
    """Configure logging to both console and a timestamped log file.

    Args:
        process_name: Name used in the log filename.
        paths: Path configuration for log directory.
        level: Logging level.

    Returns:
        The path to the log file.
    """
    timestamp = datetime.now().strftime("%y%m%d%H%M%S")
    log_filename = f"{process_name}_{timestamp}.log"
    log_path = os.path.join(paths.log_dir, log_filename)

    os.makedirs(paths.log_dir, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Clear any existing handlers
    root_logger.handlers.clear()

    # File handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(level)
    file_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(file_format)
    root_logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_format = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(console_format)
    root_logger.addHandler(console_handler)

    logging.info("Logging initialized: %s", log_path)
    return log_path
