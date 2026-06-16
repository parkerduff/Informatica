"""Replaces the ``ehrp2biis_preload`` ksh script.

Original flow:
    1. ``sqlplus`` runs ``@ $homedir/step01``, spooling to a timestamped logfile.
    2. ``grep -i "ERROR" $logfile`` decides success vs failure (ERR_FLAG).
    3. On error  -> mailx failure subject.
       On success -> uuencode the log and mailx it as ehrp2biis_prerun_rpt.txt.

This Python version executes the SQL via JDBC, captures real log output and
sends the appropriate notification with the log attached.
"""
from __future__ import annotations

import os

from pyspark_etl.config.notifications import EHRP2BIIS_RECIPIENTS
from pyspark_etl.utils.logging_config import configure_logging
from pyspark_etl.utils.notifications import send_notification
from pyspark_etl.utils.oracle_jdbc import execute_sql_script

DEFAULT_SCRIPT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "sql", "ehrp2biis_preload_step01.sql",
)


def run(script_path: str = DEFAULT_SCRIPT, log_dir: str | None = None,
        dry_run_email: bool = False) -> bool:
    """Run the preload SQL; email success/failure. Returns ``True`` on success."""
    logger, errors, logfile = configure_logging("ehrp2biis_preload", log_dir)
    logger.info("Running preload script %s", script_path)
    try:
        statements = execute_sql_script("ORA_BIIS", script_path)
        logger.info("Preload executed %d statement(s) successfully", len(statements))
    except Exception as exc:
        logger.error("Preload failed: %s", exc)

    if errors.had_errors:
        send_notification(
            "EHRP2BIIS Preload script did not complete successfully",
            EHRP2BIIS_RECIPIENTS,
            body="See attached log for details.",
            attachment_path=logfile,
            dry_run=dry_run_email,
        )
        return False

    logger.info("SQL Script Ran Successfully")
    send_notification(
        "EHRP2BIIS Preload script completed successfully",
        EHRP2BIIS_RECIPIENTS,
        body="ehrp2biis_prerun_rpt",
        attachment_path=logfile,
        dry_run=dry_run_email,
    )
    return True
