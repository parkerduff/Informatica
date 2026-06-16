"""Replaces ``ehrp2biis_afterload.sql`` execution.

The afterload is complex multi-statement PL/SQL: UPDATEs, EXECs of HISTDBA
stored procedures, INSERT...SELECT into the *_ALL tables, cancelled-action
re-sync and a final TRUNCATE. It is intentionally **not** rewritten as PySpark
DataFrames -- it stays as SQL executed sequentially via JDBC inside a single
transaction (see utils.oracle_jdbc.execute_sql_script).

The stored procedures (update_sequence_number_tbl_p, chk_ehrp2biis_wip_status_p,
HISTDBA.*) remain in the database; this job merely invokes them.
"""
from __future__ import annotations

import os

from pyspark_etl.config.notifications import EHRP2BIIS_RECIPIENTS
from pyspark_etl.utils.logging_config import configure_logging
from pyspark_etl.utils.notifications import send_notification
from pyspark_etl.utils.oracle_jdbc import execute_sql_script

DEFAULT_SCRIPT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "sql", "ehrp2biis_afterload.sql",
)


def run(script_path: str = DEFAULT_SCRIPT, log_dir: str | None = None,
        dry_run_email: bool = False) -> bool:
    """Execute the afterload SQL script via JDBC; email on completion/failure."""
    logger, errors, logfile = configure_logging("ehrp2biis_afterload", log_dir)
    logger.info("Running afterload script %s", script_path)
    try:
        statements = execute_sql_script("ORA_BIIS", script_path)
        logger.info("Afterload executed %d statement(s)", len(statements))
    except Exception as exc:
        logger.error("Afterload failed: %s", exc)

    ok = not errors.had_errors
    subject = (
        "EHRP2BIIS Afterload completed successfully" if ok
        else "EHRP2BIIS Afterload did not complete successfully"
    )
    send_notification(subject, EHRP2BIIS_RECIPIENTS, attachment_path=logfile,
                      dry_run=dry_run_email)
    return ok
