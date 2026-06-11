#!/usr/bin/env python3
"""EHRP2BIIS preload -- migration of the ``ehrp2biis_preload`` shell script.

The original shell script runs the SQL*Plus script ``step01`` (which lives on
the server at ``/data/BIISINT/bin/EHRP2BIIS/step01`` and is NOT in this repo).
Until that source is extracted, this is a placeholder that connects to SQL
Server, runs the preload SQL (currently a no-op stub), validates success and
notifies on failure.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging

from utils.db import execute_sql, pyodbc_connection
from utils.notifications import send_notification
from utils.secrets import Config, get_db_secret, load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("ehrp2biis.preload")

PROCESS_NAME = "EHRP2BIIS_PRELOAD"

# Placeholder for the extracted step01 SQL. Kept as a harmless probe so the
# pipeline runs end-to-end; replace with the real step01 statements.
PRELOAD_SQL = "SELECT 1 AS preload_stub"


def run(config: Config, run_date: dt.date) -> int:
    secret = get_db_secret(config)
    logger.warning("PRELOAD STUB -- replace PRELOAD_SQL with extracted step01 SQL")
    try:
        with pyodbc_connection(secret, config) as conn:
            cur = execute_sql(conn, PRELOAD_SQL)
            cur.fetchall()
    except Exception as exc:  # noqa: BLE001 - surface as notification then re-raise
        send_notification(
            f"{PROCESS_NAME}: FAILED", f"Preload failed for {run_date}: {exc}", config
        )
        raise
    logger.info("ehrp2biis preload complete for %s", run_date)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default=None)
    ap.add_argument("--run-date", default=dt.date.today().isoformat())
    args = ap.parse_args()
    config = load_config(args.env)
    return run(config, dt.date.fromisoformat(args.run_date))


if __name__ == "__main__":
    raise SystemExit(main())
