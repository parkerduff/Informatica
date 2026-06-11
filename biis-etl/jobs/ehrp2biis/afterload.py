#!/usr/bin/env python3
"""EHRP2BIIS afterload -- migration of ``ehrp2biis_afterload.sql`` (288 lines).

This is sequential DML, so it runs through pyodbc rather than Spark. Key
conversions from the Oracle original:
  * ``trunc(SYSDATE)``      -> parameterised ``run_date``
  * ``WHERE ROWNUM < 2``    -> ``SELECT TOP 1 ... ORDER BY ...``
  * the delete-then-insert into the ACTION_*_ALL tables is wrapped in a single
    transaction (closing the atomicity gap in the original script)
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging

from utils.db import execute_sql, pyodbc_connection
from utils.notifications import send_notification
from utils.secrets import Config, get_db_secret, load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("ehrp2biis.afterload")

PROCESS_NAME = "EHRP2BIIS_AFTERLOAD"

# Step 04: clear retained step code on secondary records for this run's primary
# events (guarded so it is a no-op if the column is absent).
SQL_RESET_RETAINED = """
IF COL_LENGTH('dbo.NWK_ACTION_SECONDARY_TBL', 'RETND1_STEP_CD') IS NOT NULL
EXEC('UPDATE a SET a.RETND1_STEP_CD = NULL
      FROM dbo.NWK_ACTION_SECONDARY_TBL a
      WHERE a.EVENT_ID IN (SELECT b.EVENT_ID FROM dbo.NWK_ACTION_PRIMARY_TBL b
                           WHERE CAST(b.LOAD_DATE AS DATE) = ''{run_date}''
                             AND b.EVENT_ID < 9000000000)
        AND a.RETND1_STEP_CD = ''0.0000000000000''')
"""

# WIP status check: ROWNUM < 2 -> TOP 1 ... ORDER BY.
SQL_PROCESS_TABLE = """
UPDATE dbo.PROCESS_TABLE
SET P_STARTDT = (
    SELECT TOP 1 b.EFFDT
    FROM dbo.EHRP_RECS_TRACKING_TBL a
    JOIN dbo.PS_GVT_JOB b
      ON a.EMPLID = b.EMPLID AND a.EMPL_RCD = b.EMPL_RCD
     AND a.EFFDT = b.EFFDT AND a.EFFSEQ = b.EFFSEQ
    WHERE a.GVT_WIP_STATUS <> b.GVT_WIP_STATUS
    ORDER BY a.EFFDT, a.BIIS_EVENT_ID
)
WHERE EXISTS (SELECT 1 FROM dbo.EHRP_RECS_TRACKING_TBL)
"""

PROCS_NO_ARGS = [
    "update_sequence_number_tbl_p",
    "updt_erp2biis_cre8_remarks01_p",
    "update_erp2biis_no900s01_p",
    "erp2biis_cre8_remarks_900s01",
    "update_erp2biis_900sonly01_p",
    "updt_orig_cancelled_trans01_p",
    "chk_ehrp2biis_wip_status_p",
]

# (ALL table, source table) pairs for the delete-then-insert promotion.
PROMOTIONS = [
    ("ACTION_PRIMARY_ALL", "NWK_ACTION_PRIMARY_TBL"),
    ("ACTION_SECONDARY_ALL", "NWK_ACTION_SECONDARY_TBL"),
    ("ACTION_REMARKS_ALL", "NWK_ACTION_REMARKS_TBL"),
]


def run(config: Config, run_date: dt.date) -> int:
    secret = get_db_secret(config)
    rd = run_date.isoformat()
    with pyodbc_connection(secret, config, autocommit=False) as conn:
        try:
            execute_sql(conn, SQL_RESET_RETAINED.format(run_date=rd))

            for proc in PROCS_NO_ARGS:
                logger.info("EXEC dbo.%s", proc)
                execute_sql(conn, f"EXEC dbo.{proc}")

            execute_sql(conn, "EXEC dbo.gather_ehrp2biis_runcounts_p @p_run_date = ?", [run_date])

            execute_sql(conn, SQL_PROCESS_TABLE)

            # Delete-then-insert promotion to *_ALL tables, atomically.
            for all_tbl, src_tbl in PROMOTIONS:
                execute_sql(
                    conn,
                    f"DELETE FROM dbo.{all_tbl} WHERE EVENT_ID IN "
                    f"(SELECT EVENT_ID FROM dbo.{src_tbl} WHERE CAST(LOAD_DATE AS DATE) = ?)",
                    [run_date],
                )
                execute_sql(
                    conn,
                    f"INSERT INTO dbo.{all_tbl} SELECT * FROM dbo.{src_tbl} "
                    f"WHERE CAST(LOAD_DATE AS DATE) = ?",
                    [run_date],
                )

            # Clear the staging input for the next cycle.
            execute_sql(conn, "TRUNCATE TABLE dbo.NWK_NEW_EHRP_ACTIONS_TBL")
            conn.commit()
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            send_notification(f"{PROCESS_NAME}: FAILED", f"Afterload failed for {rd}: {exc}", config)
            logger.exception("afterload failed; rolled back")
            raise

    send_notification(
        f"{PROCESS_NAME}: complete",
        f"Promoted action records to *_ALL tables for run_date {rd}.",
        config,
    )
    logger.info("ehrp2biis afterload complete for %s", rd)
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
