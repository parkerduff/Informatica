"""EHRP2BIIS afterload — T-SQL port of ehrp2biis_afterload.sql.

Runs sequentially over pyodbc:
  Step 04: clear RETND1_STEP_CD '0.0000000000000' on retained secondary rows.
  Step 05: run the sequence-number / formatting procedures (stubs until the
           Oracle PL/SQL source is extracted), refresh PROCESS_TABLE
           (ROWNUM < 2 -> TOP 1), run WIP-status check, then delete-and-insert
           the ACTION_*_ALL tables in a single transaction and truncate the
           staged actions.
"""
import argparse
import datetime
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.db import pyodbc_connection  # noqa: E402
from utils.notifications import send_notification  # noqa: E402
from utils.secrets import get_secret, load_config  # noqa: E402

logger = logging.getLogger("ehrp2biis.afterload")

PROCS_IN_ORDER = [
    "update_sequence_number_tbl_p",
    "updt_erp2biis_cre8_remarks01_p",
    "update_erp2biis_no900s01_p",
    "erp2biis_cre8_remarks_900s01",
    "update_erp2biis_900sonly01_p",
    "updt_orig_cancelled_trans01_p",
]


def step04_clear_retained(cur, run_date):
    cur.execute(
        """
        UPDATE a SET a.RETND1_STEP_CD = NULL
        FROM NWK_ACTION_SECONDARY_TBL a
        WHERE a.EVENT_ID IN (SELECT b.EVENT_ID FROM NWK_ACTION_PRIMARY_TBL b
                             WHERE CAST(b.LOAD_DATE AS DATE) = ?
                               AND b.EVENT_ID < 9000000000)
          AND a.RETND1_STEP_CD = '0.0000000000000'
        """,
        run_date,
    )
    logger.info("Step04 cleared RETND1_STEP_CD on %d rows", cur.rowcount)


def step05_run_procs(cur, run_date):
    for proc in PROCS_IN_ORDER:
        logger.info("Executing procedure %s", proc)
        cur.execute(f"EXEC dbo.{proc} @run_date = ?", run_date)
        while cur.nextset():
            pass


def step05_process_table(cur, run_date):
    cur.execute("UPDATE PROCESS_TABLE SET P_STARTDT = NULL")
    cur.execute(
        """
        UPDATE PROCESS_TABLE SET P_STARTDT =
          (SELECT TOP 1 a.EFFDT
             FROM EHRP_RECS_TRACKING_TBL a
             JOIN PS_GVT_JOB b
               ON a.EMPLID = b.EMPLID AND a.EMPL_RCD = b.EMPL_RCD
              AND a.EFFDT = b.EFFDT AND a.EFFSEQ = b.EFFSEQ
            WHERE a.GVT_WIP_STATUS <> b.GVT_WIP_STATUS
            ORDER BY a.EFFSEQ, a.BIIS_EVENT_ID)
        """
    )
    cur.execute(
        "UPDATE PROCESS_TABLE SET P_STARTDT = DATEADD(DAY, 10000, CAST(? AS DATETIME2)) "
        "WHERE P_STARTDT IS NULL",
        run_date,
    )
    cur.execute("EXEC dbo.chk_ehrp2biis_wip_status_p @run_date = ?", run_date)
    while cur.nextset():
        pass


def step05_refresh_all_tables(conn, run_date):
    """Delete-then-insert into the *_ALL tables in a SINGLE transaction."""
    cur = conn.cursor()
    for table, source in (
        ("ACTION_REMARKS_ALL", "NWK_ACTION_REMARKS_TBL"),
        ("ACTION_PRIMARY_ALL", "NWK_ACTION_PRIMARY_TBL"),
        ("ACTION_SECONDARY_ALL", "NWK_ACTION_SECONDARY_TBL"),
    ):
        cur.execute(
            f"""
            DELETE FROM {table}
            WHERE EVENT_ID IN (SELECT b.BIIS_EVENT_ID FROM EHRP_RECS_TRACKING_TBL b
                               WHERE CAST(b.LOAD_DATE AS DATE) = ?)
            """,
            run_date,
        )
        logger.info("Deleted %d rows from %s", cur.rowcount, table)
        cur.execute(
            f"""
            INSERT INTO {table}
            SELECT * FROM {source} d
            WHERE d.EVENT_ID IN (SELECT b.BIIS_EVENT_ID FROM EHRP_RECS_TRACKING_TBL b
                                 WHERE CAST(b.LOAD_DATE AS DATE) = ?)
            """,
            run_date,
        )
        logger.info("Inserted %d rows into %s", cur.rowcount, table)


def run(env: str = None, run_date: str = None) -> None:
    config = load_config(env)
    secret = get_secret("biis", config)
    rd = (
        datetime.datetime.strptime(run_date, "%Y-%m-%d").date()
        if run_date else datetime.date.today()
    )
    try:
        with pyodbc_connection(secret, config, autocommit=False) as conn:
            cur = conn.cursor()
            step04_clear_retained(cur, rd)
            conn.commit()
            step05_run_procs(cur, rd)
            conn.commit()
            step05_process_table(cur, rd)
            conn.commit()
            step05_refresh_all_tables(conn, rd)
            cur.execute("TRUNCATE TABLE NWK_NEW_EHRP_ACTIONS_TBL")
            conn.commit()
        send_notification("EHRP2BIIS Afterload completed", f"Afterload finished for {rd}", config)
    except Exception as exc:
        send_notification("EHRP2BIIS Afterload FAILED", str(exc), config)
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=None)
    parser.add_argument("--run-date", default=None)
    args = parser.parse_args()
    run(args.env, args.run_date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
