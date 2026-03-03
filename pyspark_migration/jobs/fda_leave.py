"""
PySpark job: FDA Leave — Cycle ID Tracking

Replaces Informatica PowerCenter mapping ``m_0150_PM_FDA_Error_Counter``
and the Update Strategy (DD_UPDATE) for CPM_CYCLE_TBL
(source file: XML/FDA_Leave).

Processing flow:
  1. Read CPM_CYCLE_TBL filtered for PROCESS_NAME = 'FDA'.
  2. Look up the current pay period from PAY_PERIOD (CURR_PP_FLAG = 'Y').
  3. Increment CYCLE_ID and update PP_NUM / PP_END_YEAR from the
     current pay period.
  4. Write back via raw SQL UPDATE using cx_Oracle (since PySpark
     cannot perform in-place updates).
"""

import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark_migration.utils.db import (
    get_tgt_jdbc_url,
    get_tgt_jdbc_properties,
    get_tgt_cx_connection,
    execute_sql,
    execute_query,
)
from pyspark_migration.utils.email import send_failure_email

logger = logging.getLogger(__name__)


def _get_current_pay_period(spark: SparkSession) -> dict:
    """Read the current pay period from PAY_PERIOD.

    Returns a dict with PP_NUM and PP_END_YEAR.
    """
    url = get_tgt_jdbc_url()
    props = get_tgt_jdbc_properties()

    rows = (
        spark.read.jdbc(url, "PAY_PERIOD", properties=props)
        .filter(col("CURR_PP_FLAG") == "Y")
        .select("PP_NUM", "PP_END_YEAR")
        .collect()
    )

    if not rows:
        raise RuntimeError("No current pay period found in PAY_PERIOD table")
    if len(rows) > 1:
        raise RuntimeError(
            f"Expected 1 current pay period, found {len(rows)}"
        )

    return {"PP_NUM": rows[0]["PP_NUM"], "PP_END_YEAR": rows[0]["PP_END_YEAR"]}


def _get_current_cycle_id() -> int:
    """Read the current CYCLE_ID for PROCESS_NAME = 'FDA' from CPM_CYCLE_TBL."""
    conn = get_tgt_cx_connection()
    try:
        result = execute_query(
            conn,
            "SELECT CYCLE_ID FROM CPM_CYCLE_TBL WHERE PROCESS_NAME = 'FDA'",
        )
        if not result:
            raise RuntimeError(
                "No row found in CPM_CYCLE_TBL for PROCESS_NAME = 'FDA'"
            )
        return result[0][0]
    finally:
        conn.close()


def _update_cycle(new_cycle_id: int, pp_num: int, pp_end_year: int) -> None:
    """Update CPM_CYCLE_TBL with the incremented CYCLE_ID and current PP info.

    Replaces the PowerCenter Update Strategy (DD_UPDATE).
    """
    conn = get_tgt_cx_connection()
    try:
        execute_sql(conn, f"""
            UPDATE CPM_CYCLE_TBL
            SET CYCLE_ID = {int(new_cycle_id)},
                PP_NUM = {int(pp_num)},
                PP_END_YEAR = {int(pp_end_year)}
            WHERE PROCESS_NAME = 'FDA'
        """)
        logger.info(
            "CPM_CYCLE_TBL updated: CYCLE_ID=%d, PP_NUM=%d, PP_END_YEAR=%d",
            new_cycle_id, pp_num, pp_end_year,
        )
    finally:
        conn.close()


def run() -> None:
    """Execute the FDA Leave Cycle ID Tracking job."""
    spark = (
        SparkSession.builder
        .appName("FDA_Leave_Cycle_ID")
        .getOrCreate()
    )

    try:
        logger.info("Starting FDA Leave Cycle ID Tracking job")

        # 1. Get current pay period
        current_pp = _get_current_pay_period(spark)
        pp_num = current_pp["PP_NUM"]
        pp_end_year = current_pp["PP_END_YEAR"]
        logger.info(
            "Current pay period: PP_NUM=%d, PP_END_YEAR=%d", pp_num, pp_end_year
        )

        # 2. Read current cycle ID
        current_cycle_id = _get_current_cycle_id()
        logger.info("Current CYCLE_ID for FDA: %d", current_cycle_id)

        # 3. Increment cycle ID
        new_cycle_id = current_cycle_id + 1

        # 4. Update CPM_CYCLE_TBL
        _update_cycle(new_cycle_id, pp_num, pp_end_year)

        logger.info("FDA Leave Cycle ID Tracking job completed successfully")

    except Exception as exc:
        logger.error("FDA Leave job failed: %s", exc)
        send_failure_email("FDA Leave Cycle ID Tracking", str(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
