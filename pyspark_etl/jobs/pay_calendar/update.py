"""Replaces the ``Pay_Calendar`` workflow (verify / set / reset CURR_PP_FLAG).

The Informatica mappings maintain exactly one "current" row in ``PAY_PERIOD``:

* ``m_Pay_Calendar_Set_Pay_Calendar`` -- sets ``CURR_PP_FLAG = 'Y'`` for the
  target pay period. The target is either the parameter pair ($$PP_NUM /
  $$PP_END_YEAR) when supplied, or the period whose date range contains the run
  date (non-parameter path).
* ``m_Pay_Calendar_Reset_Pay_Calendar`` -- clears ``CURR_PP_FLAG`` for all rows.
* ``m_Pay_Calendar_Verify_Pay_Calendar`` -- ABORTs unless exactly one row is
  current (exp_Check_Current_Flag).

These are in-place single-table updates, so they run as SQL via JDBC rather than
as Spark DataFrame writes.
"""
from __future__ import annotations

import logging
from typing import Optional

from pyspark_etl.config import connections
from pyspark_etl.utils.oracle_jdbc import execute_statements

logger = logging.getLogger(__name__)

TABLE = f"{connections.SCHEMA_HISTDBA}.PAY_PERIOD"


def reset_pay_calendar() -> None:
    """m_Pay_Calendar_Reset_Pay_Calendar: clear the current flag everywhere."""
    execute_statements("ORA_BIIS", [f"UPDATE {TABLE} SET CURR_PP_FLAG = NULL"])
    logger.info("Reset CURR_PP_FLAG on all PAY_PERIOD rows")


def set_pay_calendar(pp_num: Optional[int] = None,
                     pp_end_year: Optional[int] = None) -> None:
    """m_Pay_Calendar_Set_Pay_Calendar: mark the target period as current.

    With both ``pp_num`` and ``pp_end_year`` supplied, the parameter path is
    used; otherwise the run-date path picks the period containing today.
    """
    statements = [f"UPDATE {TABLE} SET CURR_PP_FLAG = NULL"]
    if pp_num is not None and pp_end_year is not None:
        statements.append(
            f"UPDATE {TABLE} SET CURR_PP_FLAG = 'Y' "
            f"WHERE PP_NUM = {int(pp_num)} AND PP_END_YEAR = {int(pp_end_year)}"
        )
        logger.info("Set current pay period to %s/%s (param path)", pp_num, pp_end_year)
    else:
        statements.append(
            f"UPDATE {TABLE} SET CURR_PP_FLAG = 'Y' "
            f"WHERE TRUNC(SYSDATE) BETWEEN PP_START_DTE AND PP_END_DTE"
        )
        logger.info("Set current pay period by run date (non-param path)")
    execute_statements("ORA_BIIS", statements)


def verify_pay_calendar() -> int:
    """m_Pay_Calendar_Verify_Pay_Calendar: ensure exactly one current row.

    Returns the count; raises ``RuntimeError`` for 0 or >1, mirroring the
    Informatica ABORT() calls.
    """
    import oracledb

    conn = connections.get_connection("ORA_BIIS")
    db = oracledb.connect(user=conn.user, password=conn.password, dsn=conn.dsn())
    try:
        cur = db.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE CURR_PP_FLAG = 'Y'")
        (count,) = cur.fetchone()
    finally:
        db.close()

    if count == 0:
        raise RuntimeError("!!!! There is no pay period set.")
    if count > 1:
        raise RuntimeError("!!!! There are more than one pay periods set to current.")
    logger.info("Current flag has been properly set (1 row)")
    return count
