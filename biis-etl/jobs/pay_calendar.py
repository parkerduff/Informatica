#!/usr/bin/env python3
"""Pay Calendar job -- migration of Informatica workflow ``wf_Pay_Calendar``.

The original workflow has four sessions:
  1. Reset  -- clear the current pay-period flag
  2. Set    -- mark the pay period containing the run date as current
  3. Verify -- assert exactly one current pay period exists
  4. Notify -- announce the active pay period

Pay-period rotation is pure DML, so it runs through pyodbc rather than Spark.
The ``--run-date`` parameter replaces the original ``trunc(sysdate)`` dependency.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
from typing import Any, Dict

from utils.db import execute_sql, pyodbc_connection
from utils.notifications import send_notification
from utils.secrets import Config, get_db_secret, load_config
from utils.validation import ValidationError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("pay_calendar")

PROCESS_NAME = "PAY_CALENDAR"


def session_reset(conn: Any) -> int:
    cur = execute_sql(
        conn, "UPDATE dbo.PAY_PERIOD SET CURR_PP_FLAG = NULL WHERE CURR_PP_FLAG = 'Y'"
    )
    logger.info("Reset: cleared %s current flag(s)", cur.rowcount)
    return cur.rowcount


def session_set(conn: Any, run_date: dt.date) -> int:
    cur = execute_sql(
        conn,
        "UPDATE dbo.PAY_PERIOD SET CURR_PP_FLAG = 'Y' "
        "WHERE CAST(? AS DATE) BETWEEN CAST(PP_START_DTE AS DATE) AND CAST(PP_END_DTE AS DATE)",
        [run_date],
    )
    logger.info("Set: marked %s pay period(s) current for run_date=%s", cur.rowcount, run_date)
    return cur.rowcount


def session_verify(conn: Any) -> Dict[str, Any]:
    cur = execute_sql(
        conn,
        "SELECT PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE, PAY_DTE "
        "FROM dbo.PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'",
    )
    rows = cur.fetchall()
    if len(rows) != 1:
        raise ValidationError(
            f"Verify failed: expected exactly 1 current pay period, found {len(rows)}"
        )
    r = rows[0]
    pp = {
        "pp_num": int(r[0]),
        "pp_end_year": int(r[1]),
        "pp_start_dte": r[2],
        "pp_end_dte": r[3],
        "pay_dte": r[4],
    }
    logger.info("Verify: current pay period = PP %(pp_num)s / %(pp_end_year)s", pp)
    return pp


def session_notify(pp: Dict[str, Any], config: Config) -> None:
    body = (
        f"Pay calendar updated. Current pay period: "
        f"PP {pp['pp_num']:02d} of {pp['pp_end_year']} "
        f"({pp['pp_start_dte']} - {pp['pp_end_dte']}), pay date {pp['pay_dte']}."
    )
    send_notification(f"{PROCESS_NAME}: current pay period set", body, config)


def run(config: Config, run_date: dt.date) -> Dict[str, Any]:  # pragma: no cover
    secret = get_db_secret(config)
    with pyodbc_connection(secret, config, autocommit=False) as conn:
        try:
            session_reset(conn)
            session_set(conn, run_date)
            pp = session_verify(conn)
            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception("pay_calendar failed; rolled back")
            raise
    session_notify(pp, config)
    return pp


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default=None)
    ap.add_argument("--run-date", default=dt.date.today().isoformat())
    args = ap.parse_args()
    config = load_config(args.env)
    run_date = dt.date.fromisoformat(args.run_date)
    run(config, run_date)
    logger.info("pay_calendar complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
