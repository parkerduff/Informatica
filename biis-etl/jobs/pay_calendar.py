"""Pay Calendar job — migration of Informatica workflow wf_Pay_Calendar.

Sessions (run sequentially):
  1. Reset:  clear CURR_PP_FLAG everywhere it is 'Y'.
  2. Set:    flag the pay period containing --run-date as current.
  3. Verify: exactly one row must carry CURR_PP_FLAG = 'Y'.
  4. Notify: send the current pay period details.
"""
import argparse
import datetime
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db import pyodbc_connection  # noqa: E402
from utils.notifications import send_notification  # noqa: E402
from utils.secrets import get_secret, load_config  # noqa: E402
from utils.validation import ValidationError  # noqa: E402

logger = logging.getLogger("pay_calendar")


def session_reset(conn) -> int:
    cur = conn.cursor()
    cur.execute("UPDATE PAY_PERIOD SET CURR_PP_FLAG = NULL WHERE CURR_PP_FLAG = 'Y'")
    logger.info("Reset CURR_PP_FLAG on %d rows", cur.rowcount)
    return cur.rowcount


def session_set(conn, run_date: datetime.date) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE PAY_PERIOD SET CURR_PP_FLAG = 'Y'
        WHERE CAST(PP_START_DTE AS DATE) <= ? AND CAST(PP_END_DTE AS DATE) >= ?
        """,
        run_date, run_date,
    )
    logger.info("Set CURR_PP_FLAG on %d rows for run date %s", cur.rowcount, run_date)
    return cur.rowcount


def session_verify(conn) -> dict:
    cur = conn.cursor()
    cur.execute(
        "SELECT PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE, PAY_DTE "
        "FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'"
    )
    rows = cur.fetchall()
    if len(rows) != 1:
        raise ValidationError(
            f"Pay calendar verify failed: expected exactly 1 current pay period, found {len(rows)}"
        )
    r = rows[0]
    return {
        "pp_num": int(r[0]),
        "pp_end_year": int(r[1]),
        "pp_start_dte": r[2],
        "pp_end_dte": r[3],
        "pay_dte": r[4],
    }


def session_notify(pp: dict, config: dict) -> None:
    body = (
        f"Current pay period set: PP {pp['pp_num']} / {pp['pp_end_year']} "
        f"({pp['pp_start_dte']} - {pp['pp_end_dte']}), pay date {pp['pay_dte']}"
    )
    send_notification("Pay Calendar rotation completed", body, config)


def run(env: str = None, run_date: str = None) -> dict:
    config = load_config(env)
    secret = get_secret("biis", config)
    rd = (
        datetime.datetime.strptime(run_date, "%Y-%m-%d").date()
        if run_date else datetime.date.today()
    )
    with pyodbc_connection(secret, config) as conn:
        try:
            session_reset(conn)
            session_set(conn, rd)
            pp = session_verify(conn)
        except Exception as exc:
            send_notification("Pay Calendar job FAILED", str(exc), config)
            raise
    session_notify(pp, config)
    return pp


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=None)
    parser.add_argument("--run-date", default=None)
    args = parser.parse_args()
    run(args.env, args.run_date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
