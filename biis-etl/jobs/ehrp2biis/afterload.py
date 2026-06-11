"""EHRP2BIIS afterload stage (migrated from ehrp2biis_afterload.sql).

Pure-SQL stage (no Spark) that:

1. Cleans retained-step codes (``RETND1_STEP_CD = '0.0000000000000'`` -> NULL)
   for today's non-900-series events.
2. Inserts today's staged primary/secondary/remarks rows into the BIIS
   ``ACTION_*_ALL`` tables.
3. Deletes and re-inserts cancelled actions (WIP-status changed today).
4. Truncates ``NWK_NEW_EHRP_ACTIONS_TBL`` for the next load.

All steps run inside a single transaction so a failure rolls everything back.
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

from jobs.ehrp2biis.common import NINE_HUNDRED_SERIES
from utils import db, notifications
from utils.config import get_config
from utils.schemas import column_names
from utils.spark import base_arg_parser

RETAINED_STEP_DEFAULT = "0.0000000000000"


def _cols(table: str) -> str:
    return ", ".join(column_names(table))


def clean_retained_step(cur, run_date: str) -> int:
    """Step 04: null out default retained-step codes for today's primary events."""
    cur.execute(
        "UPDATE NWK_ACTION_SECONDARY_TBL SET RETND1_STEP_CD = NULL "
        "WHERE EVENT_ID IN (SELECT EVENT_ID FROM NWK_ACTION_PRIMARY_TBL "
        "                   WHERE LOAD_DATE = ? AND EVENT_ID < ?) "
        "AND RETND1_STEP_CD = ?",
        [run_date, NINE_HUNDRED_SERIES, RETAINED_STEP_DEFAULT],
    )
    return cur.rowcount


def insert_into_all(cur, run_date: str) -> None:
    """Step 05: copy today's staged rows into the BIIS ACTION_*_ALL tables."""
    pcols = _cols("NWK_ACTION_PRIMARY_TBL")
    cur.execute(
        f"INSERT INTO ACTION_PRIMARY_ALL ({pcols}) "
        f"SELECT {pcols} FROM NWK_ACTION_PRIMARY_TBL WHERE LOAD_DATE = ?",
        [run_date],
    )
    scols = _cols("NWK_ACTION_SECONDARY_TBL")
    cur.execute(
        f"INSERT INTO ACTION_SECONDARY_ALL ({scols}) "
        f"SELECT {scols} FROM NWK_ACTION_SECONDARY_TBL "
        "WHERE EVENT_ID IN (SELECT EVENT_ID FROM NWK_ACTION_PRIMARY_TBL WHERE LOAD_DATE = ?)",
        [run_date],
    )
    rcols = _cols("NWK_ACTION_REMARKS_TBL")
    cur.execute(
        f"INSERT INTO ACTION_REMARKS_ALL ({rcols}) "
        f"SELECT {rcols} FROM NWK_ACTION_REMARKS_TBL "
        "WHERE EVENT_ID IN (SELECT EVENT_ID FROM NWK_ACTION_PRIMARY_TBL WHERE LOAD_DATE = ?)",
        [run_date],
    )


def cancelled_event_ids(cur, run_date: str):
    cur.execute(
        "SELECT BIIS_EVENT_ID FROM EHRP_RECS_TRACKING_TBL "
        "WHERE BIIS_WIP_STATUS_CHANGED_DT = ?",
        [run_date],
    )
    return [r[0] for r in cur.fetchall()]


def update_cancelled_actions(cur, run_date: str) -> None:
    """Delete then re-insert ACTION_*_ALL rows whose WIP status changed today."""
    for all_tbl, src_tbl in (
        ("ACTION_PRIMARY_ALL", "NWK_ACTION_PRIMARY_TBL"),
        ("ACTION_SECONDARY_ALL", "NWK_ACTION_SECONDARY_TBL"),
        ("ACTION_REMARKS_ALL", "NWK_ACTION_REMARKS_TBL"),
    ):
        cur.execute(
            f"DELETE FROM {all_tbl} WHERE EVENT_ID IN "
            "(SELECT BIIS_EVENT_ID FROM EHRP_RECS_TRACKING_TBL WHERE BIIS_WIP_STATUS_CHANGED_DT = ?)",
            [run_date],
        )
        cols = _cols(src_tbl)
        cur.execute(
            f"INSERT INTO {all_tbl} ({cols}) SELECT {cols} FROM {src_tbl} "
            "WHERE EVENT_ID IN (SELECT BIIS_EVENT_ID FROM EHRP_RECS_TRACKING_TBL "
            "                   WHERE BIIS_WIP_STATUS_CHANGED_DT = ?)",
            [run_date],
        )


def truncate_new_actions(cur) -> None:
    cur.execute("DELETE FROM NWK_NEW_EHRP_ACTIONS_TBL")


def run(env: str = "test", run_date: Optional[dt.date] = None) -> None:
    cfg = get_config(env)
    run_date = run_date or dt.date.today()
    rd = run_date.isoformat()

    conn = db.get_connection(cfg)
    try:
        cur = conn.cursor()
        clean_retained_step(cur, rd)
        insert_into_all(cur, rd)
        update_cancelled_actions(cur, rd)
        truncate_new_actions(cur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    notifications.send_notification(
        "EHRP2BIIS afterload completed successfully",
        f"ACTION_*_ALL updated for load date {rd}.",
        cfg,
    )


def main(argv=None) -> None:
    args = base_arg_parser("EHRP2BIIS afterload").parse_args(argv)
    run_date = dt.date.fromisoformat(args.run_date) if args.run_date else None
    run(env=args.env, run_date=run_date)
    print("EHRP2BIIS afterload OK")


if __name__ == "__main__":
    main()
