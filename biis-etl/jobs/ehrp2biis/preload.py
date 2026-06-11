"""EHRP2BIIS preload stage (migrated from ehrp2biis_preload / step01).

Prepares the staging tables for today's run by removing any rows left from a
previous same-day run (idempotency), so the ETL stage starts clean.
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

from utils import db, notifications
from utils.config import get_config
from utils.spark import parse_args

STAGING_TABLES = [
    "NWK_ACTION_PRIMARY_TBL",
    "NWK_ACTION_SECONDARY_TBL",
    "NWK_ACTION_REMARKS_TBL",
    "EHRP_RECS_TRACKING_TBL",
]


def run(env: str = "test", run_date: Optional[dt.date] = None, spark=None) -> None:
    cfg = get_config(env)
    run_date = run_date or dt.date.today()
    rd = run_date.isoformat()

    for table in ("NWK_ACTION_PRIMARY_TBL", "NWK_ACTION_SECONDARY_TBL"):
        db.execute(cfg, f"DELETE FROM {table} WHERE LOAD_DATE = ?", [rd])
    db.execute(cfg, "DELETE FROM NWK_ACTION_REMARKS_TBL WHERE LOAD_DATE = ?", [rd])
    db.execute(cfg, "DELETE FROM EHRP_RECS_TRACKING_TBL WHERE LOAD_DATE = ?", [rd])

    notifications.send_notification(
        "EHRP2BIIS Preload script completed successfully",
        f"Cleared same-day staging rows for {rd}.",
        cfg,
    )


def main(argv=None) -> None:
    args = parse_args("EHRP2BIIS preload", argv)
    run_date = dt.date.fromisoformat(args.run_date) if args.run_date else None
    run(env=args.env, run_date=run_date)
    print("EHRP2BIIS preload OK")


if __name__ == "__main__":
    main()
