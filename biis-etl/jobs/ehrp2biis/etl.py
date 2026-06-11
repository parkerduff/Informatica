"""EHRP2BIIS ETL stage (migrated from XML/EHRP2BIIS_UPDATE).

Joins the new EHRP action keys (``NWK_NEW_EHRP_ACTIONS_TBL``) to
``PS_GVT_JOB``, assigns sequential EVENT_IDs from ``SEQUENCE_NUM_TBL`` and loads
the primary / secondary / remarks staging tables plus the
``EHRP_RECS_TRACKING_TBL`` audit table.
"""
from __future__ import annotations

import datetime as dt
from typing import Optional

from pyspark.sql import Window
from pyspark.sql import functions as F

from jobs.ehrp2biis.common import JOIN_KEYS, build_full_df, get_sequence, set_sequence
from utils import db, notifications
from utils.config import get_config
from utils.spark import get_spark, parse_args

RETAINED_STEP_DEFAULT = "0.0000000000000"


def join_actions_to_gvt_job(new_actions, gvt_job):
    """Inner-join new action keys to PS_GVT_JOB on the 4-part key."""
    return new_actions.join(gvt_job, on=JOIN_KEYS, how="inner")


def assign_event_ids(matched, base: int):
    w = Window.orderBy(*JOIN_KEYS)
    return matched.withColumn("EVENT_ID", (F.lit(base) + F.row_number().over(w)).cast("long"))


def run(env: str = "test", run_date: Optional[dt.date] = None, spark=None) -> int:
    cfg = get_config(env)
    spark = spark or get_spark("ehrp2biis_etl")
    run_date = run_date or dt.date.today()
    rd = run_date.isoformat()

    new_actions = db.read_table(spark, "NWK_NEW_EHRP_ACTIONS_TBL", cfg)
    gvt_job = db.read_table(spark, "PS_GVT_JOB", cfg)

    base = get_sequence(cfg)
    matched = assign_event_ids(join_actions_to_gvt_job(new_actions, gvt_job), base).cache()
    n = matched.count()

    primary = build_full_df(matched, "NWK_ACTION_PRIMARY_TBL",
                            overrides={"EVENT_ID": F.col("EVENT_ID"), "LOAD_DATE": F.lit(rd)})
    secondary = build_full_df(matched, "NWK_ACTION_SECONDARY_TBL",
                              overrides={"EVENT_ID": F.col("EVENT_ID"),
                                         "LOAD_DATE": F.lit(rd),
                                         "RETND1_STEP_CD": F.lit(RETAINED_STEP_DEFAULT)})
    remarks = build_full_df(matched, "NWK_ACTION_REMARKS_TBL",
                            overrides={"EVENT_ID": F.col("EVENT_ID"),
                                       "REMARK_SEQ": F.lit(1),
                                       "REMARK_CD": F.lit("GEN"),
                                       "REMARK_TEXT": F.lit("Auto-generated remark from EHRP2BIIS load"),
                                       "LOAD_DATE": F.lit(rd)})
    tracking = build_full_df(matched, "EHRP_RECS_TRACKING_TBL",
                             overrides={"BIIS_EVENT_ID": F.col("EVENT_ID"),
                                        "EVENT_SUBMITTED_DT": F.lit(rd),
                                        "LOAD_DATE": F.lit(rd)})

    db.write_table(spark, primary, "NWK_ACTION_PRIMARY_TBL", cfg, mode="append")
    db.write_table(spark, secondary, "NWK_ACTION_SECONDARY_TBL", cfg, mode="append")
    db.write_table(spark, remarks, "NWK_ACTION_REMARKS_TBL", cfg, mode="append")
    db.write_table(spark, tracking, "EHRP_RECS_TRACKING_TBL", cfg, mode="append")

    set_sequence(cfg, base + n)

    notifications.send_notification(
        f"EHRP2BIIS ETL loaded {n} action(s)",
        f"Assigned EVENT_IDs {base + 1}..{base + n} for load date {rd}.",
        cfg,
    )
    return n


def main(argv=None) -> None:
    args = parse_args("EHRP2BIIS ETL", argv)
    run_date = dt.date.fromisoformat(args.run_date) if args.run_date else None
    n = run(env=args.env, run_date=run_date)
    print(f"EHRP2BIIS ETL OK: {n} action(s) loaded")


if __name__ == "__main__":
    main()
