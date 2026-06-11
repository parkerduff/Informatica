#!/usr/bin/env python3
"""EHRP2BIIS ETL -- migration of mapping ``m_EHRP2BIIS_UPDATE``.

Reads the new HR personnel actions, joins each to its PS_GVT_JOB record, assigns
a BIIS EVENT_ID from the sequence table, and writes the primary, secondary,
remarks and tracking targets. Field mapping rules live in
``jobs.ehrp2biis.mapping`` so they stay identical to the golden generator.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
from typing import Any, List

from pyspark.sql import Window
from pyspark.sql import functions as F

from jobs.common import spark_type
from jobs.ehrp2biis import mapping as M
from jobs.schemas import (NWK_ACTION_PRIMARY_TBL_COLS,
                          NWK_ACTION_SECONDARY_TBL_COLS)
from utils.db import read_table, write_table
from utils.notifications import send_notification
from utils.secrets import Config, get_db_secret, load_config
from utils.spark import get_spark
from utils.validation import validate_row_count

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("ehrp2biis.etl")

PROCESS_NAME = "EHRP2BIIS_ETL"
JOIN_KEYS = ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"]


def _build_target(joined: Any, target_cols, plan: dict, run_date: dt.date) -> Any:
    target_names = {c[0] for c in target_cols}
    event_type = M.col_type(target_cols, "EVENT_ID")
    exprs: List[Any] = [F.col("EVENT_ID").cast(spark_type(*event_type)).alias("EVENT_ID")]
    for name in plan["copy"]:
        dt_, p, s = M.col_type(target_cols, name)
        exprs.append(F.col(name).cast(spark_type(dt_, p, s)).alias(name))
    for name in plan["null"]:
        dt_, p, s = M.col_type(target_cols, name)
        exprs.append(F.lit(None).cast(spark_type(dt_, p, s)).alias(name))
    out = joined.select(*exprs)
    out = out.withColumn("LOAD_DATE", F.lit(run_date.isoformat()).cast("timestamp"))
    # LOAD_ID is NOT NULL on the primary target -- stamp the run's load id.
    if "LOAD_ID" in target_names:
        out = out.withColumn("LOAD_ID", F.lit(run_date.strftime("%Y%m%d")))
    return out


def run(config: Config, run_date: dt.date) -> dict:  # pragma: no cover
    spark = get_spark("ehrp2biis-etl")
    secret = get_db_secret(config)

    actions = read_table(spark, "NWK_NEW_EHRP_ACTIONS_TBL", config, secret,
                         columns=JOIN_KEYS)
    gvt = read_table(spark, "PS_GVT_JOB", config, secret)

    joined = actions.join(gvt, on=JOIN_KEYS, how="inner")
    validate_row_count(joined, expected_min=1, context="ehrp2biis joined actions")

    # EVENT_ID = sequence base + deterministic row number.
    seq = read_table(spark, "SEQUENCE_NUM_TBL", config, secret,
                     columns=["SEQ_NAME", "CURRENT_VALUE"]).filter(
        F.col("SEQ_NAME") == M.SEQ_NAME)
    base = int(seq.collect()[0]["CURRENT_VALUE"])
    order = Window.orderBy(*[F.col(k) for k in JOIN_KEYS])
    joined = joined.withColumn("EVENT_ID", F.lit(base) + F.row_number().over(order))

    primary = _build_target(joined, NWK_ACTION_PRIMARY_TBL_COLS, M.primary_plan(), run_date)
    secondary = _build_target(joined, NWK_ACTION_SECONDARY_TBL_COLS, M.secondary_plan(), run_date)

    # Remarks: one derived remark per action (ACTION / ACTION_REASON).
    remarks = joined.select(
        F.col("EVENT_ID").cast("decimal(15,0)").alias("EVENT_ID"),
        F.lit(1).cast("decimal(5,0)").alias("REMARK_SEQ"),
        F.col("ACTION").alias("REMARK_CD"),
        F.concat_ws("-", F.col("ACTION"), F.col("ACTION_REASON")).alias("REMARK_TEXT"),
        F.lit(run_date.isoformat()).cast("timestamp").alias("LOAD_DATE"),
    )

    # Tracking: source-mapped fields + BIIS_EVENT_ID.
    src_cols = {c.name for c in gvt.schema.fields}
    tracking_exprs = []
    for name in M.TRACKING_COLS:
        if name == "BIIS_EVENT_ID":
            tracking_exprs.append(F.col("EVENT_ID").cast("decimal(10,0)").alias(name))
        elif name == "LOAD_DATE":
            tracking_exprs.append(F.lit(run_date.isoformat()).cast("timestamp").alias(name))
        elif name in src_cols:
            tracking_exprs.append(F.col(name).alias(name))
        else:
            tracking_exprs.append(F.lit(None).cast("string").alias(name))
    tracking = joined.select(*tracking_exprs)

    write_table(primary, "NWK_ACTION_PRIMARY_TBL", config, secret, mode="append")
    write_table(secondary, "NWK_ACTION_SECONDARY_TBL", config, secret, mode="append")
    write_table(remarks, "NWK_ACTION_REMARKS_TBL", config, secret, mode="append")
    write_table(tracking, "EHRP_RECS_TRACKING_TBL", config, secret, mode="append")

    counts = {
        "primary": primary.count(),
        "secondary": secondary.count(),
        "remarks": remarks.count(),
        "tracking": tracking.count(),
    }
    send_notification(
        f"{PROCESS_NAME}: ETL complete",
        f"Wrote primary={counts['primary']} secondary={counts['secondary']} "
        f"remarks={counts['remarks']} tracking={counts['tracking']}.",
        config,
    )
    logger.info("ehrp2biis etl complete: %s", counts)
    return counts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default=None)
    ap.add_argument("--run-date", default=dt.date.today().isoformat())
    args = ap.parse_args()
    config = load_config(args.env)
    run(config, dt.date.fromisoformat(args.run_date))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
