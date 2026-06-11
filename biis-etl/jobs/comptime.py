#!/usr/bin/env python3
"""COMPTIME job -- migration of Informatica workflow ``wf_COMPTIME``.

Sessions:
  1. Get pay period -- resolve the current pay period (CURR_PP_FLAG = 'Y')
  2. Load file      -- parse the comp-time flat file, hash the SSN, derive
                       PP_YEAR_NUM, stamp LOAD_DATE, write COMP_TIME_DAILY_TBL
  3. Counters       -- record the load count in COUNTER_TBL and notify
"""
from __future__ import annotations

import argparse
import logging
from typing import Any

from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils.db import (get_current_pay_period, pyodbc_connection, write_table)
from utils.notifications import send_notification
from utils.secrets import Config, get_db_secret, load_config
from utils.spark import get_spark
from utils.validation import log_row_count, validate_row_count

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("comptime")

PROCESS_NAME = "COMPTIME"

# Flat-file source layout (Informatica source U0287D01), in order.
INPUT_COLUMNS = [
    "SSN", "NAME", "CURRENT_ACCT", "CURRENT_ORG", "FLSA_STATUS",
    "COMP_TIME_CUR_BAL", "COMP_TIME_YEAR_EARNED", "PP_END_DATE",
    "DAILY_DATE_EARNED", "COMP_TIME_RATE", "COMP_TIME_HOURS", "COMP_TIME_UNDEF",
]

# Target column order for COMP_TIME_DAILY_TBL (excluding LOAD_DATE which is set
# per-run and intentionally excluded from reconciliation).
TARGET_COLUMNS = [
    "PP_END_YEAR", "PP_NUM", "PP_YEAR_NUM", "SSN", "NAME", "CURRENT_ACCT",
    "CURRENT_ORG", "FLSA_STATUS", "COMP_TIME_CUR_BAL", "COMP_TIME_YEAR_EARNED",
    "PP_END_DATE", "DAILY_DATE_EARNED", "COMP_TIME_RATE", "COMP_TIME_HOURS",
    "COMP_TIME_UNDEF", "SSN_HASH",
]


def transform(df: Any, pay_period: dict) -> Any:
    """Apply the COMPTIME field derivations to the raw flat-file DataFrame."""
    out = df
    # Numeric casts.
    out = out.withColumn("COMP_TIME_CUR_BAL", F.col("COMP_TIME_CUR_BAL").cast(T.DecimalType(8, 2)))
    out = out.withColumn("COMP_TIME_YEAR_EARNED", F.col("COMP_TIME_YEAR_EARNED").cast(T.DecimalType(4, 0)))
    out = out.withColumn("COMP_TIME_RATE", F.col("COMP_TIME_RATE").cast(T.DecimalType(6, 2)))
    out = out.withColumn("COMP_TIME_HOURS", F.col("COMP_TIME_HOURS").cast(T.DecimalType(8, 2)))
    out = out.withColumn("COMP_TIME_UNDEF", F.col("COMP_TIME_UNDEF").cast(T.DecimalType(6, 0)))
    # YYYYMMDD -> timestamp (DATETIME2).
    out = out.withColumn("PP_END_DATE", F.to_timestamp(F.col("PP_END_DATE"), "yyyyMMdd"))
    out = out.withColumn("DAILY_DATE_EARNED", F.to_timestamp(F.col("DAILY_DATE_EARNED"), "yyyyMMdd"))
    # Pay-period derivations.
    out = out.withColumn("PP_END_YEAR", F.lit(pay_period["pp_end_year"]).cast(T.DecimalType(4, 0)))
    out = out.withColumn("PP_NUM", F.lit(pay_period["pp_num"]).cast(T.DecimalType(2, 0)))
    out = out.withColumn("PP_YEAR_NUM", F.lit(pay_period["pp_year_num"]).cast(T.DecimalType(6, 0)))
    # SSN hash (SHA-256) + load timestamp.
    out = out.withColumn("SSN_HASH", F.sha2(F.col("SSN"), 256))
    out = out.withColumn("LOAD_DATE", F.current_timestamp())
    return out


def run(config: Config, file_path: str) -> int:  # pragma: no cover
    spark = get_spark("comptime")
    secret = get_db_secret(config)

    pay_period = get_current_pay_period(spark, config, secret)
    logger.info("Current pay period: %s", pay_period)

    raw = (
        spark.read.option("header", True).schema(
            T.StructType([T.StructField(c, T.StringType(), True) for c in INPUT_COLUMNS])
        ).csv(file_path)
    )
    validate_row_count(raw, expected_min=1, context="comptime input file")

    enriched = transform(raw, pay_period)
    out = enriched.select(*TARGET_COLUMNS, "LOAD_DATE")
    write_table(out, "COMP_TIME_DAILY_TBL", config, secret, mode="append")

    count = out.count()
    with pyodbc_connection(secret, config) as conn:
        log_row_count(conn, "COMP_TIME_DAILY_TBL", PROCESS_NAME, count, pay_period)

    send_notification(
        f"{PROCESS_NAME}: load complete",
        f"Loaded {count} comp-time daily rows for PP {pay_period['pp_num']:02d}/"
        f"{pay_period['pp_end_year']}.",
        config,
    )
    logger.info("comptime complete: %d rows", count)
    return count


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default=None)
    ap.add_argument("--file-path", required=True)
    args = ap.parse_args()
    config = load_config(args.env)
    run(config, args.file_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
