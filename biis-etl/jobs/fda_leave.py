#!/usr/bin/env python3
"""FDA Leave job -- migration of Informatica mapping ``m_0150_PM_FDA_Error_Counter``.

Validates FDA time-and-attendance transactions (HI_PM_FDA_TATRAN_TBL) against
the CPM YTD / PAD / MER detail staging tables. Any transaction whose employee
SSN is missing from one or more of those sources is routed to ERROR_TBL, then
the error count is recorded in COUNTER_TBL.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
from typing import Any

from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils.db import (get_current_pay_period, pyodbc_connection, read_table,
                      write_table)
from utils.notifications import send_notification
from utils.secrets import Config, get_db_secret, load_config
from utils.spark import get_spark
from utils.validation import log_row_count

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("fda_leave")

PROCESS_NAME = "FDA_LEAVE"
ERROR_CODE = "FDA_LKP_MISS"

# Lookup source -> (table, ssn column).
LOOKUPS = [
    ("YTD", "CPM_YTD_DETAIL_STG_TBL", "DYD_SSN_1"),
    ("PAD", "CPM_PAD_DETAIL_STG_TBL", "PAD_SOC_SEC_NO"),
    ("MER", "CPM_MER_DETAIL_STG_TBL", "MER_SSN"),
]


def build_errors(spark: Any, config: Any, secret: Any) -> Any:
    tatran = read_table(
        spark, "HI_PM_FDA_TATRAN_TBL", config, secret,
        columns=["FDA_BATCH_ID", "FDA_TK_NO", "FDA_EMP_ID", "FDA_PP_YEAR", "FDA_PP_NUM"],
    )

    joined = tatran
    miss_flags = []
    for label, table, ssn_col in LOOKUPS:
        lk = (
            read_table(spark, table, config, secret, columns=[f"{ssn_col}"])
            .select(F.col(ssn_col).alias(f"_{label}_ssn"))
            .dropDuplicates()
        )
        joined = joined.join(
            lk, joined["FDA_EMP_ID"] == lk[f"_{label}_ssn"], how="left"
        )
        miss_flags.append((label, F.col(f"_{label}_ssn").isNull()))

    # Router: a transaction is in error if any lookup missed.
    any_missing = miss_flags[0][1]
    for _, flag in miss_flags[1:]:
        any_missing = any_missing | flag
    errors = joined.filter(any_missing)

    # Deterministic message listing the missing sources in fixed order.
    missing_label = F.concat_ws(
        ",",
        *[F.when(flag, F.lit(label)) for label, flag in miss_flags],
    )
    errors = errors.withColumn(
        "ERROR_MESSAGE", F.concat(F.lit("Missing CPM detail: "), missing_label)
    )
    return errors.select("FDA_EMP_ID", "ERROR_MESSAGE").dropDuplicates(["FDA_EMP_ID"])


def run(config: Config, run_date: dt.date) -> int:  # pragma: no cover
    spark = get_spark("fda_leave")
    secret = get_db_secret(config)
    pay_period = get_current_pay_period(spark, config, secret)

    errors = build_errors(spark, config, secret)

    out = (
        errors
        .withColumn("PROCESS_NAME", F.lit(PROCESS_NAME))
        .withColumn("SOURCE_KEY", F.col("FDA_EMP_ID"))
        .withColumn("ERROR_DATE", F.lit(run_date.isoformat()).cast(T.TimestampType()))
        .withColumn("PP_END_YEAR", F.lit(pay_period["pp_end_year"]).cast(T.DecimalType(4, 0)))
        .withColumn("PP_NUM", F.lit(pay_period["pp_num"]).cast(T.DecimalType(2, 0)))
        .withColumn("CYCLE_ID", F.lit(1).cast(T.DecimalType(3, 0)))
        .withColumn("ERROR_CODE", F.lit(ERROR_CODE))
        .select("PROCESS_NAME", "ERROR_MESSAGE", "SOURCE_KEY", "ERROR_DATE",
                "PP_END_YEAR", "PP_NUM", "CYCLE_ID", "ERROR_CODE")
    )
    write_table(out, "ERROR_TBL", config, secret, mode="append")
    error_count = out.count()

    with pyodbc_connection(secret, config) as conn:
        log_row_count(conn, "ERROR_TBL", PROCESS_NAME, error_count, pay_period,
                      description="FDA leave validation errors")

    send_notification(
        f"{PROCESS_NAME}: validation complete",
        f"{error_count} FDA transactions failed CPM detail lookup for "
        f"PP {pay_period['pp_num']:02d}/{pay_period['pp_end_year']}.",
        config,
    )
    logger.info("fda_leave complete: %d errors", error_count)
    return error_count


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
