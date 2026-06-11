"""FDA Leave job — migration of the Informatica FDA_Leave workflow.

Validates HI_PM_FDA_TATRAN_TBL records against the CPM YTD / PAD / MER staging
tables; records that fail any lookup are routed to ERROR_TBL with a
description, and counts are written to COUNTER_TBL.
"""
import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs.spark_common import get_spark  # noqa: E402
from utils.db import (  # noqa: E402
    get_current_pay_period, pyodbc_connection, read_table, write_table,
)
from utils.notifications import send_notification  # noqa: E402
from utils.secrets import get_secret, load_config  # noqa: E402
from utils.validation import log_row_count  # noqa: E402

logger = logging.getLogger("fda_leave")

PROCESS_NAME = "FDA_LEAVE"

TATRAN_COLUMNS = [
    "FDA_BATCH_ID", "FDA_TK_NO", "FDA_EMP_ID", "FDA_PP_YEAR", "FDA_PP_NUM",
    "FDA_REC_TYPE", "FDA_SEQ", "FDA_DATA",
]
STG_COLUMNS = ["EMP_ID", "PP_YEAR", "PP_NUM"]


def build_errors(tatran, ytd, pad, mer):
    """Router logic: a TATRAN record errors when its employee/pay-period key
    is missing from any of the three CPM staging tables."""
    from pyspark.sql import functions as F

    def flag(stg, name):
        keyed = stg.select(
            F.trim(F.col("EMP_ID")).alias("EMP_ID"),
            F.col("PP_YEAR").cast("int").alias("PP_YEAR"),
            F.col("PP_NUM").cast("int").alias("PP_NUM"),
        ).dropDuplicates().withColumn(f"__in_{name}", F.lit(1))
        return keyed

    base = (
        tatran
        .withColumn("__EMP", F.trim(F.col("FDA_EMP_ID")))
        .withColumn("__YEAR", F.col("FDA_PP_YEAR").cast("int"))
        .withColumn("__NUM", F.col("FDA_PP_NUM").cast("int"))
    )
    for stg, name in ((ytd, "ytd"), (pad, "pad"), (mer, "mer")):
        keyed = flag(stg, name)
        base = base.join(
            keyed,
            (base["__EMP"] == keyed["EMP_ID"])
            & (base["__YEAR"] == keyed["PP_YEAR"])
            & (base["__NUM"] == keyed["PP_NUM"]),
            "left",
        ).drop("EMP_ID", "PP_YEAR", "PP_NUM")

    missing_desc = F.concat_ws(
        "; ",
        F.when(F.col("__in_ytd").isNull(), F.lit("Missing CPM YTD detail record")),
        F.when(F.col("__in_pad").isNull(), F.lit("Missing CPM PAD detail record")),
        F.when(F.col("__in_mer").isNull(), F.lit("Missing CPM MER detail record")),
    )
    errors = (
        base.filter(
            F.col("__in_ytd").isNull() | F.col("__in_pad").isNull() | F.col("__in_mer").isNull()
        )
        .withColumn("ERROR_MESSAGE", missing_desc)
        .withColumn(
            "SOURCE_KEY",
            F.concat_ws("|", F.col("__EMP"), F.col("__YEAR").cast("string"),
                        F.col("__NUM").cast("string")),
        )
    )
    return errors


def run(env: str = None, run_date: str = None) -> int:
    config = load_config(env)
    secret = get_secret("biis", config)
    spark = get_spark("biis-fda-leave")
    try:
        pay_period = get_current_pay_period(spark, config, secret)
        tatran = read_table(spark, "HI_PM_FDA_TATRAN_TBL", config, secret, columns=TATRAN_COLUMNS)
        ytd = read_table(spark, "CPM_YTD_DETAIL_STG_TBL", config, secret, columns=STG_COLUMNS)
        pad = read_table(spark, "CPM_PAD_DETAIL_STG_TBL", config, secret, columns=STG_COLUMNS)
        mer = read_table(spark, "CPM_MER_DETAIL_STG_TBL", config, secret, columns=STG_COLUMNS)

        from pyspark.sql import functions as F

        errors = build_errors(tatran, ytd, pad, mer)
        error_rows = (
            errors.select(
                F.lit(PROCESS_NAME).alias("PROCESS_NAME"),
                F.col("ERROR_MESSAGE"),
                F.col("SOURCE_KEY"),
                F.current_timestamp().alias("ERROR_DATE"),
                F.lit(pay_period["pp_end_year"]).cast("decimal(4,0)").alias("PP_END_YEAR"),
                F.lit(pay_period["pp_num"]).cast("decimal(2,0)").alias("PP_NUM"),
            )
        )
        n_errors = error_rows.count()
        if n_errors:
            write_table(error_rows, "ERROR_TBL", config, secret, mode="append")
        with pyodbc_connection(secret, config) as conn:
            log_row_count(conn, "ERROR_TBL", PROCESS_NAME, n_errors, pay_period)
            conn.commit()
        send_notification(
            "FDA Leave validation completed",
            f"{n_errors} TATRAN records failed CPM staging lookups",
            config,
        )
        return n_errors
    except Exception as exc:
        send_notification("FDA Leave validation FAILED", str(exc), config)
        raise
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=None)
    parser.add_argument("--run-date", default=None)
    args = parser.parse_args()
    run(args.env, args.run_date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
