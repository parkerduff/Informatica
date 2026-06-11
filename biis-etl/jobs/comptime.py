"""COMPTIME job — migration of Informatica workflow wf_COMPTIME.

Loads the U0287D01 comp-time CSV flat file into COMP_TIME_DAILY_TBL with the
derived PP_YEAR_NUM, a SHA-256 SSN hash, and a COUNTER_TBL audit row.
"""
import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs.spark_common import get_spark  # noqa: E402
from utils.db import get_current_pay_period, pyodbc_connection, write_table  # noqa: E402
from utils.notifications import send_notification  # noqa: E402
from utils.secrets import get_secret, load_config  # noqa: E402
from utils.validation import log_row_count, validate_row_count  # noqa: E402

logger = logging.getLogger("comptime")

SOURCE_COLUMNS = [
    "SSN", "NAME", "CURRENT_ACCT", "CURRENT_ORG", "FLSA_STATUS",
    "COMP_TIME_CUR_BAL", "COMP_TIME_YEAR_EARNED", "PP_END_DATE",
    "DAILY_DATE_EARNED", "COMP_TIME_RATE", "COMP_TIME_HOURS", "COMP_TIME_UNDEF",
]

PROCESS_NAME = "COMPTIME"


def derive_pp_year_num(pp_end_year: int, pp_num: int) -> int:
    return int(f"{pp_end_year}{pp_num:02d}")


def transform(df, pay_period: dict):
    from pyspark.sql import functions as F

    pp_year_num = derive_pp_year_num(pay_period["pp_end_year"], pay_period["pp_num"])
    return (
        df.withColumn("PP_END_YEAR", F.lit(pay_period["pp_end_year"]).cast("decimal(4,0)"))
        .withColumn("PP_NUM", F.lit(pay_period["pp_num"]).cast("decimal(2,0)"))
        .withColumn("PP_YEAR_NUM", F.lit(pp_year_num).cast("decimal(6,0)"))
        .withColumn("SSN_HASH", F.sha2(F.col("SSN").cast("string"), 256))
        .withColumn("LOAD_DATE", F.current_timestamp())
        .withColumn("COMP_TIME_CUR_BAL", F.col("COMP_TIME_CUR_BAL").cast("decimal(8,2)"))
        .withColumn("COMP_TIME_YEAR_EARNED", F.col("COMP_TIME_YEAR_EARNED").cast("decimal(4,0)"))
        .withColumn("PP_END_DATE", F.to_timestamp("PP_END_DATE", "yyyy-MM-dd"))
        .withColumn("DAILY_DATE_EARNED", F.to_timestamp("DAILY_DATE_EARNED", "yyyy-MM-dd"))
        .withColumn("COMP_TIME_RATE", F.col("COMP_TIME_RATE").cast("decimal(6,2)"))
        .withColumn("COMP_TIME_HOURS", F.col("COMP_TIME_HOURS").cast("decimal(8,2)"))
        .withColumn("COMP_TIME_UNDEF", F.col("COMP_TIME_UNDEF").cast("decimal(6,0)"))
        .select(
            "PP_END_YEAR", "PP_NUM", "PP_YEAR_NUM", "SSN", "NAME", "CURRENT_ACCT",
            "CURRENT_ORG", "FLSA_STATUS", "COMP_TIME_CUR_BAL", "COMP_TIME_YEAR_EARNED",
            "PP_END_DATE", "DAILY_DATE_EARNED", "COMP_TIME_RATE", "COMP_TIME_HOURS",
            "COMP_TIME_UNDEF", "SSN_HASH", "LOAD_DATE",
        )
    )


def run(env: str = None, file_path: str = None) -> int:
    config = load_config(env)
    secret = get_secret("biis", config)
    spark = get_spark("biis-comptime")
    try:
        pay_period = get_current_pay_period(spark, config, secret)
        df = spark.read.csv(file_path, header=True, inferSchema=False)
        missing = [c for c in SOURCE_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"Comp time file missing columns: {missing}")
        out = transform(df, pay_period)
        count = validate_row_count(out, expected_min=1, context="COMPTIME load")
        write_table(out, "COMP_TIME_DAILY_TBL", config, secret, mode="append")
        with pyodbc_connection(secret, config) as conn:
            log_row_count(conn, "COMP_TIME_DAILY_TBL", PROCESS_NAME, count, pay_period)
            conn.commit()
        send_notification(
            "COMPTIME load completed",
            f"Loaded {count} comp time rows for PP {pay_period['pp_num']}/{pay_period['pp_end_year']}",
            config,
        )
        return count
    except Exception as exc:
        send_notification("COMPTIME load FAILED", str(exc), config)
        raise
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=None)
    parser.add_argument("--file-path", required=True)
    args = parser.parse_args()
    run(args.env, args.file_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
