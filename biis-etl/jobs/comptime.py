"""COMPTIME workflow (migrated from XML/COMPTIME).

Reads the U0287D01 comp-time flat file (12 columns), stamps it with the
current pay period, hashes the SSN, loads ``COMP_TIME_DAILY_TBL`` and records
the row count in ``COUNTER_TBL``.
"""
from __future__ import annotations

from typing import Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from utils import db, notifications
from utils.config import get_config
from utils.schemas import column_names
from utils.spark import get_spark, parse_args
from utils.validation import ValidationError

PROCESS_NAME = "COMPTIME"

# 12-column U0287D01 flat-file layout (the 3 PP_* columns are derived).
INPUT_FIELDS = [
    ("SSN", StringType()),
    ("NAME", StringType()),
    ("CURRENT_ACCT", StringType()),
    ("CURRENT_ORG", StringType()),
    ("FLSA_STATUS", StringType()),
    ("COMP_TIME_CUR_BAL", StringType()),
    ("COMP_TIME_YEAR_EARNED", StringType()),
    ("PP_END_DATE", StringType()),
    ("DAILY_DATE_EARNED", StringType()),
    ("COMP_TIME_RATE", StringType()),
    ("COMP_TIME_HOURS", StringType()),
    ("COMP_TIME_UNDEF", StringType()),
]

INPUT_SCHEMA = StructType([StructField(n, t, True) for n, t in INPUT_FIELDS])


def derive_pp_year_num(pp_end_year: int, pp_num: int) -> int:
    """``year=2026, pp=3 -> 202603``."""
    return pp_end_year * 100 + pp_num


def parse_comptime_csv(spark, path: str) -> DataFrame:
    """Parse the comp-time CSV into a typed DataFrame (12 columns)."""
    return spark.read.csv(path, schema=INPUT_SCHEMA, header=True)


def hash_ssn(df: DataFrame) -> DataFrame:
    """Replace SSN with its SHA-256 hash (PII protection)."""
    return df.withColumn("SSN", F.sha2(F.col("SSN").cast("string"), 256))


def coerce_numerics(df: DataFrame) -> DataFrame:
    """Cast numeric columns; non-numeric values become NULL (graceful)."""
    numeric = [
        "COMP_TIME_CUR_BAL",
        "COMP_TIME_YEAR_EARNED",
        "COMP_TIME_RATE",
        "COMP_TIME_HOURS",
        "COMP_TIME_UNDEF",
    ]
    for c in numeric:
        df = df.withColumn(c, F.col(c).cast(DoubleType()))
    return df


def add_pay_period(df: DataFrame, pp_end_year: int, pp_num: int) -> DataFrame:
    """Stamp every row with the current pay period columns."""
    return (
        df.withColumn("PP_END_YEAR", F.lit(pp_end_year))
        .withColumn("PP_NUM", F.lit(pp_num))
        .withColumn("PP_YEAR_NUM", F.lit(derive_pp_year_num(pp_end_year, pp_num)))
    )


def transform(df: DataFrame, pp_end_year: int, pp_num: int) -> DataFrame:
    if df.rdd.isEmpty():
        raise ValidationError("COMPTIME input file is empty")
    df = coerce_numerics(df)
    df = hash_ssn(df)
    df = add_pay_period(df, pp_end_year, pp_num)
    return df.select(*column_names("COMP_TIME_DAILY_TBL"))


def current_pay_period(spark, cfg) -> Tuple[int, int]:
    pp = (
        db.read_table(spark, "PAY_PERIOD", cfg)
        .filter(F.col("CURR_PP_FLAG") == "Y")
        .select("PP_END_YEAR", "PP_NUM")
        .limit(1)
        .collect()
    )
    if not pp:
        raise ValidationError("No current pay period set in PAY_PERIOD")
    return int(pp[0]["PP_END_YEAR"]), int(pp[0]["PP_NUM"])


def run(env: str = "test", file_path: Optional[str] = None,
        run_date=None, spark=None) -> int:
    cfg = get_config(env)
    spark = spark or get_spark("comptime")
    if not file_path:
        raise ValueError("--file-path is required for COMPTIME")

    pp_end_year, pp_num = current_pay_period(spark, cfg)
    raw = parse_comptime_csv(spark, file_path)
    out = transform(raw, pp_end_year, pp_num)

    db.write_table(spark, out, "COMP_TIME_DAILY_TBL", cfg, mode="overwrite")
    n = out.count()

    counter = spark.createDataFrame(
        [(PROCESS_NAME, pp_end_year, pp_num, n, str(run_date) if run_date else None)],
        schema=db_counter_schema(),
    )
    db.write_table(spark, counter, "COUNTER_TBL", cfg, mode="overwrite")

    notifications.send_notification(
        f"COMPTIME loaded {n} rows for PP {pp_num:02d}/{pp_end_year}",
        f"COMP_TIME_DAILY_TBL loaded with {n} records.",
        cfg,
    )
    return n


def db_counter_schema() -> StructType:
    from pyspark.sql.types import LongType

    return StructType([
        StructField("PROCESS_NAME", StringType(), True),
        StructField("PP_END_YEAR", LongType(), True),
        StructField("PP_NUM", LongType(), True),
        StructField("COUNTER_VALUE", LongType(), True),
        StructField("RUN_DATE", StringType(), True),
    ])


def main(argv=None) -> None:
    args = parse_args("COMPTIME workflow", argv)
    n = run(env=args.env, file_path=args.file_path, run_date=args.run_date)
    print(f"COMPTIME OK: loaded {n} rows")


if __name__ == "__main__":
    main()
