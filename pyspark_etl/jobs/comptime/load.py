"""Replaces ``m_COMPTIME_Load_COMP_TIME_DAILY_TBL`` (XML/COMPTIME).

Data flow:
    1. Read fixed-width flat file ``U0287D01`` (12 fields).
    2. Keep numeric-SSN detail rows (exp_Initial: IS_NUMBER(SSN)).
    3. Convert PP_END_DATE / DAILY_DATE_EARNED (both YYYYMMDD).
    4. Look up the current pay period and derive PP_YEAR_NUM = year*100 + num.
    5. Write to Oracle ``COMP_TIME_DAILY_TBL``.
"""
from __future__ import annotations

import logging
from typing import Optional

from pyspark_etl.config import connections
from pyspark_etl.config.schemas import FixedField
from pyspark_etl.transforms.date_conversions import date_col
from pyspark_etl.transforms.record_type_filter import is_number
from pyspark_etl.utils.oracle_jdbc import read_table, write_table

logger = logging.getLogger(__name__)

TARGET_TABLE = "COMP_TIME_DAILY_TBL"

U0287D01 = [
    FixedField("SSN", 0, 9),
    FixedField("NAME", 9, 30),
    FixedField("CURRENT_ACCT", 39, 6),
    FixedField("CURRENT_ORG", 45, 7),
    FixedField("FLSA_STATUS", 52, 1),
    FixedField("COMP_TIME_CUR_BAL", 53, 8, "number"),
    FixedField("COMP_TIME_YEAR_EARNED", 61, 4, "number"),
    FixedField("PP_END_DATE", 65, 8),
    FixedField("DAILY_DATE_EARNED", 73, 8),
    FixedField("COMP_TIME_RATE", 81, 6, "number"),
    FixedField("COMP_TIME_HOURS", 87, 8, "number"),
    FixedField("COMP_TIME_UNDEF", 95, 6, "number"),
]


def read_fixed_width(spark, input_file: str):
    from pyspark.sql import functions as F

    raw = spark.read.text(input_file)
    return raw.select(*[
        F.substring(F.col("value"), f.spark_start, f.length).alias(f.name)
        for f in U0287D01
    ])


def run(spark, input_file: str, target_schema: Optional[str] = None,
        write: bool = True):
    from pyspark.sql import functions as F
    from pyspark.sql.functions import udf
    from pyspark.sql.types import BooleanType

    schema = target_schema or connections.SCHEMA_INFO_TARGET
    logger.info("COMPTIME load starting from %s", input_file)

    df = read_fixed_width(spark, input_file)
    df = df.filter(udf(is_number, BooleanType())(F.col("SSN")))

    pp = read_table(
        spark, "ORA_BIIS", connections.SCHEMA_HISTDBA, "PAY_PERIOD",
        columns=["PP_NUM", "PP_END_YEAR", "CURR_PP_FLAG"],
    ).filter(F.col("CURR_PP_FLAG") == "Y").select("PP_NUM", "PP_END_YEAR")

    df = df.crossJoin(F.broadcast(pp))
    df = (
        df.withColumn("PP_END_DATE", date_col(F.col("PP_END_DATE"), "YYYYMMDD"))
          .withColumn("DAILY_DATE_EARNED", date_col(F.col("DAILY_DATE_EARNED"), "YYYYMMDD"))
          .withColumn("PP_YEAR_NUM", F.col("PP_END_YEAR") * 100 + F.col("PP_NUM"))
    )

    target_cols = [
        "PP_END_YEAR", "PP_NUM", "PP_YEAR_NUM", "SSN", "NAME", "CURRENT_ACCT",
        "CURRENT_ORG", "FLSA_STATUS", "COMP_TIME_CUR_BAL",
        "COMP_TIME_YEAR_EARNED", "PP_END_DATE", "DAILY_DATE_EARNED",
        "COMP_TIME_RATE", "COMP_TIME_HOURS", "COMP_TIME_UNDEF",
    ]
    out = df.select(*target_cols)
    if write:
        write_table(out, "ORA_BIIS", schema, TARGET_TABLE)
    logger.info("COMPTIME load complete")
    return out
