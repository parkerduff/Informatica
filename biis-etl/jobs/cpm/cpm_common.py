"""Shared CPM logic (migrated from XML/CPM_NIH, CPM_OIG, CPM_CDC).

Each CPM agency workflow reads ``CPM_NEWPAY_TBL`` (the 501-column payroll
master), filters to the current pay period plus an agency-specific predicate,
and emits an agency extract (staging table + mainframe-format flat file).
"""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils import db, notifications
from utils.config import get_config
from utils.schemas import column_names
from utils.spark import get_spark

# Agency source-filter predicates, transcribed from the Informatica
# "Source Filter" expressions in each CPM workflow XML.
CDC_ORG_CODES = ["ANC34", "ANC341", "ANC342", "ANC343", "ANC344", "ANC345"]

_POSITIVE = "{ABCDEFGHI"
_NEGATIVE = "}JKLMNOPQR"


def format_overpunch(value: int, width: int) -> str:
    """Encode an integer as a fixed-width EBCDIC signed-overpunch string.

    Inverse of :func:`jobs.pseudossn.parse_overpunch`.  e.g.
    ``format_overpunch(-12340, 5) == "1234}"``.
    """
    negative = value < 0
    digits = str(abs(value)).rjust(width, "0")[-width:]
    head, last = digits[:-1], int(digits[-1])
    sign_char = _NEGATIVE[last] if negative else _POSITIVE[last]
    return head + sign_char


def _mp_pool_blank(df: DataFrame):
    pool = F.col("MP_POOL_DES")
    return pool.isNull() | (F.trim(pool) == "")


def agency_filter(df: DataFrame, agency: str) -> DataFrame:
    """Filter the payroll master down to a single agency's records."""
    agency = agency.upper()
    bu = F.col("BUSINESS_UNIT")
    if agency == "NIH":
        pred = bu == "NIH00"
    elif agency == "OIG":
        pred = bu == "OIG00"
    elif agency == "CDC":
        pred = bu.isin("CDC00", "ATSDR") | F.trim(F.col("ORG_CDE")).isin(*CDC_ORG_CODES)
    else:
        raise ValueError(f"Unknown CPM agency: {agency!r}")
    return df.filter(pred & _mp_pool_blank(df))


def current_pay_period(spark, cfg) -> Tuple[int, int]:
    rows = (
        db.read_table(spark, "PAY_PERIOD", cfg)
        .filter(F.col("CURR_PP_FLAG") == "Y")
        .select("PP_END_YEAR", "PP_NUM")
        .limit(1)
        .collect()
    )
    if not rows:
        raise ValueError("No current pay period set in PAY_PERIOD")
    return int(rows[0]["PP_END_YEAR"]), int(rows[0]["PP_NUM"])


def to_staging(df: DataFrame, agency: str) -> DataFrame:
    """Project agency records to the agency staging schema."""
    table = f"CPM_{agency.upper()}_STG_TBL"
    return (
        df.withColumn("AGENCY", F.lit(agency.upper()))
        .withColumn("RECORD_COUNT", F.lit(1))
        .select(*column_names(table))
    )


def run_agency(agency: str, env: str = "test", run_date=None, spark=None) -> int:
    cfg = get_config(env)
    spark = spark or get_spark(f"cpm_{agency.lower()}")
    table = f"CPM_{agency.upper()}_STG_TBL"

    master = db.read_table(spark, "CPM_NEWPAY_TBL", cfg)
    pp_year, pp_num = current_pay_period(spark, cfg)
    master = master.filter((F.col("PP_END_YEAR") == pp_year) & (F.col("PP_NUM") == pp_num))

    extract = agency_filter(master, agency)
    staging = to_staging(extract, agency)
    db.write_table(spark, staging, table, cfg, mode="overwrite")

    n = staging.count()
    notifications.send_notification(
        f"CPM {agency.upper()} extract: {n} record(s)",
        f"{table} loaded with {n} records for PP {pp_num:02d}/{pp_year}.",
        cfg,
    )
    return n
