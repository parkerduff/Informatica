"""Pay Calendar workflow (migrated from XML/Pay_Calendar).

The Informatica workflow rotates the "current pay period" flag in the
``PAY_PERIOD`` table:

1. *Reset* every ``CURR_PP_FLAG`` to NULL.
2. *Set* ``CURR_PP_FLAG = 'Y'`` on exactly one period, chosen either from a
   parameter file (PP_NUM / PP_END_YEAR) or, by default, from the run date
   (the period whose start/end dates bracket the run date).
3. *Verify* exactly one period is current.
4. *Notify* with the selected period details.
"""
from __future__ import annotations

import datetime as dt
from typing import Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils import db, notifications
from utils.config import get_config
from utils.spark import get_spark, parse_args
from utils.validation import validate_exactly_one


def reset_flags(df: DataFrame) -> DataFrame:
    """Set ``CURR_PP_FLAG`` to NULL on every row (Reset workflow)."""
    return df.withColumn("CURR_PP_FLAG", F.lit(None).cast("string"))


def find_current_by_date(df: DataFrame, run_date: dt.date) -> Optional[Tuple[int, int]]:
    """Return ``(PP_NUM, PP_END_YEAR)`` of the period bracketing ``run_date``."""
    rd = run_date.isoformat()
    match = (
        df.filter((F.substring(F.col("PP_START_DTE"), 1, 10) <= rd)
                  & (F.substring(F.col("PP_END_DTE"), 1, 10) >= rd))
        .select("PP_NUM", "PP_END_YEAR")
        .limit(1)
        .collect()
    )
    if not match:
        return None
    return int(match[0]["PP_NUM"]), int(match[0]["PP_END_YEAR"])


def set_current(df: DataFrame, pp_num: int, pp_end_year: int) -> DataFrame:
    """Mark the (pp_num, pp_end_year) period current; all others NULL."""
    is_current = (F.col("PP_NUM") == F.lit(pp_num)) & (F.col("PP_END_YEAR") == F.lit(pp_end_year))
    return df.withColumn(
        "CURR_PP_FLAG",
        F.when(is_current, F.lit("Y")).otherwise(F.lit(None).cast("string")),
    )


def count_current(df: DataFrame) -> int:
    return df.filter(F.col("CURR_PP_FLAG") == "Y").count()


def build_notification(pp_num: int, pp_end_year: int) -> Tuple[str, str]:
    subject = f"Pay Calendar: current pay period set to PP {pp_num:02d}/{pp_end_year}"
    body = (
        "The Pay Calendar workflow completed successfully.\n"
        f"Current pay period: PP_NUM={pp_num}, PP_END_YEAR={pp_end_year}\n"
        "Exactly one period is now flagged current."
    )
    return subject, body


def run(env: str = "test",
        run_date: Optional[dt.date] = None,
        pp_num: Optional[int] = None,
        pp_end_year: Optional[int] = None,
        spark=None) -> Tuple[int, int]:
    cfg = get_config(env)
    spark = spark or get_spark("pay_calendar")
    run_date = run_date or dt.date.today()

    df = db.read_table(spark, "PAY_PERIOD", cfg)
    df = reset_flags(df)

    if pp_num is None or pp_end_year is None:
        found = find_current_by_date(df, run_date)
        if found is None:
            raise ValueError(f"No pay period brackets run date {run_date}")
        pp_num, pp_end_year = found

    df = set_current(df, pp_num, pp_end_year)
    validate_exactly_one(count_current(df), "current pay period")

    db.write_table(spark, df, "PAY_PERIOD", cfg, mode="overwrite")

    subject, body = build_notification(pp_num, pp_end_year)
    notifications.send_notification(subject, body, cfg)
    return pp_num, pp_end_year


def main(argv=None) -> None:
    args = parse_args("Pay Calendar workflow", argv)
    run_date = dt.date.fromisoformat(args.run_date) if args.run_date else None
    pp_num, pp_end_year = run(env=args.env, run_date=run_date)
    print(f"Pay Calendar OK: current PP_NUM={pp_num} PP_END_YEAR={pp_end_year}")


if __name__ == "__main__":
    main()
