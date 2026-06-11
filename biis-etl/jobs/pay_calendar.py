"""PySpark migration of Informatica workflow ``wf_Pay_Calendar``.

Source: ``XML/Pay_Calendar`` (Informatica PowerCenter 9.6.1).

The workflow chains four sessions, each implemented here as a function:

1. ``s_Pay_Calendar_Reset_Pay_Calendar``   -> :func:`reset_pay_calendar`
2. ``s_Pay_Calendar_Set_Pay_Calendar``     -> :func:`set_pay_calendar`
3. ``s_Pay_Calendar_Verify_Pay_Calendar``  -> :func:`verify_pay_calendar`
4. ``s_Pay_Calendar_Build_Message``        -> :func:`build_message`
"""
from __future__ import annotations

import argparse
from datetime import datetime
from typing import Any, Dict, Optional

from pyspark.sql import functions as F

from utils import config as cfg
from utils import db

ENVIRONMENT = "TEST: "

PAY_PERIOD_COLUMNS = [
    "PP_NUM",
    "PP_END_YEAR",
    "PP_START_DTE",
    "PP_END_DTE",
    "LV_NUM",
    "LV_YEAR",
    "PAY_DTE",
    "CURR_PP_FLAG",
]


# ---------------------------------------------------------------------------
# Step 1 - m_Pay_Calendar_Reset_Pay_Calendar
# ---------------------------------------------------------------------------
def reset_pay_calendar(spark, config: Dict[str, Any]) -> int:
    """Clear the current pay-period flag.

    Maps the Source Qualifier filter ``CURR_PP_FLAG = 'Y'`` + Expression
    ``o_CURR_PP_FLAG = NULL`` + Update Strategy ``DD_UPDATE``.
    Returns the number of rows that were reset.
    """
    current = db.spark_df_from_query(
        spark,
        config,
        "SELECT PP_NUM, PP_END_YEAR FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'",
    ).withColumn("o_CURR_PP_FLAG", F.lit(None).cast("string"))
    affected = current.count()
    db.execute_sql(config, "UPDATE PAY_PERIOD SET CURR_PP_FLAG = NULL WHERE CURR_PP_FLAG = 'Y'")
    return affected


# ---------------------------------------------------------------------------
# Step 2 - m_Pay_Calendar_Set_Pay_Calendar
# ---------------------------------------------------------------------------
def _is_number(value: Any) -> bool:
    """Informatica ``IS_NUMBER`` semantics for the parameter validation."""
    if value is None:
        return False
    try:
        float(str(value).strip())
        return True
    except (ValueError, TypeError):
        return False


def set_pay_calendar(
    spark,
    config: Dict[str, Any],
    run_date,
    pp_num_param: Optional[Any] = None,
    pp_end_year_param: Optional[Any] = None,
) -> Dict[str, Any]:
    """Set ``CURR_PP_FLAG='Y'`` for the active pay period.

    Implements the Router ``rtr_Parameter_Non_Parameter``:
      * PARAMETERS_EXIST  -> use ``$$PP_NUM`` / ``$$PP_END_YEAR`` when both are
        numeric and a matching PAY_PERIOD row exists (lookup match).
      * PARAMETERS_NOT_EXIST -> use ``TRUNC(SESSSTARTTIME)`` and match against
        ``PP_START_DTE <= date <= PP_END_DTE``.
    """
    run_dt = _coerce_date(run_date)

    param_exists = _is_number(pp_num_param) and _is_number(pp_end_year_param)
    selected = None
    path = "PARAMETERS_NOT_EXIST"

    if param_exists:
        pp_num = int(float(pp_num_param))
        pp_year = int(float(pp_end_year_param))
        lookup = db.spark_df_from_query(
            spark,
            config,
            "SELECT PP_NUM, PP_END_YEAR FROM PAY_PERIOD "
            f"WHERE PP_NUM = {pp_num} AND PP_END_YEAR = {pp_year}",
        )
        if lookup.count() == 1:
            path = "PARAMETERS_EXIST"
            selected = (pp_num, pp_year)

    if selected is None:
        # Date path: lookup by run date window.
        rows = db.spark_df_from_query(
            spark,
            config,
            "SELECT PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE FROM PAY_PERIOD",
        )
        match = rows.filter(
            (F.col("PP_START_DTE") <= F.lit(run_dt))
            & (F.col("PP_END_DTE") >= F.lit(run_dt))
        )
        picked = match.select("PP_NUM", "PP_END_YEAR").limit(1).collect()
        if not picked:
            raise ValueError(f"No pay period contains run_date {run_dt}")
        selected = (int(picked[0]["PP_NUM"]), int(picked[0]["PP_END_YEAR"]))

    pp_num, pp_year = selected
    db.execute_sql(
        config,
        f"UPDATE PAY_PERIOD SET CURR_PP_FLAG = 'Y' "
        f"WHERE PP_NUM = {pp_num} AND PP_END_YEAR = {pp_year}",
    )
    return {"path": path, "pp_num": pp_num, "pp_end_year": pp_year}


# ---------------------------------------------------------------------------
# Step 3 - m_Pay_Calendar_Verify_Pay_Calendar
# ---------------------------------------------------------------------------
def verify_pay_calendar(spark, config: Dict[str, Any]) -> Dict[str, Any]:
    """Verify exactly one current pay period (Lookup Sql Override + DECODE)."""
    count = db.execute_scalar(
        config, "SELECT COUNT(*) FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'"
    )
    count = int(count or 0)
    if count == 1:
        return {"current_count": 1, "status": "pass",
                "message": "!!!! Current flag has been properly set"}
    if count == 0:
        raise ValueError("!!!! There is no pay period set.")
    raise ValueError("!!!! There are more than one pay periods set to current.")


# ---------------------------------------------------------------------------
# Step 4 - m_Pay_Calendar_Build_Message
# ---------------------------------------------------------------------------
def _pad_pp_num(pp_num: int) -> str:
    """``IIF(PP_NUM < 10, LPAD(TO_CHAR(PP_NUM), 2, '0'), TO_CHAR(PP_NUM))``."""
    return f"{int(pp_num):02d}" if int(pp_num) < 10 else str(int(pp_num))


def build_message(spark, config: Dict[str, Any]) -> Dict[str, Any]:
    """Build the notification subject/body for the current pay period."""
    rows = db.spark_df_from_query(
        spark,
        config,
        "SELECT PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE "
        "FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'",
    ).collect()
    if not rows:
        raise ValueError("No current pay period to build a message for.")
    row = rows[0]
    pp_num = int(row["PP_NUM"])
    pp_year = int(row["PP_END_YEAR"])
    v_pp_num = _pad_pp_num(pp_num)
    start = _coerce_date(row["PP_START_DTE"])
    end = _coerce_date(row["PP_END_DTE"])

    subject = (
        f"{ENVIRONMENT}Pay Calendar Process Completed Successfully for: "
        f"{pp_year}-{v_pp_num}"
    )
    message = (
        f"Current Pay Period = {v_pp_num}\n"
        f"Begin Date         = {start.strftime('%m/%d/%Y')}\n"
        f"End Date           = {end.strftime('%m/%d/%Y')}"
    )
    return {
        "subject": subject,
        "message": message,
        "pp_num": pp_num,
        "pp_end_year": pp_year,
    }


# ---------------------------------------------------------------------------
# Main entry point - wf_Pay_Calendar
# ---------------------------------------------------------------------------
def run(
    spark,
    config: Dict[str, Any],
    run_date,
    pp_num_param: Optional[Any] = None,
    pp_end_year_param: Optional[Any] = None,
) -> Dict[str, Any]:
    reset_count = reset_pay_calendar(spark, config)
    set_result = set_pay_calendar(spark, config, run_date, pp_num_param, pp_end_year_param)
    verify_result = verify_pay_calendar(spark, config)
    message = build_message(spark, config)
    return {
        "status": "SUCCESS",
        "reset_count": reset_count,
        "path": set_result["path"],
        "message_subject": message["subject"],
        "message_body": message["message"],
        "pp_num": message["pp_num"],
        "pp_end_year": message["pp_end_year"],
        "verify": verify_result,
    }


def _coerce_date(value) -> datetime:
    if isinstance(value, datetime):
        return value
    if hasattr(value, "year") and not isinstance(value, str):
        return datetime(value.year, value.month, value.day)
    return datetime.strptime(str(value)[:10], "%Y-%m-%d")


def _build_spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.master("local[*]").appName("pay_calendar").getOrCreate()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Pay Calendar PySpark job")
    parser.add_argument("--env", default="test")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--pp-num", default=None)
    parser.add_argument("--pp-end-year", default=None)
    args = parser.parse_args()

    config = cfg.load_config(args.env)
    spark = _build_spark()
    try:
        result = run(spark, config, args.run_date, args.pp_num, args.pp_end_year)
        print("Pay Calendar job result:")
        for key, value in result.items():
            print(f"  {key}: {value}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
