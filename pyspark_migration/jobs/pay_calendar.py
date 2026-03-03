"""
PySpark job: Pay Calendar

Replaces Informatica PowerCenter workflow ``wf_Pay_Calendar``
(source file: XML/Pay_Calendar).

Processing flow (4 sessions executed in order):
  1. Reset  — Set all CURR_PP_FLAG to 'N'
  2. Set    — Flag the correct pay period as current ('Y')
  3. Verify — Confirm exactly one current pay period exists
  4. Build Message & Email — Notify stakeholders

The Router transformation ``rtr_Parameter_Non_Parameter`` is replaced by
a simple Python if/else: if parameter values are provided, use them;
otherwise, derive from the system date.
"""

import logging
import os
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark_migration.utils.db import (
    get_tgt_jdbc_url,
    get_tgt_jdbc_properties,
    get_tgt_cx_connection,
    execute_sql,
)
from pyspark_migration.utils.email import send_email

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Step 1: Reset Pay Calendar
# Replaces s_Pay_Calendar_Reset_Pay_Calendar
# ---------------------------------------------------------------------------

def reset_pay_calendar() -> None:
    """Clear the current pay period flag on all rows.

    Since PySpark cannot perform in-place UPDATEs, this uses a raw SQL
    UPDATE via cx_Oracle.
    """
    conn = get_tgt_cx_connection()
    try:
        logger.info("Resetting all CURR_PP_FLAG to 'N'")
        execute_sql(conn, "UPDATE PAY_PERIOD SET CURR_PP_FLAG = 'N'")
        logger.info("Pay calendar reset complete")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Step 2: Set Pay Calendar
# Replaces m_Pay_Calendar_Set_Pay_Calendar with Router rtr_Parameter_Non_Parameter
# ---------------------------------------------------------------------------

def set_pay_calendar(
    param_pp_num: int = None,
    param_pp_end_year: int = None,
) -> None:
    """Flag the correct pay period as current.

    If ``param_pp_num`` and ``param_pp_end_year`` are provided (from
    Airflow Variables / CLI args replacing $$MAP_PP_NUM and
    $$MAP_PP_END_YEAR), use those to locate the pay period row.

    Otherwise, find the row where today falls between PP_START_DTE and
    PP_END_DTE (replacing the ``exp_Set_Date`` + ``lkp_Current_Pay_Period``
    non-parameter path).
    """
    conn = get_tgt_cx_connection()
    try:
        if param_pp_num and param_pp_end_year:
            logger.info(
                "Setting current pay period from parameters: PP_NUM=%s, PP_END_YEAR=%s",
                param_pp_num, param_pp_end_year,
            )
            execute_sql(conn, f"""
                UPDATE PAY_PERIOD
                SET CURR_PP_FLAG = 'Y'
                WHERE PP_NUM = {int(param_pp_num)}
                  AND PP_END_YEAR = {int(param_pp_end_year)}
            """)
        else:
            logger.info(
                "Setting current pay period from system date: %s", date.today()
            )
            execute_sql(conn, """
                UPDATE PAY_PERIOD
                SET CURR_PP_FLAG = 'Y'
                WHERE PP_START_DTE <= TRUNC(SYSDATE)
                  AND PP_END_DTE >= TRUNC(SYSDATE)
            """)
        logger.info("Pay calendar set complete")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Step 3: Verify Pay Calendar
# Replaces s_Pay_Calendar_Verify_Pay_Calendar
# ---------------------------------------------------------------------------

def verify_pay_calendar(spark: SparkSession) -> None:
    """Verify that exactly one pay period is flagged as current.

    Replaces the ``lkp_Pay_Calendar_Verify`` lookup which counts rows
    with CURR_PP_FLAG = 'Y', and the ``exp_Check_Current_Flag`` expression
    that ABORTs when the count is not exactly 1.
    """
    url = get_tgt_jdbc_url()
    props = get_tgt_jdbc_properties()

    count = (
        spark.read.jdbc(url, "PAY_PERIOD", properties=props)
        .filter(col("CURR_PP_FLAG") == "Y")
        .count()
    )

    if count == 0:
        raise RuntimeError("!!!! There is no pay period set.")
    if count > 1:
        raise RuntimeError(
            "!!!! There are more than one pay periods set to current."
        )

    logger.info("Pay calendar verification passed: exactly 1 current pay period")


# ---------------------------------------------------------------------------
# Step 4: Build Message & Email
# Replaces s_Pay_Calendar_Build_Message + Email_Pay_Calendar
# ---------------------------------------------------------------------------

def build_message_and_email(spark: SparkSession) -> None:
    """Build the notification message and send the email.

    Reads the current pay period details to compose the subject and body,
    then sends via smtplib.  The recipient list comes from the Airflow
    Variable / env var replacing $$WF_PAY_CALENDAR_EMAIL_LIST.
    """
    url = get_tgt_jdbc_url()
    props = get_tgt_jdbc_properties()

    current_pp = (
        spark.read.jdbc(url, "PAY_PERIOD", properties=props)
        .filter(col("CURR_PP_FLAG") == "Y")
        .collect()
    )

    if not current_pp:
        raise RuntimeError("No current pay period found for email notification")

    row = current_pp[0]
    pp_num = row["PP_NUM"]
    pp_end_year = row["PP_END_YEAR"]
    pp_start = row["PP_START_DTE"]
    pp_end = row["PP_END_DTE"]

    pp_num_str = str(pp_num).zfill(2)

    subject = os.environ.get(
        "WF_SUBJECT",
        f"Pay Calendar updated: Current Pay Period {pp_end_year}-{pp_num_str}",
    )
    message = os.environ.get(
        "WF_MESSAGE",
        (
            f"Current pay period has been set to PP {pp_num_str} "
            f"of year {pp_end_year} "
            f"(Start: {pp_start}, End: {pp_end})."
        ),
    )

    # Recipient list from env var (comma-separated) or default
    email_list_str = os.environ.get("WF_PAY_CALENDAR_EMAIL_LIST", "")
    recipients = (
        [e.strip() for e in email_list_str.split(",") if e.strip()]
        if email_list_str
        else None  # falls back to DEFAULT_RECIPIENTS in email module
    )

    send_email(subject=subject, body=message, recipients=recipients)
    logger.info("Pay calendar notification email sent")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(
    param_pp_num: int = None,
    param_pp_end_year: int = None,
) -> None:
    """Execute the full Pay Calendar workflow."""
    spark = (
        SparkSession.builder
        .appName("Pay_Calendar")
        .getOrCreate()
    )

    try:
        logger.info("Starting Pay Calendar workflow")

        # Step 1 — Reset
        reset_pay_calendar()

        # Step 2 — Set
        set_pay_calendar(param_pp_num, param_pp_end_year)

        # Step 3 — Verify
        verify_pay_calendar(spark)

        # Step 4 — Build Message & Email
        build_message_and_email(spark)

        logger.info("Pay Calendar workflow completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Pay Calendar PySpark job")
    parser.add_argument("--pp-num", type=int, default=None, help="Pay period number")
    parser.add_argument("--pp-end-year", type=int, default=None, help="Pay period end year")
    args = parser.parse_args()

    run(param_pp_num=args.pp_num, param_pp_end_year=args.pp_end_year)
