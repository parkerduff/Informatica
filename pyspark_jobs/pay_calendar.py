"""
PySpark migration of Informatica workflow: wf_Pay_Calendar
Source XML: XML/Pay_Calendar

Manages the Pay Period table by resetting, setting, and verifying the current
pay period flag, then building and emailing a notification message.

Workflow sequence (strictly sequential):
    Start -> s_Pay_Calendar_Reset_Pay_Calendar
          -> s_Pay_Calendar_Set_Pay_Calendar
          -> s_Pay_Calendar_Verify_Pay_Calendar
          -> s_Pay_Calendar_Build_Message
          -> Email_Pay_Calendar

Original schedule: ONDEMAND on Prd_IS / Dom_Prd
"""

import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import (
    ENV_PREFIX,
    PerfTimer,
    execute_jdbc_statement,
    format_pp_num,
    get_jdbc_url,
    get_param,
    get_spark_session,
    logger,
    parse_parameter_file,
    read_oracle_table,
    send_email,
    write_reject_file,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
JOB_NAME = "wf_Pay_Calendar"
BAD_FILE_DIR = os.environ.get("PM_BAD_FILE_DIR", "/data/BIISINT/data/int/bad")
TARGET_FILE_DIR = os.environ.get("PM_TARGET_FILE_DIR", "/data/BIISINT/data/int/out")
PARAM_FILE = os.environ.get(
    "BIIS_PARAM_FILE", "/data/BIISINT/control/BIIS_parms.iparms"
)


# ===================================================================
# Step 1: Reset Pay Calendar  (s_Pay_Calendar_Reset_Pay_Calendar)
# ===================================================================
def step_reset_pay_calendar(
    spark: SparkSession,
    jdbc_url: str,
    connection_name: str = "INFO_TARGET",
) -> int:
    """Read PAY_PERIOD where CURR_PP_FLAG = 'Y' and reset it to NULL.

    Corresponds to mapping m_Pay_Calendar_Reset_Pay_Calendar.
    Uses Update Strategy DD_UPDATE on target RESET_PAY_PERIOD (= PAY_PERIOD).
    """
    logger.info("Step 1: Reset Pay Calendar - clearing current pay period flag")

    pay_period_df = read_oracle_table(
        spark, "PAY_PERIOD", jdbc_url=jdbc_url, connection_name=connection_name,
        predicate="CURR_PP_FLAG = 'Y'",
    )

    rows_to_reset = pay_period_df.count()
    logger.info("Found %d row(s) with CURR_PP_FLAG = 'Y'", rows_to_reset)

    if rows_to_reset == 0:
        logger.warning("No rows found with CURR_PP_FLAG = 'Y' to reset.")
        return 0

    # Perform the UPDATE via JDBC statement
    reset_sql = "UPDATE PAY_PERIOD SET CURR_PP_FLAG = NULL WHERE CURR_PP_FLAG = 'Y'"
    try:
        execute_jdbc_statement(reset_sql, jdbc_url=jdbc_url, connection_name=connection_name)
        logger.info("Reset %d row(s) CURR_PP_FLAG to NULL", rows_to_reset)
    except Exception as exc:
        # Write rejected rows to bad file
        write_reject_file(
            pay_period_df,
            os.path.join(BAD_FILE_DIR, "reset_pay_period1.bad"),
        )
        raise RuntimeError(
            f"Failed to reset pay period flag: {exc}"
        ) from exc

    return rows_to_reset


# ===================================================================
# Step 2: Set Pay Calendar  (s_Pay_Calendar_Set_Pay_Calendar)
# ===================================================================
def step_set_pay_calendar(
    spark: SparkSession,
    jdbc_url: str,
    pp_end_year_param: str,
    pp_num_param: str,
    connection_name: str = "INFO_TARGET",
) -> tuple:
    """Set the current pay period flag to 'Y'.

    Uses a Router-style branch:
      - If $$WF_PP_END_YEAR and $$WF_PP_NUM are valid numbers, use them
        (parameter-based path).
      - Otherwise, use system date to find current pay period
        (non-parameter path).

    Corresponds to mapping m_Pay_Calendar_Set_Pay_Calendar.
    Returns (pp_end_year, pp_num) of the row that was set.
    """
    logger.info("Step 2: Set Pay Calendar")

    # Determine if parameters exist (numeric)
    param_pp_end_year = 0
    param_pp_num = 0
    try:
        param_pp_end_year = int(pp_end_year_param) if pp_end_year_param else 0
    except (ValueError, TypeError):
        param_pp_end_year = 0
    try:
        param_pp_num = int(pp_num_param) if pp_num_param else 0
    except (ValueError, TypeError):
        param_pp_num = 0

    pay_period_df = read_oracle_table(
        spark, "PAY_PERIOD", jdbc_url=jdbc_url, connection_name=connection_name,
    )

    if param_pp_end_year > 0 and param_pp_num > 0:
        # ---- Parameter-based path ----
        logger.info(
            "Using parameter-based path: PP_END_YEAR=%d, PP_NUM=%d",
            param_pp_end_year,
            param_pp_num,
        )
        # Lookup: verify the pay period exists
        match_df = pay_period_df.filter(
            (F.col("PP_NUM") == param_pp_num)
            & (F.col("PP_END_YEAR") == param_pp_end_year)
        )
        match_row = match_df.first()

        if match_row is None:
            raise RuntimeError(
                f"Pay period PP_NUM={param_pp_num}, "
                f"PP_END_YEAR={param_pp_end_year} not found in PAY_PERIOD table."
            )

        target_pp_num = param_pp_num
        target_pp_end_year = param_pp_end_year
    else:
        # ---- Non-parameter path: use system date ----
        logger.info("Using system-date-based path")
        current_date = datetime.now().date()

        # Lookup: find pay period where PP_START_DTE <= today <= PP_END_DTE
        match_df = pay_period_df.filter(
            (F.col("PP_START_DTE") <= current_date)
            & (F.col("PP_END_DTE") >= current_date)
        )
        match_row = match_df.first()

        if match_row is None:
            raise RuntimeError(
                f"No pay period found covering current date {current_date}."
            )

        target_pp_num = match_row["PP_NUM"]
        target_pp_end_year = match_row["PP_END_YEAR"]
        logger.info(
            "System date matched PP_END_YEAR=%s, PP_NUM=%s",
            target_pp_end_year,
            target_pp_num,
        )

    # Perform the UPDATE: set CURR_PP_FLAG = 'Y'
    update_sql = (
        f"UPDATE PAY_PERIOD SET CURR_PP_FLAG = 'Y' "
        f"WHERE PP_NUM = {target_pp_num} AND PP_END_YEAR = {target_pp_end_year}"
    )
    try:
        execute_jdbc_statement(update_sql, jdbc_url=jdbc_url, connection_name=connection_name)
        logger.info(
            "Set CURR_PP_FLAG = 'Y' for PP_END_YEAR=%s, PP_NUM=%s",
            target_pp_end_year,
            target_pp_num,
        )
    except Exception as exc:
        write_reject_file(
            match_df,
            os.path.join(BAD_FILE_DIR, "pay_period1.bad"),
        )
        raise RuntimeError(
            f"Failed to set current pay period: {exc}"
        ) from exc

    return (int(target_pp_end_year), int(target_pp_num))


# ===================================================================
# Step 3: Verify Pay Calendar  (s_Pay_Calendar_Verify_Pay_Calendar)
# ===================================================================
def step_verify_pay_calendar(
    spark: SparkSession,
    jdbc_url: str,
    connection_name: str = "INFO_TARGET",
) -> None:
    """Verify that exactly one row in PAY_PERIOD has CURR_PP_FLAG = 'Y'.

    Corresponds to mapping m_Pay_Calendar_Verify_Pay_Calendar.
    Aborts if count != 1 (replicating Informatica ABORT() expression).
    """
    logger.info("Step 3: Verify Pay Calendar")

    # SQL override from XML: SELECT COUNT(*) FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'
    count_df = read_oracle_table(
        spark, "PAY_PERIOD", jdbc_url=jdbc_url, connection_name=connection_name,
        predicate="CURR_PP_FLAG = 'Y'",
    )
    count = count_df.count()

    if count == 0:
        raise RuntimeError(
            "!!!! There is no pay period set. "
            "Expected exactly 1 current pay period, found 0."
        )
    elif count > 1:
        raise RuntimeError(
            f"!!!! There are more than one pay periods set to current. "
            f"Expected 1, found {count}."
        )

    logger.info("Verification passed: exactly 1 current pay period found.")


# ===================================================================
# Step 4: Build Message  (s_Pay_Calendar_Build_Message)
# ===================================================================
def step_build_message(
    pp_end_year: int,
    pp_num: int,
) -> tuple:
    """Build the email subject and message body.

    Corresponds to mapping m_Pay_Calendar_Build_Message.
    Returns (subject, message).
    """
    logger.info("Step 4: Build Message")

    pp_num_str = format_pp_num(pp_num)

    subject = (
        f"{ENV_PREFIX}Pay Calendar has been set for "
        f"Pay Period: {pp_end_year}-{pp_num_str}"
    )
    message = (
        f"The current pay period has been set to "
        f"Pay Period: {pp_end_year}-{pp_num_str}"
    )

    # Write to flat file target PAY_PERIOD_MESSAGE_FILE
    message_file = os.path.join(TARGET_FILE_DIR, "pay_period_message.txt")
    os.makedirs(os.path.dirname(message_file) or ".", exist_ok=True)
    with open(message_file, "w") as fh:
        fh.write(f"{subject}\n{message}\n")
    logger.info("Message file written to %s", message_file)

    return (subject, message)


# ===================================================================
# Step 5: Email  (Email_Pay_Calendar)
# ===================================================================
def step_email_pay_calendar(
    subject: str,
    message: str,
    email_list: str,
) -> None:
    """Send pay calendar notification email.

    Corresponds to Email_Pay_Calendar task.
    """
    logger.info("Step 5: Email Pay Calendar")

    recipients = [addr.strip() for addr in email_list.split(";") if addr.strip()]
    if not recipients:
        recipients = [addr.strip() for addr in email_list.split() if addr.strip()]

    send_email(subject=subject, body=message, recipients=recipients or None)


# ===================================================================
# Main workflow
# ===================================================================
def run_pay_calendar() -> None:
    """Execute the full wf_Pay_Calendar workflow."""
    spark = get_spark_session(JOB_NAME)
    jdbc_url = get_jdbc_url()

    # Load parameters
    params = parse_parameter_file(PARAM_FILE)
    pp_end_year_param = get_param(params, "$$WF_PP_END_YEAR", "WF_PP_END_YEAR")
    pp_num_param = get_param(params, "$$WF_PP_NUM", "WF_PP_NUM")
    email_list = get_param(
        params,
        "$$WF_PAY_CALENDAR_EMAIL_LIST",
        "WF_PAY_CALENDAR_EMAIL_LIST",
        default="peter.chen@hhs.gov nathan.knight@hhs.gov marvin.simon@hhs.gov",
    )

    perf = PerfTimer(JOB_NAME)
    src_rows = 0
    tgt_rows = 0

    with perf:
        try:
            # Step 1: Reset
            src_rows = step_reset_pay_calendar(spark, jdbc_url)

            # Step 2: Set
            pp_end_year, pp_num = step_set_pay_calendar(
                spark, jdbc_url, pp_end_year_param, pp_num_param,
            )
            tgt_rows = 1

            # Step 3: Verify
            step_verify_pay_calendar(spark, jdbc_url)

            # Step 4: Build Message
            subject, message = step_build_message(pp_end_year, pp_num)

            # Step 5: Email
            step_email_pay_calendar(subject, message, email_list)

            logger.info("Workflow %s completed successfully.", JOB_NAME)

        except Exception as exc:
            logger.error("Workflow %s FAILED: %s", JOB_NAME, exc)
            send_email(
                subject=f"{ENV_PREFIX}{JOB_NAME} FAILED",
                body=f"Workflow {JOB_NAME} failed with error:\n\n{exc}",
            )
            raise

    # Log performance
    perf.log_to_csv(
        "/data/BIISINT/data/int/log/migration_perf_log.csv",
        src_rows=src_rows,
        tgt_rows=tgt_rows,
    )
    try:
        perf.log_to_table(spark, src_rows=src_rows, tgt_rows=tgt_rows)
    except Exception:
        logger.warning("Could not write perf metrics to MIGRATION_PERF_LOG table", exc_info=True)

    spark.stop()


if __name__ == "__main__":
    run_pay_calendar()
