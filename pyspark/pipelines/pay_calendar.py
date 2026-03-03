"""
Pay Calendar PySpark Pipeline.

Replaces: XML/Pay_Calendar (Informatica PowerCenter workflow wf_Pay_Calendar)

Sessions converted:
  1. s_Pay_Calendar_Reset_Pay_Calendar  - Reset current pay period flag
  2. s_Pay_Calendar_Set_Pay_Calendar    - Set new current period
  3. s_Pay_Calendar_Verify_Pay_Calendar - Verify exactly one current period
  4. s_Pay_Calendar_Build_Message       - Generate email notification

Source: PAY_PERIOD table (HISTDBA schema)
Target: PAY_PERIOD table (update CURR_PP_FLAG)
"""

import logging
from datetime import datetime

from pyspark.utils.config import AppConfig
from pyspark.utils.spark_session import create_spark_session
from pyspark.utils.db_utils import read_table, execute_sql
from pyspark.utils.logging_utils import setup_logging
from pyspark.utils.notifications import (
    send_success_notification,
    send_failure_notification,
)

logger = logging.getLogger(__name__)


def reset_pay_calendar(spark, config):
    """Step 1: Reset all CURR_PP_FLAG values to NULL.

    Equivalent to Informatica session s_Pay_Calendar_Reset_Pay_Calendar.
    """
    logger.info("Step 1: Resetting pay calendar current flags")
    execute_sql(spark, config.db, [
        "UPDATE HISTDBA.PAY_PERIOD SET CURR_PP_FLAG = NULL",
        "COMMIT",
    ])
    logger.info("All CURR_PP_FLAG values reset to NULL")


def set_pay_calendar(spark, config):
    """Step 2: Set the current pay period.

    If parameters PP_NUM and PP_END_YEAR are provided, use them to
    identify the current period. Otherwise use the system date to find
    the pay period whose start/end dates bracket today.

    Equivalent to Informatica mapping m_Pay_Calendar_Set_Pay_Calendar
    with Router transformation rtr_Parameter_Non_Parameter.
    """
    logger.info("Step 2: Setting current pay period")
    params = config.pay_calendar_params

    if params.params_exist:
        # Parameter path: look up by PP_NUM and PP_END_YEAR
        logger.info(
            "Using parameters: PP_NUM=%s, PP_END_YEAR=%s",
            params.pp_num,
            params.pp_end_year,
        )
        pay_period_df = read_table(
            spark,
            config.db,
            table_name="HISTDBA.PAY_PERIOD",
            query=(
                f"SELECT PP_NUM, PP_END_YEAR FROM HISTDBA.PAY_PERIOD "
                f"WHERE PP_NUM = {params.pp_num} "
                f"AND PP_END_YEAR = {params.pp_end_year}"
            ),
        )

        if pay_period_df.count() == 0:
            raise ValueError(
                f"No pay period found for PP_NUM={params.pp_num}, "
                f"PP_END_YEAR={params.pp_end_year}"
            )

        execute_sql(spark, config.db, [
            (
                f"UPDATE HISTDBA.PAY_PERIOD SET CURR_PP_FLAG = 'Y' "
                f"WHERE PP_NUM = {params.pp_num} "
                f"AND PP_END_YEAR = {params.pp_end_year}"
            ),
            "COMMIT",
        ])
        logger.info(
            "Current flag set via parameters: PP_NUM=%s, PP_END_YEAR=%s",
            params.pp_num,
            params.pp_end_year,
        )
    else:
        # Non-parameter path: use system date
        current_date = datetime.now().date()
        logger.info("Using system date: %s", current_date)

        pay_period_df = read_table(
            spark,
            config.db,
            table_name="HISTDBA.PAY_PERIOD",
            query=(
                f"SELECT PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE "
                f"FROM HISTDBA.PAY_PERIOD "
                f"WHERE PP_START_DTE <= TO_DATE('{current_date}', 'YYYY-MM-DD') "
                f"AND PP_END_DTE >= TO_DATE('{current_date}', 'YYYY-MM-DD')"
            ),
        )

        if pay_period_df.count() == 0:
            raise ValueError(
                f"No pay period found for current date {current_date}"
            )

        row = pay_period_df.first()
        pp_num = row["PP_NUM"]
        pp_end_year = row["PP_END_YEAR"]

        execute_sql(spark, config.db, [
            (
                f"UPDATE HISTDBA.PAY_PERIOD SET CURR_PP_FLAG = 'Y' "
                f"WHERE PP_NUM = {pp_num} "
                f"AND PP_END_YEAR = {pp_end_year}"
            ),
            "COMMIT",
        ])
        logger.info(
            "Current flag set via date: PP_NUM=%s, PP_END_YEAR=%s",
            pp_num,
            pp_end_year,
        )


def verify_pay_calendar(spark, config):
    """Step 3: Verify exactly one pay period is marked as current.

    Equivalent to Informatica mapping m_Pay_Calendar_Verify_Pay_Calendar
    with lkp_Current_Pay_Period and exp_Check_Current_Flag.

    Raises ValueError if zero or more than one period is current.
    """
    logger.info("Step 3: Verifying pay calendar")

    count_df = read_table(
        spark,
        config.db,
        table_name="HISTDBA.PAY_PERIOD",
        query=(
            "SELECT COUNT(*) AS COUNT_CURRENT "
            "FROM HISTDBA.PAY_PERIOD "
            "WHERE CURR_PP_FLAG = 'Y'"
        ),
    )
    count_current = count_df.first()["COUNT_CURRENT"]

    if count_current == 1:
        logger.info("Current flag has been properly set")
    elif count_current == 0:
        raise ValueError("There is no pay period set.")
    else:
        raise ValueError(
            "There are more than one pay periods set to current."
        )


def build_message(spark, config):
    """Step 4: Build notification message with current pay period info.

    Equivalent to Informatica session s_Pay_Calendar_Build_Message.
    """
    logger.info("Step 4: Building pay calendar notification message")

    current_pp_df = read_table(
        spark,
        config.db,
        table_name="HISTDBA.PAY_PERIOD",
        query=(
            "SELECT PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE "
            "FROM HISTDBA.PAY_PERIOD "
            "WHERE CURR_PP_FLAG = 'Y'"
        ),
    )

    row = current_pp_df.first()
    pp_num = row["PP_NUM"]
    pp_end_year = row["PP_END_YEAR"]
    pp_num_str = str(pp_num).zfill(2)

    env_prefix = f"{config.environment}: "
    subject = (
        f"{env_prefix}Pay Calendar set successfully for "
        f"Pay Period: {pp_end_year}-{pp_num_str}"
    )

    message = (
        f"Pay Period Number: {pp_num_str}\n"
        f"Pay Period Year: {pp_end_year}\n"
        f"Start Date: {row['PP_START_DTE']}\n"
        f"End Date: {row['PP_END_DTE']}\n"
    )

    return subject, message


def run(config=None):
    """Execute the full Pay Calendar workflow.

    Equivalent to Informatica workflow wf_Pay_Calendar which runs
    sessions sequentially:
      Reset -> Set -> Verify -> Build Message
    """
    if config is None:
        config = AppConfig()

    log_file = setup_logging("pay_calendar", config.paths)
    spark = create_spark_session(config, "Pay_Calendar")

    try:
        reset_pay_calendar(spark, config)
        set_pay_calendar(spark, config)
        verify_pay_calendar(spark, config)
        subject, message = build_message(spark, config)

        send_success_notification(
            config.email,
            process_name=subject,
            message=message,
            log_file=log_file,
        )
        logger.info("Pay Calendar workflow completed successfully")

    except Exception as e:
        logger.exception("Pay Calendar workflow failed")
        send_failure_notification(
            config.email,
            process_name="Pay Calendar",
            error_message=str(e),
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
