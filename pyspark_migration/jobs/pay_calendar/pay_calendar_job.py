"""
Pay Calendar Job

Implements the 5-step Reset-Set-Verify-Message-Email workflow.
Reference: XML/Pay_Calendar (lines 5, 45-46, 86-91)

Replaces Informatica workflow wf_Pay_Calendar with sessions:
1. s_Pay_Calendar_Reset_Pay_Calendar   - Clears CURR_PP_FLAG
2. s_Pay_Calendar_Set_Pay_Calendar     - Sets current pay period
3. s_Pay_Calendar_Verify_Pay_Calendar  - Validates exactly one current period
4. s_Pay_Calendar_Build_Message        - Generates notification string
5. Email notification                  - Sends results
"""

import argparse
import logging
import sys

from pyspark_migration.common.db_utils import execute_sql, read_oracle_query
from pyspark_migration.common.notification import send_email
from pyspark_migration.common.spark_session import get_spark_session

logger = logging.getLogger(__name__)


def step_reset(connection_name="ORA_BIIS"):
    """
    Step 1: Reset all pay period flags to NULL.

    Replaces session s_Pay_Calendar_Reset_Pay_Calendar:
        UPDATE PAY_PERIOD SET CURR_PP_FLAG = NULL

    Parameters
    ----------
    connection_name : str
    """
    logger.info("Step 1: Resetting all pay period flags")
    sql = "UPDATE HISTDBA.PAY_PERIOD SET CURR_PP_FLAG = NULL"
    rows = execute_sql(connection_name, sql)
    logger.info("Reset complete. %d rows updated.", rows)


def step_set(pp_num=None, pp_end_year=None, connection_name="ORA_BIIS"):
    """
    Step 2: Set the current pay period.

    Replaces session s_Pay_Calendar_Set_Pay_Calendar and the Router
    rtr_Parameter_Non_Parameter logic:
    - If parameters provided: SET WHERE PP_NUM and PP_END_YEAR match
    - If no parameters:       SET WHERE SYSDATE falls between start and end

    Parameters
    ----------
    pp_num : int, optional
    pp_end_year : int, optional
    connection_name : str
    """
    if pp_num is not None and pp_end_year is not None:
        logger.info(
            "Step 2: Setting pay period with parameters PP_NUM=%d, PP_END_YEAR=%d",
            pp_num, pp_end_year,
        )
        sql = (
            "UPDATE HISTDBA.PAY_PERIOD SET CURR_PP_FLAG = 'Y' "
            "WHERE PP_NUM = :pp_num AND PP_END_YEAR = :pp_end_year"
        )
        rows = execute_sql(connection_name, sql, {"pp_num": pp_num, "pp_end_year": pp_end_year})
    else:
        logger.info("Step 2: Setting pay period based on current date (SYSDATE)")
        sql = (
            "UPDATE HISTDBA.PAY_PERIOD SET CURR_PP_FLAG = 'Y' "
            "WHERE PP_START_DTE <= SYSDATE AND PP_END_DTE >= SYSDATE"
        )
        rows = execute_sql(connection_name, sql)

    logger.info("Set complete. %d rows updated.", rows)


def step_verify(spark, connection_name="ORA_BIIS"):
    """
    Step 3: Verify exactly one current pay period exists.

    Replaces session s_Pay_Calendar_Verify_Pay_Calendar and the lookup
    lkp_Current_Pay_Period (Pay_Calendar lines 86-91) that checks
    COUNT_CURRENT >= in_MIN_PP_NUM.

    Parameters
    ----------
    spark : SparkSession
    connection_name : str

    Returns
    -------
    dict
        Current pay period details.

    Raises
    ------
    SystemExit
        If count != 1.
    """
    logger.info("Step 3: Verifying current pay period count")

    count_df = read_oracle_query(
        spark,
        "SELECT COUNT(*) AS CNT FROM HISTDBA.PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'",
        connection_name,
    )
    count = count_df.collect()[0]["CNT"]

    if count == 0:
        logger.error("!!!! There is no pay period set.")
        sys.exit(1)
    elif count > 1:
        logger.error("!!!! There are more than one pay periods set to current.")
        sys.exit(1)

    logger.info("Verification passed: exactly 1 current pay period.")

    details_df = read_oracle_query(
        spark,
        "SELECT PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE, LV_NUM, LV_YEAR, PAY_DTE "
        "FROM HISTDBA.PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'",
        connection_name,
    )
    row = details_df.collect()[0]
    return row.asDict()


def step_build_message(pp_details):
    """
    Step 4: Build notification message.

    Replaces session s_Pay_Calendar_Build_Message and the expression
    transformations that format the pay period details.

    Parameters
    ----------
    pp_details : dict
        Current pay period details from step_verify.

    Returns
    -------
    tuple of (str, str)
        (subject, message_body)
    """
    logger.info("Step 4: Building notification message")

    pp_num = pp_details.get("PP_NUM", "")
    pp_end_year = pp_details.get("PP_END_YEAR", "")
    pp_start_dte = pp_details.get("PP_START_DTE", "")
    pp_end_dte = pp_details.get("PP_END_DTE", "")
    pay_dte = pp_details.get("PAY_DTE", "")

    subject = f"Pay Calendar set for Pay Period {pp_end_year}-{str(pp_num).zfill(2)}"

    message = (
        f"The current pay period has been set:\n"
        f"  Pay Period Number:  {pp_num}\n"
        f"  Pay Period Year:    {pp_end_year}\n"
        f"  Start Date:         {pp_start_dte}\n"
        f"  End Date:           {pp_end_dte}\n"
        f"  Pay Date:           {pay_dte}\n"
    )

    return subject, message


def step_send_email(subject, message):
    """
    Step 5: Send email notification.

    Parameters
    ----------
    subject : str
    message : str
    """
    logger.info("Step 5: Sending email notification")
    send_email(subject, message)


def run(pp_num=None, pp_end_year=None, environment=None):
    """
    Execute the full Pay Calendar workflow.

    Parameters
    ----------
    pp_num : int, optional
    pp_end_year : int, optional
    environment : str, optional
    """
    logger.info("=" * 60)
    logger.info("Starting Pay Calendar Job")
    logger.info("=" * 60)

    spark = get_spark_session("pay_calendar", environment)

    try:
        step_reset()
        step_set(pp_num, pp_end_year)
        pp_details = step_verify(spark)
        subject, message = step_build_message(pp_details)
        step_send_email(subject, message)
        logger.info("Pay Calendar Job completed successfully")
    except SystemExit:
        raise
    except Exception:
        logger.exception("Pay Calendar Job failed")
        from pyspark_migration.common.notification import send_failure_email
        send_failure_email("Pay Calendar", "Job failed - check logs")
        raise
    finally:
        spark.stop()


def main():
    """CLI entry point for Pay Calendar job."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="BIIS Pay Calendar Job")
    parser.add_argument("--pp-num", type=int, help="Pay period number")
    parser.add_argument("--pp-end-year", type=int, help="Pay period end year")
    parser.add_argument("--environment", type=str, help="Environment (dev/test/prod)")
    args = parser.parse_args()

    run(
        pp_num=args.pp_num,
        pp_end_year=args.pp_end_year,
        environment=args.environment,
    )


if __name__ == "__main__":
    main()
