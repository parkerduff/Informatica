"""
Compensatory Time PySpark Pipeline.

Replaces: XML/COMPTIME (Informatica PowerCenter workflow wf_COMPTIME)

Mappings converted:
  1. m_COMPTIME_Get_Current_PP         - Determine current pay period
  2. m_COMPTIME_Load_COMP_TIME_DAILY_TBL - Load daily comp time records
  3. m_COMPTIME_Build_Message_Counters - Count records and build notification

Source: U0287D01 flat file (CSV), PAY_PERIOD table
Target: COMP_TIME_DAILY_TBL, COUNTER_TBL
"""

import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
)

from pyspark.utils.config import AppConfig
from pyspark.utils.spark_session import create_spark_session
from pyspark.utils.db_utils import read_table, write_table
from pyspark.utils.logging_utils import setup_logging
from pyspark.utils.notifications import (
    send_success_notification,
    send_failure_notification,
)

logger = logging.getLogger(__name__)

# Schema for the U0287D01 flat file input
COMPTIME_SCHEMA = StructType([
    StructField("SSN", StringType(), True),
    StructField("NAME", StringType(), True),
    StructField("CURRENT_ACCT", StringType(), True),
    StructField("CURRENT_ORG", StringType(), True),
    StructField("FLSA_STATUS", StringType(), True),
    StructField("COMP_TIME_CUR_BAL", DecimalType(8, 2), True),
    StructField("COMP_TIME_YEAR_EARNED", DecimalType(4, 0), True),
    StructField("PP_END_DATE", StringType(), True),
    StructField("DAILY_DATE_EARNED", StringType(), True),
    StructField("COMP_TIME_RATE", DecimalType(6, 2), True),
    StructField("COMP_TIME_HOURS", DecimalType(8, 2), True),
    StructField("COMP_TIME_UNDEF", DecimalType(6, 0), True),
])


def get_current_pay_period(spark: SparkSession, config: AppConfig) -> dict:
    """Look up the current pay period from the PAY_PERIOD table.

    Equivalent to Informatica mapping m_COMPTIME_Get_Current_PP which
    performs a lookup on PAY_PERIOD where CURR_PP_FLAG = 'Y'.

    Returns:
        Dictionary with PP_NUM and PP_END_YEAR.
    """
    logger.info("Looking up current pay period")

    pay_period_df = read_table(
        spark,
        config.db,
        table_name="HISTDBA.PAY_PERIOD",
        query=(
            "SELECT PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE "
            "FROM HISTDBA.PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'"
        ),
    )

    if pay_period_df.count() == 0:
        raise ValueError("No current pay period found (CURR_PP_FLAG = 'Y')")

    row = pay_period_df.first()
    result = {
        "PP_NUM": int(row["PP_NUM"]),
        "PP_END_YEAR": int(row["PP_END_YEAR"]),
    }
    logger.info(
        "Current pay period: PP_NUM=%s, PP_END_YEAR=%s",
        result["PP_NUM"],
        result["PP_END_YEAR"],
    )
    return result


def load_comp_time_daily(
    spark: SparkSession,
    config: AppConfig,
    input_file: str,
    pay_period: dict,
) -> int:
    """Load compensatory time daily records from flat file to database.

    Equivalent to Informatica mapping m_COMPTIME_Load_COMP_TIME_DAILY_TBL.

    Processing flow:
      1. Read U0287D01 CSV file (SQ_U0287D01)
      2. Determine record type - filter valid records where SSN is numeric
         (exp_Initial -> fil_Valid_Records)
      3. Look up current pay period (lkp_PAY_PERIOD)
      4. Convert date strings YYYYMMDD -> date objects (exp_Convert)
      5. Compute PP_YEAR_NUM = PP_END_YEAR || LPAD(PP_NUM, 2, '0')
         (exp_Final)
      6. Write to COMP_TIME_DAILY_TBL

    Args:
        spark: Active SparkSession.
        config: Application configuration.
        input_file: Path to the U0287D01 input file.
        pay_period: Dict with PP_NUM and PP_END_YEAR from current period.

    Returns:
        Number of detail records loaded.
    """
    logger.info("Loading comp time daily records from: %s", input_file)

    # Step 1: Read the flat file (Source Qualifier SQ_U0287D01)
    raw_df = spark.read.csv(
        input_file,
        schema=COMPTIME_SCHEMA,
        header=False,
        quote='"',
    )

    # Step 2: exp_Initial - Determine record type
    # Valid records have numeric SSN (IS_NUMBER(SSN) -> 'D')
    df = raw_df.withColumn(
        "RECORD_TYPE_FLAG",
        F.when(F.col("SSN").rlike("^[0-9]+$"), F.lit("D")).otherwise(F.lit("NO")),
    ).withColumn(
        "VALID_RECORD_FLAG",
        F.when(F.col("SSN").rlike("^[0-9]+$"), F.lit(1)).otherwise(F.lit(0)),
    )

    # fil_Valid_Records - Filter to valid records only
    valid_df = df.filter(F.col("VALID_RECORD_FLAG") == 1)

    record_count = valid_df.count()
    logger.info("Valid detail records: %d", record_count)

    if record_count == 0:
        logger.warning("No valid records found in input file")
        return 0

    # Step 3 & 4: exp_Convert - Add pay period info and convert dates
    pp_num = pay_period["PP_NUM"]
    pp_end_year = pay_period["PP_END_YEAR"]
    pp_year_num = int(f"{pp_end_year}{str(pp_num).zfill(2)}")

    result_df = valid_df.select(
        F.lit(pp_end_year).cast(DecimalType(4, 0)).alias("PP_END_YEAR"),
        F.lit(pp_num).cast(DecimalType(2, 0)).alias("PP_NUM"),
        F.lit(pp_year_num).cast(DecimalType(6, 0)).alias("PP_YEAR_NUM"),
        F.col("SSN"),
        F.col("NAME"),
        F.col("CURRENT_ACCT"),
        F.col("CURRENT_ORG"),
        F.col("FLSA_STATUS"),
        F.col("COMP_TIME_CUR_BAL"),
        F.col("COMP_TIME_YEAR_EARNED"),
        # Convert date strings YYYYMMDD to date
        F.when(
            F.to_date(F.col("PP_END_DATE"), "yyyyMMdd").isNotNull(),
            F.to_date(F.col("PP_END_DATE"), "yyyyMMdd"),
        ).alias("PP_END_DATE"),
        F.when(
            F.to_date(F.col("DAILY_DATE_EARNED"), "yyyyMMdd").isNotNull(),
            F.to_date(F.col("DAILY_DATE_EARNED"), "yyyyMMdd"),
        ).alias("DAILY_DATE_EARNED"),
        F.col("COMP_TIME_RATE"),
        F.col("COMP_TIME_HOURS"),
        F.col("COMP_TIME_UNDEF"),
    )

    # Step 5: Write to COMP_TIME_DAILY_TBL
    write_table(result_df, config.db, "HISTDBA.COMP_TIME_DAILY_TBL", mode="append")
    logger.info("Loaded %d records to COMP_TIME_DAILY_TBL", record_count)

    return record_count


def build_message_and_counters(
    spark: SparkSession,
    config: AppConfig,
    record_count: int,
    pay_period: dict,
) -> tuple:
    """Build notification message and write counters.

    Equivalent to Informatica mapping m_COMPTIME_Build_Message_Counters.

    Processing flow:
      1. Compute counter descriptions (exp_Counters)
      2. Write to COUNTER_TBL (exp_Final -> COUNTER_TBL)
      3. Build email subject and message (exp_Build_Message)

    Args:
        spark: Active SparkSession.
        config: Application configuration.
        record_count: Number of detail records processed.
        pay_period: Current pay period info.

    Returns:
        Tuple of (subject, message).
    """
    logger.info("Building message and counters")

    pp_num = pay_period["PP_NUM"]
    pp_end_year = pay_period["PP_END_YEAR"]
    pp_num_str = str(pp_num).zfill(2)
    run_date = datetime.now()

    # Write to COUNTER_TBL
    counter_data = [(
        run_date,
        "m_COMPTIME_Build_Message_Counters",
        "Number of detail records from the COMP TIME file.",
        float(record_count),
        pp_end_year,
        pp_num,
        None,
    )]
    counter_df = spark.createDataFrame(
        counter_data,
        schema=[
            "RUN_DATE", "PROCESS_NAME", "COUNTER_DESCRIPTION",
            "COUNTER_VALUE", "PP_END_YEAR", "PP_NUM", "CYCLE_ID",
        ],
    )
    write_table(counter_df, config.db, "HISTDBA.COUNTER_TBL", mode="append")

    # Build email subject and message
    env_prefix = f"{config.environment}: "
    subject = (
        f"{env_prefix}Comp Time File loaded successfully for "
        f"Pay Period: {pp_end_year}-{pp_num_str}"
    )
    message = f"Number of Detail Records from Comp Time file\t= {record_count}"

    logger.info("Subject: %s", subject)
    logger.info("Message: %s", message)
    return subject, message


def run(config=None, input_file=None):
    """Execute the full COMPTIME workflow.

    Equivalent to Informatica workflow wf_COMPTIME.
    """
    if config is None:
        config = AppConfig()

    log_file = setup_logging("comptime", config.paths)
    spark = create_spark_session(config, "COMPTIME")

    try:
        # Determine input file
        if input_file is None:
            input_file = os.path.join(config.paths.comptime_input_dir, "U0287D01")

        # Step 1: Get current pay period
        pay_period = get_current_pay_period(spark, config)

        # Step 2: Load comp time daily records
        record_count = load_comp_time_daily(
            spark, config, input_file, pay_period
        )

        # Step 3: Build message and counters
        subject, message = build_message_and_counters(
            spark, config, record_count, pay_period
        )

        send_success_notification(
            config.email,
            process_name=subject,
            message=message,
            log_file=log_file,
        )
        logger.info("COMPTIME workflow completed successfully")

    except Exception as e:
        logger.exception("COMPTIME workflow failed")
        send_failure_notification(
            config.email,
            process_name="COMPTIME",
            error_message=str(e),
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
