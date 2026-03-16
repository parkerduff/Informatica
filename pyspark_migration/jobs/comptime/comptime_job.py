"""
COMPTIME Job

Implements two mappings:
1. m_COMPTIME_Load_COMP_TIME_DAILY_TBL - Load comp time data
2. m_COMPTIME_Build_Message_Counters   - Build counters and notification

Reference: XML/COMPTIME (lines 6-27 for source, lines 84-97 for message builder)
"""

import argparse
import logging

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from pyspark_migration.common.db_utils import write_oracle_table
from pyspark_migration.common.error_logging import log_counter
from pyspark_migration.common.file_utils import parse_csv
from pyspark_migration.common.notification import send_email
from pyspark_migration.common.pay_period import get_current_pay_period
from pyspark_migration.common.spark_session import get_spark_session
from pyspark_migration.common.validation import classify_record_type, is_numeric
from pyspark_migration.config.settings import FILE_PATHS, get_environment

logger = logging.getLogger(__name__)

# CSV schema for U0287D01 file with 12 fields (XML/COMPTIME lines 6-27)
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


def mapping_load_comp_time_daily(spark, input_file, pp_num, pp_end_year):
    """
    Mapping 1: m_COMPTIME_Load_COMP_TIME_DAILY_TBL

    1. Read CSV file U0287D01 with 12 fields
    2. Classify record types using SSN field (H/T/D pattern)
    3. Filter: RECORD_TYPE_FLAG='D' AND IS_NUMBER(SSN)
    4. Convert PP_END_DATE and DAILY_DATE_EARNED from YYYYMMDD
    5. Lookup PAY_PERIOD for PP_NUM, PP_END_YEAR
    6. Calculate PP_YEAR_NUM = str(PP_END_YEAR) + str(PP_NUM).zfill(2)
    7. Write to COMP_TIME_DAILY_TBL

    Parameters
    ----------
    spark : SparkSession
    input_file : str
    pp_num : int
    pp_end_year : int

    Returns
    -------
    int
        Number of detail records loaded.
    """
    logger.info("Mapping 1: Loading COMP_TIME_DAILY_TBL from %s", input_file)

    # Step 1: Read CSV
    raw_df = parse_csv(spark, input_file, schema=COMPTIME_SCHEMA)
    total_count = raw_df.count()
    logger.info("Read %d records from COMPTIME CSV", total_count)

    # Step 2: Classify record types (same H/T/D pattern as PseudoSSN)
    classified_df = raw_df.withColumn(
        "RECORD_TYPE_FLAG",
        classify_record_type(F.col("SSN")),
    )

    # Step 3: Filter detail records with numeric SSN
    # Replicates COMPTIME exp_Initial (line 109): IS_NUMBER(SSN) -> 'D'
    detail_df = classified_df.filter(
        (F.col("RECORD_TYPE_FLAG") == "D") & is_numeric(F.col("SSN"))
    )
    detail_count = detail_df.count()
    logger.info("Detail records after filter: %d", detail_count)

    # Step 4: Convert dates from YYYYMMDD strings
    converted_df = (
        detail_df
        .withColumn(
            "PP_END_DATE_CONV",
            F.to_date(F.col("PP_END_DATE"), "yyyyMMdd"),
        )
        .withColumn(
            "DAILY_DATE_EARNED_CONV",
            F.to_date(F.col("DAILY_DATE_EARNED"), "yyyyMMdd"),
        )
    )

    # Step 5 & 6: Add pay period info and calculate PP_YEAR_NUM
    pp_num_padded = str(pp_num).zfill(2)
    pp_year_num = int(f"{pp_end_year}{pp_num_padded}")

    enriched_df = (
        converted_df
        .withColumn("PP_END_YEAR", F.lit(pp_end_year).cast(DecimalType(4, 0)))
        .withColumn("PP_NUM", F.lit(pp_num).cast(DecimalType(2, 0)))
        .withColumn("PP_YEAR_NUM", F.lit(pp_year_num).cast(DecimalType(6, 0)))
    )

    # Step 7: Select target columns and write
    target_df = enriched_df.select(
        "PP_END_YEAR", "PP_NUM", "PP_YEAR_NUM",
        "SSN", "NAME", "CURRENT_ACCT", "CURRENT_ORG", "FLSA_STATUS",
        "COMP_TIME_CUR_BAL", "COMP_TIME_YEAR_EARNED",
        F.col("PP_END_DATE_CONV").alias("PP_END_DATE"),
        F.col("DAILY_DATE_EARNED_CONV").alias("DAILY_DATE_EARNED"),
        "COMP_TIME_RATE", "COMP_TIME_HOURS", "COMP_TIME_UNDEF",
    )

    write_oracle_table(target_df, "COMP_TIME_DAILY_TBL", mode="append")
    logger.info("Wrote %d records to COMP_TIME_DAILY_TBL", detail_count)

    return detail_count


def mapping_build_message_counters(spark, detail_count, pp_num, pp_end_year):
    """
    Mapping 2: m_COMPTIME_Build_Message_Counters

    1. Count detail records from mapping 1
    2. Build subject with environment prefix (XML/COMPTIME lines 91-93)
    3. Build message body
    4. Write counter to COUNTER_TBL
    5. Send email notification

    Parameters
    ----------
    spark : SparkSession
    detail_count : int
    pp_num : int
    pp_end_year : int
    """
    logger.info("Mapping 2: Building message and counters")

    # Step 2: Build subject line with environment prefix
    # Replicates DECODE(SUBSTR($PMRepositoryServiceName, 1, 4), ...)
    env = get_environment()
    env_prefix = {"dev": "Dev: ", "test": "Test: ", "prod": "Prod: "}.get(env, "")
    pp_num_padded = str(pp_num).zfill(2)
    subject = (
        f"{env_prefix}Comp Time File loaded successfully "
        f"for Pay Period: {pp_end_year}-{pp_num_padded}"
    )

    # Step 3: Build message (XML/COMPTIME line 94)
    message = (
        f"Number of Detail Records from Comp Time file\t= {detail_count}"
    )

    # Step 4: Write counter to COUNTER_TBL
    log_counter(
        spark,
        "m_COMPTIME_Build_Message_Counters",
        "Number of detail records from the COMP TIME file.",
        detail_count,
        pp_end_year,
        pp_num,
    )

    # Step 5: Send email
    send_email(subject, message, include_env_prefix=False)
    logger.info("Counter and notification sent for COMPTIME")


def run(input_file=None, environment=None):
    """
    Execute the full COMPTIME workflow.

    Parameters
    ----------
    input_file : str, optional
    environment : str, optional
    """
    logger.info("=" * 60)
    logger.info("Starting COMPTIME Job")
    logger.info("=" * 60)

    if input_file is None:
        input_file = FILE_PATHS["comptime_input"]

    spark = get_spark_session("comptime", environment)

    try:
        # Get current pay period
        pp_df = get_current_pay_period(spark)
        pp_row = pp_df.collect()[0]
        pp_num = int(pp_row["PP_NUM"])
        pp_end_year = int(pp_row["PP_END_YEAR"])

        # Run mapping 1
        detail_count = mapping_load_comp_time_daily(
            spark, input_file, pp_num, pp_end_year
        )

        # Run mapping 2
        mapping_build_message_counters(spark, detail_count, pp_num, pp_end_year)

        logger.info("COMPTIME Job completed successfully")

    except Exception:
        logger.exception("COMPTIME Job failed")
        from pyspark_migration.common.notification import send_failure_email
        send_failure_email("COMPTIME", "Job failed - check logs")
        raise
    finally:
        spark.stop()


def main():
    """CLI entry point for COMPTIME job."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="BIIS COMPTIME Job")
    parser.add_argument("--input-file", type=str, help="Path to U0287D01 CSV file")
    parser.add_argument("--environment", type=str, help="Environment (dev/test/prod)")
    args = parser.parse_args()

    run(input_file=args.input_file, environment=args.environment)


if __name__ == "__main__":
    main()
