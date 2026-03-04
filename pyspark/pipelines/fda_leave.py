"""
FDA Leave Validation PySpark Pipeline.

Replaces: XML/FDA_Leave (Informatica PowerCenter workflow wf_FDA_Leave)

This pipeline processes FDA leave and earnings data, validates records,
and generates output files for FDA-specific leave tracking.

Source Files:
  - HI_PM_FDA_TATRAN_FLAT (flat file with FDA transaction data)
  - HI_PM_FDA_TATRAN_FLAT_FILE_NAME (file containing input filename)

Source Tables:
  - HISTDBA.PAY_PERIOD
  - HI_PM_FDA_TATRAN_TBL (existing FDA transaction records)
  - HI_GENERIC_SRC_TBL (generic source configuration)
  - CPM_CYCLE_TBL (cycle tracking)
  - ERROR_TBL

Target Tables:
  - HI_PM_FDA_TATRAN_TBL (updated FDA transaction records)
  - CPM_CYCLE_TBL (updated cycle info)
  - ERROR_TBL, COUNTER_TBL

Target Files:
  - HI_PM_FDA_TATRAN_FLAT (output flat file)
  - CPM_FDA_CPM_PAY_PERIOD_FILE
  - CPM_FDA_PAY_PERIOD_FILE
  - FDA_EXTRACT_MESSAGE_FILE
  - GENERIC_TARGET_FILE
"""

import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

from pyspark.utils.config import AppConfig
from pyspark.utils.spark_session import create_spark_session
from pyspark.utils.db_utils import read_table, write_table
from pyspark.utils.logging_utils import setup_logging
from pyspark.utils.notifications import (
    send_success_notification,
    send_failure_notification,
)

logger = logging.getLogger(__name__)

# FDA agency code
FDA_AGENCY_CODE = "HHS-FDA"


def get_current_pay_period(spark: SparkSession, config: AppConfig) -> dict:
    """Look up the current pay period."""
    pay_period_df = read_table(
        spark, config.db, table_name="HISTDBA.PAY_PERIOD",
        query=(
            "SELECT PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE "
            "FROM HISTDBA.PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'"
        ),
    )
    if pay_period_df.count() == 0:
        raise ValueError("No current pay period found")
    row = pay_period_df.first()
    return {
        "PP_NUM": int(row["PP_NUM"]),
        "PP_END_YEAR": int(row["PP_END_YEAR"]),
        "PP_START_DTE": row["PP_START_DTE"],
        "PP_END_DTE": row["PP_END_DTE"],
    }


def get_cycle_info(
    spark: SparkSession, config: AppConfig,
) -> dict:
    """Get the current CPM cycle information.

    Reads from CPM_CYCLE_TBL to determine the current processing
    cycle for FDA data.

    Returns:
        Dictionary with cycle ID and status.
    """
    logger.info("Getting CPM cycle information")

    cycle_df = read_table(spark, config.db, table_name="CPM_CYCLE_TBL")

    if cycle_df.count() == 0:
        logger.warning("No CPM cycle records found")
        return {"CYCLE_ID": 0, "CYCLE_STATUS": "NEW"}

    row = cycle_df.first()
    result = {
        "CYCLE_ID": row["CYCLE_ID"] if "CYCLE_ID" in cycle_df.columns else 0,
        "CYCLE_STATUS": row["CYCLE_STATUS"] if "CYCLE_STATUS" in cycle_df.columns else "UNKNOWN",
    }
    logger.info("CPM cycle: %s", result)
    return result


def load_fda_flat_file(
    spark: SparkSession,
    config: AppConfig,
    input_file: str,
    pay_period: dict,
) -> int:
    """Load FDA transaction data from flat file.

    Equivalent to Informatica mapping m_FDA_Leave_Load which reads
    HI_PM_FDA_TATRAN_FLAT, validates records, and loads to
    HI_PM_FDA_TATRAN_TBL.

    Args:
        spark: Active SparkSession.
        config: Application configuration.
        input_file: Path to the FDA flat file.
        pay_period: Current pay period info.

    Returns:
        Number of records loaded.
    """
    logger.info("Loading FDA flat file: %s", input_file)

    if not os.path.exists(input_file):
        logger.warning("FDA flat file not found: %s", input_file)
        return 0

    # Read the flat file
    raw_df = spark.read.text(input_file)

    if raw_df.count() == 0:
        logger.info("FDA flat file is empty")
        return 0

    # Parse FDA transaction fields from fixed-width positions
    parsed_df = raw_df.withColumn(
        "SSN", F.substring(F.col("value"), 1, 9)
    ).withColumn(
        "EMPLOYEE_NAME", F.trim(F.substring(F.col("value"), 10, 30))
    ).withColumn(
        "TRANSACTION_CODE", F.trim(F.substring(F.col("value"), 40, 4))
    ).withColumn(
        "RAW_DATA", F.col("value")
    )

    # Validate SSN format
    valid_df = parsed_df.filter(F.col("SSN").rlike("^[0-9]{9}$"))
    invalid_count = parsed_df.count() - valid_df.count()

    if invalid_count > 0:
        logger.warning("%d invalid FDA records filtered out", invalid_count)
        invalid_df = parsed_df.filter(
            ~F.col("SSN").rlike("^[0-9]{9}$")
        ).select(
            F.lit("FDA Leave Load").alias("PROCESS_NAME"),
            F.lit("Invalid SSN format").alias("ERROR_DESC"),
            F.col("SSN"),
            F.current_timestamp().alias("ERROR_DATE"),
        )
        write_table(invalid_df, config.db, "ERROR_TBL", mode="append")

    # Add pay period and load metadata
    result_df = valid_df.withColumn(
        "PP_END_YEAR", F.lit(pay_period["PP_END_YEAR"])
    ).withColumn(
        "PP_NUM", F.lit(pay_period["PP_NUM"])
    ).withColumn(
        "LOAD_DATE", F.current_timestamp()
    )

    write_table(result_df, config.db, "HI_PM_FDA_TATRAN_TBL", mode="append")

    count = result_df.count()
    logger.info("Loaded %d FDA transaction records", count)
    return count


def extract_fda_leave_data(
    spark: SparkSession,
    config: AppConfig,
    pay_period: dict,
) -> int:
    """Extract FDA leave data from the transaction table.

    Equivalent to Informatica mapping m_FDA_Leave_Extract which reads
    HI_PM_FDA_TATRAN_TBL, joins with HI_GENERIC_SRC_TBL for formatting,
    and generates the FDA output flat file.

    Returns:
        Number of records extracted.
    """
    logger.info("Extracting FDA leave data")

    fda_df = read_table(spark, config.db, table_name="HI_PM_FDA_TATRAN_TBL")

    if fda_df.count() == 0:
        logger.info("No FDA transaction records to extract")
        return 0

    # Join with generic source for formatting rules
    generic_src_df = read_table(
        spark, config.db, table_name="HI_GENERIC_SRC_TBL",
    )

    # Apply formatting if generic source has matching records
    if "HI_GENERIC_SRC_KEY" in fda_df.columns:
        joined_df = fda_df.join(
            generic_src_df, on="HI_GENERIC_SRC_KEY", how="left",
        )
    else:
        joined_df = fda_df

    # Generate output flat file
    output_dir = config.paths.cpm_output_dir
    os.makedirs(output_dir, exist_ok=True)

    pp_num_str = str(pay_period["PP_NUM"]).zfill(2)
    pp_end_year = pay_period["PP_END_YEAR"]

    output_file = os.path.join(
        output_dir,
        f"FDA_TATRAN_{pp_end_year}_{pp_num_str}.txt",
    )
    joined_df.toPandas().to_csv(
        output_file, sep="|", index=False, header=True,
    )

    count = joined_df.count()
    logger.info("Extracted %d FDA records to %s", count, output_file)
    return count


def write_pay_period_files(config: AppConfig, pay_period: dict) -> None:
    """Write FDA-specific pay period files.

    Generates:
      - CPM_FDA_CPM_PAY_PERIOD_FILE
      - CPM_FDA_PAY_PERIOD_FILE
    """
    output_dir = config.paths.cpm_output_dir
    os.makedirs(output_dir, exist_ok=True)

    pp_num_str = str(pay_period["PP_NUM"]).zfill(2)
    pp_end_year = pay_period["PP_END_YEAR"]

    # CPM FDA pay period file
    cpm_pp_file = os.path.join(output_dir, "CPM_FDA_CPM_PAY_PERIOD_FILE.txt")
    with open(cpm_pp_file, "w") as f:
        f.write(f"{pp_end_year}|{pp_num_str}\n")

    # FDA pay period file
    fda_pp_file = os.path.join(output_dir, "CPM_FDA_PAY_PERIOD_FILE.txt")
    with open(fda_pp_file, "w") as f:
        f.write(
            f"{pp_end_year}|{pp_num_str}|"
            f"{pay_period['PP_START_DTE']}|"
            f"{pay_period['PP_END_DTE']}\n"
        )

    logger.info("FDA pay period files written to %s", output_dir)


def update_cycle(
    spark: SparkSession, config: AppConfig, cycle_info: dict,
) -> None:
    """Update the CPM cycle table after FDA processing.

    Marks the current cycle as completed for FDA processing.
    """
    logger.info("Updating CPM cycle table")

    cycle_data = [(
        cycle_info.get("CYCLE_ID", 0),
        "COMPLETED",
        datetime.now(),
    )]
    cycle_df = spark.createDataFrame(
        cycle_data,
        schema=["CYCLE_ID", "CYCLE_STATUS", "COMPLETION_DATE"],
    )
    write_table(cycle_df, config.db, "CPM_CYCLE_TBL", mode="overwrite")


def build_counters_and_message(
    spark: SparkSession,
    config: AppConfig,
    pay_period: dict,
    loaded_count: int,
    extracted_count: int,
) -> tuple:
    """Build counters and notification message."""
    pp_num = pay_period["PP_NUM"]
    pp_end_year = pay_period["PP_END_YEAR"]
    pp_num_str = str(pp_num).zfill(2)
    run_date = datetime.now()

    counter_data = [
        (run_date, "wf_FDA_Leave", desc, float(count), pp_end_year, pp_num, None)
        for desc, count in [
            ("FDA Records Loaded", loaded_count),
            ("FDA Records Extracted", extracted_count),
        ]
    ]
    counter_df = spark.createDataFrame(
        counter_data,
        schema=[
            "RUN_DATE", "PROCESS_NAME", "COUNTER_DESCRIPTION",
            "COUNTER_VALUE", "PP_END_YEAR", "PP_NUM", "CYCLE_ID",
        ],
    )
    write_table(counter_df, config.db, "COUNTER_TBL", mode="append")

    env_prefix = f"{config.environment}: "
    subject = (
        f"{env_prefix}FDA Leave processed for "
        f"Pay Period: {pp_end_year}-{pp_num_str}"
    )

    message = (
        f"FDA Leave Processing Results for Pay Period "
        f"{pp_end_year}-{pp_num_str}\n"
        f"{'=' * 60}\n"
        f"Records Loaded:    {loaded_count}\n"
        f"Records Extracted: {extracted_count}\n"
    )

    # Write message file
    msg_file = os.path.join(
        config.paths.cpm_output_dir, "FDA_EXTRACT_MESSAGE_FILE.txt",
    )
    with open(msg_file, "w") as f:
        f.write(message)

    return subject, message


def run(config=None, input_file=None):
    """Execute the full FDA Leave workflow.

    Equivalent to Informatica workflow wf_FDA_Leave.
    """
    if config is None:
        config = AppConfig()

    log_file = setup_logging("fda_leave", config.paths)
    spark = create_spark_session(config, "FDA_Leave")

    try:
        if input_file is None:
            input_file = os.path.join(
                config.paths.cpm_input_dir, "HI_PM_FDA_TATRAN_FLAT",
            )

        # Step 1: Get current pay period
        pay_period = get_current_pay_period(spark, config)

        # Step 2: Get cycle info
        cycle_info = get_cycle_info(spark, config)

        # Step 3: Load FDA flat file
        loaded_count = load_fda_flat_file(
            spark, config, input_file, pay_period,
        )

        # Step 4: Extract FDA leave data
        extracted_count = extract_fda_leave_data(spark, config, pay_period)

        # Step 5: Write pay period files
        write_pay_period_files(config, pay_period)

        # Step 6: Update cycle
        update_cycle(spark, config, cycle_info)

        # Step 7: Build counters and message
        subject, message = build_counters_and_message(
            spark, config, pay_period, loaded_count, extracted_count,
        )

        send_success_notification(
            config.email, process_name=subject,
            message=message, log_file=log_file,
        )
        logger.info("FDA Leave workflow completed successfully")

    except Exception as e:
        logger.exception("FDA Leave workflow failed")
        send_failure_notification(
            config.email, process_name="FDA Leave", error_message=str(e),
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
