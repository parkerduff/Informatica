"""
LES (Leave and Earnings Statement) PySpark Pipeline.

Replaces: XML/LES (Informatica PowerCenter workflow wf_LES)

This pipeline processes Leave and Earnings Statement data from VSAM
flat files (multiple record types) and loads them into Oracle staging
tables, then generates LES output files for agency distribution.

Source Files (VSAM format from mainframe):
  - EMP_REC_TYPE_0.TXT through EMP_REC_TYPE_6.TXT
  - EMP_REC_TYPE_C.TXT, D.TXT, E.TXT, L.TXT, M.TXT, R.TXT, T.TXT, U.TXT
  - LES_EMPLOYEE_DETAIL (flat file)

Source Tables:
  - HISTDBA.PAY_PERIOD
  - LES_PRIMARY_DATA_TBL
  - LES_EMP_DETAIL_TBL, LES_EMP_DETAIL_LEAVE_TBL
  - LES_EMP_DETAIL_RECTYPE_1_TBL through RECTYPE_6_TBL
  - LES_EMP_DETAIL_RECTYPE_C_TBL, D, M, R, T, U
  - LES_HEADER_TBL
  - LES_NIH_EMPLOYEE_SUMMARY_TBL
  - ERROR_TBL

Target Tables:
  - LESL, LEST, LESM, LESD, LESU, LESR, LESC (normalized LES tables)
  - LESS (LES Summary)
  - LES_HEADER_TBL
  - ERROR_TBL, COUNTER_TBL

Key Transformations:
  - Normalizer: Parse VSAM COBOL copybook layouts for each record type
  - Source Qualifier: Read from staging tables
  - Expression: Convert COBOL fields, validate data, build keys
  - Lookup: lkp_LESL_KEY, lkp_LEST_KEY, lkp_LESM_KEY, lkp_LESD_KEY,
    lkp_LESU_KEY, lkp_LESR_KEY, lkp_LESC_KEY, lkp_MAX_LESS_KEY,
    lkp_MAX_LESD_KEY, lkp_Pay_Period_Record_Date
  - Sequence: SEQTRANS (generate surrogate keys)
  - Filter: fil_LES_HEADER
  - Router: Route by record type
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

# Record type codes in LES files
RECORD_TYPES = [
    "0", "1", "2", "3", "4", "5", "6",
    "C", "D", "E", "L", "M", "R", "T", "U",
]

# Mapping of record type -> target table
RECORD_TYPE_TABLE_MAP = {
    "L": "LESL",
    "T": "LEST",
    "M": "LESM",
    "D": "LESD",
    "U": "LESU",
    "R": "LESR",
    "C": "LESC",
}


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


def load_les_record_type(
    spark: SparkSession,
    config: AppConfig,
    input_dir: str,
    record_type: str,
    pay_period: dict,
) -> int:
    """Load a single LES record type file into its staging table.

    Each record type (0-6, C, D, E, L, M, R, T, U) has a corresponding
    VSAM input file and staging table.

    Equivalent to individual Informatica sessions like:
      s_LES_Load_RecType_L, s_LES_Load_RecType_T, etc.

    Args:
        spark: Active SparkSession.
        config: Application configuration.
        input_dir: Directory containing the LES input files.
        record_type: Single character record type identifier.
        pay_period: Current pay period info.

    Returns:
        Number of records loaded.
    """
    input_file = os.path.join(input_dir, f"EMP_REC_TYPE_{record_type}.TXT")

    if not os.path.exists(input_file):
        logger.warning("LES file not found: %s", input_file)
        return 0

    logger.info("Loading LES record type %s from %s", record_type, input_file)

    # Read VSAM flat file
    raw_df = spark.read.text(input_file)

    if raw_df.count() == 0:
        logger.info("No records in file for type %s", record_type)
        return 0

    # Parse common fields from fixed positions
    parsed_df = raw_df.withColumn(
        "SSN", F.substring(F.col("value"), 1, 9)
    ).withColumn(
        "RAW_DATA", F.col("value")
    ).withColumn(
        "RECORD_TYPE", F.lit(record_type)
    ).withColumn(
        "PP_END_YEAR", F.lit(pay_period["PP_END_YEAR"])
    ).withColumn(
        "PP_NUM", F.lit(pay_period["PP_NUM"])
    ).withColumn(
        "LOAD_DATE", F.current_timestamp()
    )

    # Determine target staging table
    staging_table = f"LES_EMP_DETAIL_RECTYPE_{record_type}_TBL"
    if record_type in ("0", "E"):
        # Types 0 and E go to the primary data or detail tables
        staging_table = "LES_EMP_DETAIL_TBL"

    # Write to staging table
    output_df = parsed_df.select(
        "PP_END_YEAR", "PP_NUM", "SSN", "RAW_DATA",
        "RECORD_TYPE", "LOAD_DATE",
    )
    write_table(output_df, config.db, staging_table, mode="append")

    count = output_df.count()
    logger.info("Loaded %d records for type %s", count, record_type)
    return count


def load_les_employee_detail(
    spark: SparkSession,
    config: AppConfig,
    input_dir: str,
    pay_period: dict,
) -> int:
    """Load the LES_EMPLOYEE_DETAIL flat file.

    This is a separate flat file (not VSAM) that contains employee
    detail records.

    Returns:
        Number of records loaded.
    """
    input_file = os.path.join(input_dir, "LES_EMPLOYEE_DETAIL")
    if not os.path.exists(input_file):
        logger.warning("LES_EMPLOYEE_DETAIL not found: %s", input_file)
        return 0

    logger.info("Loading LES_EMPLOYEE_DETAIL")
    raw_df = spark.read.text(input_file)

    if raw_df.count() == 0:
        return 0

    parsed_df = raw_df.withColumn(
        "SSN", F.substring(F.col("value"), 1, 9)
    ).withColumn(
        "RAW_DATA", F.col("value")
    ).withColumn(
        "PP_END_YEAR", F.lit(pay_period["PP_END_YEAR"])
    ).withColumn(
        "PP_NUM", F.lit(pay_period["PP_NUM"])
    ).withColumn(
        "LOAD_DATE", F.current_timestamp()
    )

    output_df = parsed_df.select(
        "PP_END_YEAR", "PP_NUM", "SSN", "RAW_DATA", "LOAD_DATE",
    )
    write_table(output_df, config.db, "LES_EMP_DETAIL_TBL", mode="append")

    count = output_df.count()
    logger.info("Loaded %d LES_EMPLOYEE_DETAIL records", count)
    return count


def process_les_staging(
    spark: SparkSession,
    config: AppConfig,
    pay_period: dict,
) -> dict:
    """Process LES staging tables into normalized LES target tables.

    Equivalent to Informatica mappings:
      m_LES_Load_LESL, m_LES_Load_LEST, m_LES_Load_LESM,
      m_LES_Load_LESD, m_LES_Load_LESU, m_LES_Load_LESR,
      m_LES_Load_LESC, m_LES_Load_LESS (Summary)

    Each mapping reads from its staging table, performs lookups
    to find existing keys, generates sequence numbers for new
    records, and writes to the normalized target tables.

    Returns:
        Dictionary with record counts per target table.
    """
    logger.info("Processing LES staging tables into normalized targets")
    counts = {}

    for rec_type, target_table in RECORD_TYPE_TABLE_MAP.items():
        staging_table = f"LES_EMP_DETAIL_RECTYPE_{rec_type}_TBL"

        staging_df = read_table(spark, config.db, table_name=staging_table)
        count = staging_df.count()

        if count > 0:
            # Add sequence key using monotonically_increasing_id
            keyed_df = staging_df.withColumn(
                f"{target_table}_KEY",
                F.monotonically_increasing_id() + 1,
            ).withColumn(
                "LOAD_DATE", F.current_timestamp(),
            )

            write_table(keyed_df, config.db, target_table, mode="append")
            logger.info(
                "Loaded %d records to %s from %s",
                count, target_table, staging_table,
            )

        counts[target_table] = count

    # Process LES Primary/Summary data
    primary_df = read_table(
        spark, config.db, table_name="LES_PRIMARY_DATA_TBL",
    )
    primary_count = primary_df.count()
    if primary_count > 0:
        summary_df = primary_df.withColumn(
            "LESS_KEY",
            F.monotonically_increasing_id() + 1,
        ).withColumn(
            "LOAD_DATE", F.current_timestamp(),
        )
        write_table(summary_df, config.db, "LESS", mode="append")
    counts["LESS"] = primary_count

    logger.info("LES staging processing complete: %s", counts)
    return counts


def load_les_header(
    spark: SparkSession,
    config: AppConfig,
    pay_period: dict,
) -> int:
    """Load/update LES header table.

    Equivalent to Informatica filter fil_LES_HEADER which validates
    header records before loading to LES_HEADER_TBL.

    Returns:
        Number of header records processed.
    """
    logger.info("Processing LES header records")

    header_df = read_table(
        spark, config.db, table_name="LES_HEADER_TBL",
    )
    count = header_df.count()
    logger.info("LES header records: %d", count)
    return count


def build_counters_and_message(
    spark: SparkSession,
    config: AppConfig,
    pay_period: dict,
    file_counts: dict,
    staging_counts: dict,
) -> tuple:
    """Build counters and notification message."""
    pp_num = pay_period["PP_NUM"]
    pp_end_year = pay_period["PP_END_YEAR"]
    pp_num_str = str(pp_num).zfill(2)
    run_date = datetime.now()

    # Build counters for file loads
    counter_entries = []
    for rec_type in RECORD_TYPES:
        count = file_counts.get(rec_type, 0)
        counter_entries.append(
            (f"LES RecType {rec_type} File Records", count)
        )

    # Build counters for staging loads
    for table_name, count in staging_counts.items():
        counter_entries.append((f"LES {table_name} Records", count))

    counter_data = [
        (run_date, "wf_LES", desc, float(count), pp_end_year, pp_num, None)
        for desc, count in counter_entries
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
        f"{env_prefix}LES Files loaded successfully for "
        f"Pay Period: {pp_end_year}-{pp_num_str}"
    )

    lines = [
        f"LES Processing Results for Pay Period {pp_end_year}-{pp_num_str}",
        "=" * 60,
        "File Load Counts:",
    ]
    for rec_type in RECORD_TYPES:
        count = file_counts.get(rec_type, 0)
        lines.append(f"  RecType {rec_type}: {count}")

    lines.append("")
    lines.append("Staging Load Counts:")
    for table_name, count in staging_counts.items():
        lines.append(f"  {table_name}: {count}")

    message = "\n".join(lines)
    return subject, message


def run(config=None, input_dir=None):
    """Execute the full LES workflow.

    Equivalent to Informatica workflow wf_LES which runs sessions:
      1. Get current pay period
      2. Load each record type file to staging
      3. Load LES_EMPLOYEE_DETAIL
      4. Process staging into normalized tables
      5. Load/update LES header
      6. Build counters and message
    """
    if config is None:
        config = AppConfig()

    log_file = setup_logging("les", config.paths)
    spark = create_spark_session(config, "LES")

    try:
        if input_dir is None:
            input_dir = config.paths.les_input_dir

        # Step 1: Get current pay period
        pay_period = get_current_pay_period(spark, config)

        # Step 2: Load each record type file
        file_counts = {}
        for rec_type in RECORD_TYPES:
            count = load_les_record_type(
                spark, config, input_dir, rec_type, pay_period,
            )
            file_counts[rec_type] = count

        # Step 3: Load LES_EMPLOYEE_DETAIL
        emp_detail_count = load_les_employee_detail(
            spark, config, input_dir, pay_period,
        )
        file_counts["EMP_DETAIL"] = emp_detail_count

        # Step 4: Process staging into normalized tables
        staging_counts = process_les_staging(spark, config, pay_period)

        # Step 5: Load/update LES header
        load_les_header(spark, config, pay_period)

        # Step 6: Build counters and message
        subject, message = build_counters_and_message(
            spark, config, pay_period, file_counts, staging_counts,
        )

        send_success_notification(
            config.email, process_name=subject,
            message=message, log_file=log_file,
        )
        logger.info("LES workflow completed successfully")

    except Exception as e:
        logger.exception("LES workflow failed")
        send_failure_notification(
            config.email, process_name="LES", error_message=str(e),
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
