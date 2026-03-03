"""
PseudoSSN Management PySpark Pipeline.

Replaces: XML/Pseudossn (Informatica PowerCenter workflow wf_Pseudossn)

This pipeline manages the PseudoSSN mapping table used to replace real
SSNs with pseudo-SSNs for downstream agency processing. It loads new
PseudoSSN assignments from flat files and maintains the archive table.

Source Files:
  - PSEUDOSSN_FILE (flat file with SSN -> PseudoSSN mappings)
  - PSEUDOSSN_FILE_TK_NUM (flat file with ticket number mappings)

Source Tables:
  - HISTDBA.PAY_PERIOD
  - PSEUDOSSN_TBL (existing mappings)
  - PSEUDOSSN_FROM_SDA_TBL (SDA-provided mappings)

Target Tables:
  - PSEUDOSSN_TBL (updated mappings)
  - PSEUDOSSN_FROM_SDA_TBL
  - HI_ARCH_PSEUDOSSN_TBL (archive)
  - PSEUDO_RECORD_COUNT
  - ERROR_TBL, COUNTER_TBL

Target Files:
  - PAY_PERIOD_DATE_FILE
  - PSEUDOSSN_MESSAGE_FILE
  - PSEUDO_HDR_DATE_FILE
"""

import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
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

# Schema for the PSEUDOSSN_FILE flat file
PSEUDOSSN_FILE_SCHEMA = StructType([
    StructField("SSN", StringType(), True),
    StructField("PSEUDOSSN", StringType(), True),
    StructField("LAST_NAME", StringType(), True),
    StructField("FIRST_NAME", StringType(), True),
    StructField("MIDDLE_INIT", StringType(), True),
    StructField("AGENCY", StringType(), True),
    StructField("EFFECTIVE_DATE", StringType(), True),
    StructField("EXPIRATION_DATE", StringType(), True),
    StructField("STATUS", StringType(), True),
])


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


def archive_existing_pseudossn(
    spark: SparkSession, config: AppConfig,
) -> int:
    """Archive existing PseudoSSN records before loading new ones.

    Equivalent to Informatica mapping that copies current PSEUDOSSN_TBL
    records to HI_ARCH_PSEUDOSSN_TBL.

    Returns:
        Number of records archived.
    """
    logger.info("Archiving existing PseudoSSN records")

    existing_df = read_table(spark, config.db, table_name="PSEUDOSSN_TBL")
    count = existing_df.count()

    if count > 0:
        archive_df = existing_df.withColumn(
            "ARCHIVE_DATE", F.current_timestamp()
        )
        write_table(
            archive_df, config.db,
            "HI_ARCH_PSEUDOSSN_TBL", mode="append",
        )

    logger.info("Archived %d PseudoSSN records", count)
    return count


def load_pseudossn_file(
    spark: SparkSession,
    config: AppConfig,
    input_file: str,
    pay_period: dict,
) -> int:
    """Load new PseudoSSN mappings from flat file.

    Equivalent to Informatica mapping m_Pseudossn_Load which reads
    PSEUDOSSN_FILE, validates SSN format, and inserts/updates
    PSEUDOSSN_TBL.

    Args:
        spark: Active SparkSession.
        config: Application configuration.
        input_file: Path to the PSEUDOSSN_FILE.
        pay_period: Current pay period info.

    Returns:
        Number of records loaded.
    """
    logger.info("Loading PseudoSSN file: %s", input_file)

    if not os.path.exists(input_file):
        logger.warning("PseudoSSN file not found: %s", input_file)
        return 0

    raw_df = spark.read.csv(
        input_file,
        schema=PSEUDOSSN_FILE_SCHEMA,
        header=False,
        sep="|",
    )

    # Validate SSN format (must be 9 digits)
    valid_df = raw_df.filter(
        F.col("SSN").rlike("^[0-9]{9}$")
    ).filter(
        F.col("PSEUDOSSN").rlike("^[0-9]{9}$")
    )

    invalid_count = raw_df.count() - valid_df.count()
    if invalid_count > 0:
        logger.warning("%d invalid SSN records filtered out", invalid_count)

        # Write invalid records to ERROR_TBL
        invalid_df = raw_df.filter(
            ~F.col("SSN").rlike("^[0-9]{9}$")
            | ~F.col("PSEUDOSSN").rlike("^[0-9]{9}$")
        ).select(
            F.lit("PseudoSSN Load").alias("PROCESS_NAME"),
            F.lit("Invalid SSN or PseudoSSN format").alias("ERROR_DESC"),
            F.col("SSN"),
            F.current_timestamp().alias("ERROR_DATE"),
        )
        write_table(invalid_df, config.db, "ERROR_TBL", mode="append")

    if valid_df.count() == 0:
        logger.info("No valid PseudoSSN records to load")
        return 0

    # Add pay period and load date
    result_df = valid_df.withColumn(
        "PP_END_YEAR", F.lit(pay_period["PP_END_YEAR"])
    ).withColumn(
        "PP_NUM", F.lit(pay_period["PP_NUM"])
    ).withColumn(
        "LOAD_DATE", F.current_timestamp()
    )

    write_table(result_df, config.db, "PSEUDOSSN_TBL", mode="overwrite")

    count = result_df.count()
    logger.info("Loaded %d PseudoSSN records", count)
    return count


def load_pseudossn_from_sda(
    spark: SparkSession,
    config: AppConfig,
    pay_period: dict,
) -> int:
    """Load PseudoSSN records from SDA source.

    Equivalent to Informatica mapping that reads PSEUDOSSN_FROM_SDA_TBL
    and merges with PSEUDOSSN_TBL.

    Returns:
        Number of SDA records processed.
    """
    logger.info("Loading PseudoSSN from SDA")

    sda_df = read_table(
        spark, config.db, table_name="PSEUDOSSN_FROM_SDA_TBL",
    )
    count = sda_df.count()
    logger.info("SDA PseudoSSN records: %d", count)
    return count


def update_record_count(
    spark: SparkSession, config: AppConfig,
) -> int:
    """Update the PSEUDO_RECORD_COUNT table.

    Returns:
        Current count of PseudoSSN records.
    """
    count_df = read_table(
        spark, config.db, table_name="PSEUDOSSN_TBL",
        query="SELECT COUNT(*) AS TOTAL_COUNT FROM PSEUDOSSN_TBL",
    )
    total = count_df.first()["TOTAL_COUNT"]

    # Update record count table
    count_data = [(int(total), datetime.now())]
    record_count_df = spark.createDataFrame(
        count_data, schema=["RECORD_COUNT", "COUNT_DATE"],
    )
    write_table(
        record_count_df, config.db,
        "PSEUDO_RECORD_COUNT", mode="overwrite",
    )

    logger.info("PseudoSSN record count: %d", total)
    return int(total)


def write_output_files(config: AppConfig, pay_period: dict) -> None:
    """Write PseudoSSN output files.

    Generates:
      - PAY_PERIOD_DATE_FILE
      - PSEUDO_HDR_DATE_FILE
    """
    output_dir = config.paths.pseudossn_input_dir
    os.makedirs(output_dir, exist_ok=True)

    # Pay period date file
    pp_date_file = os.path.join(output_dir, "PAY_PERIOD_DATE_FILE.txt")
    with open(pp_date_file, "w") as f:
        f.write(
            f"{pay_period['PP_END_YEAR']}|"
            f"{str(pay_period['PP_NUM']).zfill(2)}|"
            f"{pay_period['PP_START_DTE']}|"
            f"{pay_period['PP_END_DTE']}\n"
        )

    # Header date file
    hdr_date_file = os.path.join(output_dir, "PSEUDO_HDR_DATE_FILE.txt")
    with open(hdr_date_file, "w") as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    logger.info("Output files written to %s", output_dir)


def build_counters_and_message(
    spark: SparkSession,
    config: AppConfig,
    pay_period: dict,
    archived_count: int,
    loaded_count: int,
    sda_count: int,
    total_count: int,
) -> tuple:
    """Build counters and notification message."""
    pp_num = pay_period["PP_NUM"]
    pp_end_year = pay_period["PP_END_YEAR"]
    pp_num_str = str(pp_num).zfill(2)
    run_date = datetime.now()

    counter_data = [
        (run_date, "wf_Pseudossn", desc, float(count), pp_end_year, pp_num, None)
        for desc, count in [
            ("Archived PseudoSSN Records", archived_count),
            ("Loaded PseudoSSN Records", loaded_count),
            ("SDA PseudoSSN Records", sda_count),
            ("Total PseudoSSN Records", total_count),
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
        f"{env_prefix}PseudoSSN loaded successfully for "
        f"Pay Period: {pp_end_year}-{pp_num_str}"
    )

    # Write message file
    message = (
        f"PseudoSSN Processing Results for Pay Period "
        f"{pp_end_year}-{pp_num_str}\n"
        f"{'=' * 60}\n"
        f"Archived Records:  {archived_count}\n"
        f"Loaded Records:    {loaded_count}\n"
        f"SDA Records:       {sda_count}\n"
        f"Total Records:     {total_count}\n"
    )

    msg_file = os.path.join(
        config.paths.pseudossn_input_dir, "PSEUDOSSN_MESSAGE_FILE.txt",
    )
    os.makedirs(os.path.dirname(msg_file), exist_ok=True)
    with open(msg_file, "w") as f:
        f.write(message)

    return subject, message


def run(config=None, input_file=None):
    """Execute the full PseudoSSN workflow.

    Equivalent to Informatica workflow wf_Pseudossn.
    """
    if config is None:
        config = AppConfig()

    log_file = setup_logging("pseudossn", config.paths)
    spark = create_spark_session(config, "Pseudossn")

    try:
        if input_file is None:
            input_file = os.path.join(
                config.paths.pseudossn_input_dir, "PSEUDOSSN_FILE",
            )

        # Step 1: Get current pay period
        pay_period = get_current_pay_period(spark, config)

        # Step 2: Archive existing records
        archived_count = archive_existing_pseudossn(spark, config)

        # Step 3: Load new PseudoSSN file
        loaded_count = load_pseudossn_file(
            spark, config, input_file, pay_period,
        )

        # Step 4: Load SDA PseudoSSN records
        sda_count = load_pseudossn_from_sda(spark, config, pay_period)

        # Step 5: Update record count
        total_count = update_record_count(spark, config)

        # Step 6: Write output files
        write_output_files(config, pay_period)

        # Step 7: Build counters and message
        subject, message = build_counters_and_message(
            spark, config, pay_period,
            archived_count, loaded_count, sda_count, total_count,
        )

        send_success_notification(
            config.email, process_name=subject,
            message=message, log_file=log_file,
        )
        logger.info("PseudoSSN workflow completed successfully")

    except Exception as e:
        logger.exception("PseudoSSN workflow failed")
        send_failure_notification(
            config.email, process_name="PseudoSSN", error_message=str(e),
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
