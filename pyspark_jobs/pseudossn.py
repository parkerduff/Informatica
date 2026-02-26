"""
PySpark migration of Informatica workflow: Pseudossn
Source XML: XML/Pseudossn

Loads pseudo-SSN mappings for PII de-identification. Reads from
PSEUDOSSN_FROM_SDA_TBL (Oracle) and PSEUDOSSN_FILE_TK_NUM (flat file),
performs date parsing and signed numeric handling, then updates
PSEUDOSSN_TBL using Update Strategy DD_UPDATE.

Outputs go to COUNTER_TBL, PSEUDOSSN_MESSAGE_FILE, ERROR_TBL.

Original schedule: ONDEMAND
"""

import os
from datetime import datetime
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

from utils import (
    ENV_PREFIX,
    PerfTimer,
    format_pp_num,
    get_current_pay_period,
    get_jdbc_url,
    get_param,
    get_spark_session,
    logger,
    parse_parameter_file,
    read_oracle_table,
    send_email,
    write_counter,
    write_oracle_table,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
JOB_NAME = "wf_Pseudossn"
MAPPING_NAME = "m_Pseudossn_Load_Pseudossn_From_SDA_Tbl"
BAD_FILE_DIR = os.environ.get("PM_BAD_FILE_DIR", "/data/BIISINT/data/int/bad")
TARGET_FILE_DIR = os.environ.get("PM_TARGET_FILE_DIR", "/data/BIISINT/data/int/out")
PARAM_FILE = os.environ.get(
    "BIIS_PARAM_FILE", "/data/BIISINT/control/BIIS_parms.iparms"
)


# ===================================================================
# Signed Numeric Handling
# ===================================================================
def parse_signed_numeric(value_col: str, sign_position: int = 5) -> F.Column:
    """Parse a signed numeric field from Informatica's COMP-3 convention.

    Extracts the sign character from the specified position:
    - '+' or no sign -> positive
    - '-' -> negative

    Implements the Informatica logic:
        DECODE(TRUE,
            IS_NUMBER(v) AND sign='+', TO_DECIMAL(v,2),
            IS_NUMBER(v) AND sign='-', TO_DECIMAL(v,2)*-1,
            IS_NUMBER(v), TO_DECIMAL(v,2),
            0)
    """
    value = F.col(value_col)
    # Extract the sign from the last character of the sign position
    sign_char = F.substring(value, sign_position + 1, 1)
    # Extract the numeric portion (everything before the sign)
    numeric_part = F.substring(value, 1, sign_position)

    return (
        F.when(
            numeric_part.rlike("^[0-9.]+$") & (sign_char == F.lit("+")),
            numeric_part.cast(DecimalType(18, 2)),
        )
        .when(
            numeric_part.rlike("^[0-9.]+$") & (sign_char == F.lit("-")),
            numeric_part.cast(DecimalType(18, 2)) * F.lit(-1),
        )
        .when(
            numeric_part.rlike("^[0-9.]+$"),
            numeric_part.cast(DecimalType(18, 2)),
        )
        .otherwise(F.lit(0).cast(DecimalType(18, 2)))
    )


# ===================================================================
# Step 1: Get Current Pay Period
# ===================================================================
def step_get_pay_period(
    spark: SparkSession,
    jdbc_url: str,
) -> Tuple[int, int]:
    """Look up current pay period.

    Returns (pp_end_year, pp_num).
    """
    logger.info("Step 1: Get Current Pay Period")
    row = get_current_pay_period(spark, jdbc_url=jdbc_url)
    pp_num = int(row["PP_NUM"])
    pp_end_year = int(row["PP_END_YEAR"])
    logger.info("Current pay period: PP_END_YEAR=%d, PP_NUM=%d", pp_end_year, pp_num)
    return (pp_end_year, pp_num)


# ===================================================================
# Step 2: Read source data (SDA table + flat file)
# ===================================================================
def step_read_sources(
    spark: SparkSession,
    jdbc_url: str,
    flat_file_path: str,
) -> Tuple[DataFrame, DataFrame]:
    """Read PSEUDOSSN_FROM_SDA_TBL (Oracle) and PSEUDOSSN_FILE_TK_NUM
    (flat file) sources.

    Returns (sda_df, file_df).
    """
    logger.info("Step 2: Read source data")

    # Source 1: Oracle table
    sda_df = read_oracle_table(
        spark, "PSEUDOSSN_FROM_SDA_TBL", jdbc_url=jdbc_url,
    )
    logger.info("Read %d row(s) from PSEUDOSSN_FROM_SDA_TBL", sda_df.count())

    # Source 2: Flat file (if it exists)
    if os.path.isfile(flat_file_path):
        file_df = spark.read.option("header", "true").csv(flat_file_path)
        logger.info("Read %d row(s) from %s", file_df.count(), flat_file_path)
    else:
        logger.warning("Flat file not found: %s", flat_file_path)
        file_df = spark.createDataFrame([], StructType([
            StructField("TK_NUM", StringType(), True),
        ]))

    return (sda_df, file_df)


# ===================================================================
# Step 3: Apply Conversions (exp_Conversions)
# ===================================================================
def step_apply_conversions(
    sda_df: DataFrame,
) -> DataFrame:
    """Apply the exp_Conversions transformation logic.

    Key transformations:
    - HIRE_DATE: MMDDYYYY -> date (null-safe)
    - UNIF_ALLOW_DATE: YYYYMMDD -> date (null-safe)
    - UNIF_ALLOW_AMT: signed numeric handling
    """
    logger.info("Step 3: Apply Conversions")

    # Date parsing: HIRE_DATE (format MMDDYYYY)
    # Informatica: SUBSTR(in_HIRE_DATE, 1, 2) || '/' ||
    #              SUBSTR(in_HIRE_DATE, 3, 2) || '/' ||
    #              SUBSTR(in_HIRE_DATE, 5, 4)
    converted_df = sda_df

    if "HIRE_DATE" in sda_df.columns:
        converted_df = converted_df.withColumn(
            "HIRE_DATE_CONVERTED",
            F.when(
                F.col("HIRE_DATE").isNotNull()
                & (F.length(F.col("HIRE_DATE")) >= 8)
                & F.col("HIRE_DATE").rlike("^[0-9]{8}$"),
                F.to_date(F.col("HIRE_DATE"), "MMddyyyy"),
            ),
        )
    else:
        converted_df = converted_df.withColumn(
            "HIRE_DATE_CONVERTED", F.lit(None).cast(DateType())
        )

    # Date parsing: UNIF_ALLOW_DATE (format YYYYMMDD)
    # Informatica: SUBSTR(in_UNIF_ALLOW_DATE, 5, 2) || '/' ||
    #              SUBSTR(in_UNIF_ALLOW_DATE, 7, 2) || '/' ||
    #              SUBSTR(in_UNIF_ALLOW_DATE, 1, 4)
    if "UNIF_ALLOW_DATE" in sda_df.columns:
        converted_df = converted_df.withColumn(
            "UNIF_ALLOW_DATE_CONVERTED",
            F.when(
                F.col("UNIF_ALLOW_DATE").isNotNull()
                & (F.length(F.col("UNIF_ALLOW_DATE")) >= 8)
                & F.col("UNIF_ALLOW_DATE").rlike("^[0-9]{8}$"),
                F.to_date(F.col("UNIF_ALLOW_DATE"), "yyyyMMdd"),
            ),
        )
    else:
        converted_df = converted_df.withColumn(
            "UNIF_ALLOW_DATE_CONVERTED", F.lit(None).cast(DateType())
        )

    # Signed numeric: UNIF_ALLOW_AMT
    # Informatica: SUBSTR(in_UNIF_ALLOW_AMT, 6, 1) extracts sign
    # DECODE(TRUE, IS_NUMBER(v) AND sign='+', TO_DECIMAL(v,2),
    #        IS_NUMBER(v) AND sign='-', TO_DECIMAL(v,2)*-1,
    #        IS_NUMBER(v), TO_DECIMAL(v,2), 0)
    if "UNIF_ALLOW_AMT" in sda_df.columns:
        # Extract sign from position 6 (1-indexed)
        sign_char = F.substring(F.col("UNIF_ALLOW_AMT"), 6, 1)
        numeric_part = F.substring(F.col("UNIF_ALLOW_AMT"), 1, 5)

        converted_df = converted_df.withColumn(
            "UNIF_ALLOW_AMT_CONVERTED",
            F.when(
                numeric_part.rlike("^[0-9.]+$") & (sign_char == F.lit("+")),
                numeric_part.cast(DecimalType(18, 2)),
            )
            .when(
                numeric_part.rlike("^[0-9.]+$") & (sign_char == F.lit("-")),
                numeric_part.cast(DecimalType(18, 2)) * F.lit(-1),
            )
            .when(
                numeric_part.rlike("^[0-9.]+$"),
                numeric_part.cast(DecimalType(18, 2)),
            )
            .otherwise(F.lit(0).cast(DecimalType(18, 2))),
        )
    else:
        converted_df = converted_df.withColumn(
            "UNIF_ALLOW_AMT_CONVERTED", F.lit(0).cast(DecimalType(18, 2))
        )

    return converted_df


# ===================================================================
# Step 4: Update PSEUDOSSN_TBL (Update Strategy DD_UPDATE)
# ===================================================================
def step_update_pseudossn_tbl(
    spark: SparkSession,
    jdbc_url: str,
    converted_df: DataFrame,
    file_df: DataFrame,
) -> Tuple[int, int]:
    """Update PSEUDOSSN_TBL using Update Strategy (DD_UPDATE).

    In PySpark: JDBC UPDATE or write with overwrite mode.
    Returns (updated_count, error_count).
    """
    logger.info("Step 4: Update PSEUDOSSN_TBL")

    updated_count = 0
    error_count = 0

    # Process SDA table data
    sda_count = converted_df.count()
    if sda_count > 0:
        # Build target columns for PSEUDOSSN_TBL
        target_columns = []
        for col_name in converted_df.columns:
            if not col_name.endswith("_CONVERTED"):
                target_columns.append(col_name)

        # Add converted columns with proper names
        update_df = converted_df
        if "HIRE_DATE_CONVERTED" in converted_df.columns:
            update_df = update_df.withColumn("HIRE_DATE", F.col("HIRE_DATE_CONVERTED"))
        if "UNIF_ALLOW_DATE_CONVERTED" in converted_df.columns:
            update_df = update_df.withColumn("UNIF_ALLOW_DATE", F.col("UNIF_ALLOW_DATE_CONVERTED"))
        if "UNIF_ALLOW_AMT_CONVERTED" in converted_df.columns:
            update_df = update_df.withColumn("UNIF_ALLOW_AMT", F.col("UNIF_ALLOW_AMT_CONVERTED"))

        # Drop _CONVERTED columns
        for col_name in update_df.columns:
            if col_name.endswith("_CONVERTED"):
                update_df = update_df.drop(col_name)

        # Write to PSEUDOSSN_TBL (DD_UPDATE -> overwrite/merge)
        try:
            write_oracle_table(
                update_df, "PSEUDOSSN_TBL", mode="append", jdbc_url=jdbc_url,
            )
            updated_count = sda_count
            logger.info("Updated %d row(s) in PSEUDOSSN_TBL from SDA", updated_count)
        except Exception as exc:
            logger.error("Failed to update PSEUDOSSN_TBL from SDA: %s", exc)
            error_count = sda_count

    # Process flat file data (TK_NUM updates)
    if file_df.count() > 0:
        try:
            # For each row in file_df, update PSEUDOSSN_TBL.TK_NUM
            file_count = file_df.count()
            write_oracle_table(
                file_df, "PSEUDOSSN_TBL", mode="append", jdbc_url=jdbc_url,
            )
            updated_count += file_count
            logger.info("Updated %d row(s) in PSEUDOSSN_TBL from file", file_count)
        except Exception as exc:
            logger.error("Failed to update PSEUDOSSN_TBL from file: %s", exc)
            error_count += file_df.count()

    return (updated_count, error_count)


# ===================================================================
# Step 5: Write counters, errors, and message file
# ===================================================================
def step_finalize(
    spark: SparkSession,
    jdbc_url: str,
    pp_end_year: int,
    pp_num: int,
    updated_count: int,
    error_count: int,
    email_list: str,
) -> None:
    """Write counters, error records, and send email notification."""
    logger.info("Step 5: Finalize - write counters and send email")

    run_date = datetime.now()
    process_name = MAPPING_NAME

    # Write counters
    counters = {
        "Records updated in PSEUDOSSN_TBL": updated_count,
        "Error records": error_count,
    }
    for desc, value in counters.items():
        write_counter(
            spark,
            process_name=process_name,
            counter_description=desc,
            counter_value=value,
            pp_end_year=pp_end_year,
            pp_num=pp_num,
            run_date=run_date,
            jdbc_url=jdbc_url,
        )

    # Write message file
    pp_num_str = format_pp_num(pp_num)
    subject = (
        f"{ENV_PREFIX}Pseudossn load completed for "
        f"Pay Period: {pp_end_year}-{pp_num_str}"
    )
    message = (
        f"Pseudossn load completed.\n"
        f"Records updated: {updated_count}\n"
        f"Error records: {error_count}\n"
        f"Pay Period: {pp_end_year}-{pp_num_str}"
    )

    message_file = os.path.join(TARGET_FILE_DIR, "pseudossn_message.txt")
    os.makedirs(os.path.dirname(message_file) or ".", exist_ok=True)
    with open(message_file, "w") as fh:
        fh.write(f"{subject}\n{message}\n")
    logger.info("Message file written to %s", message_file)

    # Send email
    recipients = [
        addr.strip()
        for addr in email_list.replace(";", " ").split()
        if addr.strip()
    ]
    send_email(subject=subject, body=message, recipients=recipients or None)


# ===================================================================
# Main workflow
# ===================================================================
def run_pseudossn() -> None:
    """Execute the full wf_Pseudossn workflow."""
    spark = get_spark_session(JOB_NAME)
    jdbc_url = get_jdbc_url()

    # Load parameters
    params = parse_parameter_file(PARAM_FILE)
    email_list = get_param(
        params,
        "$$WF_PSEUDOSSN_EMAIL_LIST",
        "WF_PSEUDOSSN_EMAIL_LIST",
        default="peter.chen@hhs.gov nathan.knight@hhs.gov marvin.simon@hhs.gov",
    )
    flat_file_path = get_param(
        params,
        "$Param_PSEUDOSSN_File",
        "PARAM_PSEUDOSSN_FILE",
        default="/data/BIISINT/data/int/in/PSEUDOSSN/pseudossn_tk_num.csv",
    )

    perf = PerfTimer(JOB_NAME)
    src_rows = 0
    tgt_rows = 0
    error_rows = 0

    with perf:
        try:
            # Step 1: Get pay period
            pp_end_year, pp_num = step_get_pay_period(spark, jdbc_url)

            # Step 2: Read sources
            sda_df, file_df = step_read_sources(spark, jdbc_url, flat_file_path)
            src_rows = sda_df.count() + file_df.count()

            # Step 3: Apply conversions
            converted_df = step_apply_conversions(sda_df)

            # Step 4: Update PSEUDOSSN_TBL
            tgt_rows, error_rows = step_update_pseudossn_tbl(
                spark, jdbc_url, converted_df, file_df,
            )

            # Step 5: Finalize
            step_finalize(
                spark, jdbc_url, pp_end_year, pp_num,
                tgt_rows, error_rows, email_list,
            )

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
        error_rows=error_rows,
    )
    try:
        perf.log_to_table(
            spark, src_rows=src_rows, tgt_rows=tgt_rows, error_rows=error_rows,
        )
    except Exception:
        logger.warning("Could not write perf metrics to MIGRATION_PERF_LOG table", exc_info=True)

    spark.stop()


if __name__ == "__main__":
    run_pseudossn()
