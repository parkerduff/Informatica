"""
PySpark migration of Informatica workflow: wf_COMPTIME
Source XML: XML/COMPTIME

Loads compensatory time data from a CSV flat file into COMP_TIME_DAILY_TBL,
builds counter/message records, and sends a notification email.

Workflow sequence (strictly sequential):
    Start -> s_COMPTIME_Current_Pay_Period
          -> s_COMPTIME_Load_COMP_TIME_DAILY_TBL
          -> s_COMPTIME_Build_Message_Counters
          -> email_COMPTIME_Complete

Original schedule: ONDEMAND on Test_IS / Dom_dev
"""

import os
import shutil
from datetime import datetime

from pyspark.sql import SparkSession
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
JOB_NAME = "wf_COMPTIME"
MAPPING_NAME = "m_COMPTIME_Load_COMP_TIME_DAILY_TBL"
BAD_FILE_DIR = os.environ.get("PM_BAD_FILE_DIR", "/data/BIISINT/data/int/bad")
TARGET_FILE_DIR = os.environ.get("PM_TARGET_FILE_DIR", "/data/BIISINT/data/int/out")
PARAM_FILE = os.environ.get(
    "BIIS_PARAM_FILE", "/data/BIISINT/control/BIIS_parms.iparms"
)

# Source flat file schema (MS1252 / comma-delimited / double-quoted)
COMPTIME_SCHEMA = StructType(
    [
        StructField("SSN", StringType(), True),
        StructField("NAME", StringType(), True),
        StructField("CURRENT_ACCT", StringType(), True),
        StructField("CURRENT_ORG", StringType(), True),
        StructField("FLSA_STATUS", StringType(), True),
        StructField("COMP_TIME_CUR_BAL", StringType(), True),
        StructField("COMP_TIME_YEAR_EARNED", StringType(), True),
        StructField("PP_END_DATE", StringType(), True),
        StructField("DAILY_DATE_EARNED", StringType(), True),
        StructField("COMP_TIME_RATE", StringType(), True),
        StructField("COMP_TIME_HOURS", StringType(), True),
        StructField("COMP_TIME_UNDEF", StringType(), True),
    ]
)


# ===================================================================
# Step 1: Get Current Pay Period  (s_COMPTIME_Current_Pay_Period)
# ===================================================================
def step_get_current_pay_period(
    spark: SparkSession,
    jdbc_url: str,
) -> tuple:
    """Look up current pay period from PAY_PERIOD table.

    Returns (pp_num, pp_end_year).
    """
    logger.info("Step 1: Get Current Pay Period")
    row = get_current_pay_period(spark, jdbc_url=jdbc_url)
    pp_num = int(row["PP_NUM"])
    pp_end_year = int(row["PP_END_YEAR"])
    logger.info("Current pay period: PP_END_YEAR=%d, PP_NUM=%d", pp_end_year, pp_num)
    return (pp_num, pp_end_year)


# ===================================================================
# Step 2: Load COMP_TIME_DAILY_TBL
#         (s_COMPTIME_Load_COMP_TIME_DAILY_TBL)
# ===================================================================
def step_load_comp_time_daily(
    spark: SparkSession,
    jdbc_url: str,
    comptime_filepath: str,
    pp_end_year: int,
    pp_num: int,
    pp_year_num: int,
) -> int:
    """Read the COMPTIME CSV, filter detail records, convert dates,
    join with PAY_PERIOD lookup, and insert into COMP_TIME_DAILY_TBL.

    Returns the number of rows written.
    """
    logger.info("Step 2: Load COMP_TIME_DAILY_TBL from %s", comptime_filepath)

    # --- Read source flat file ---
    raw_df = (
        spark.read.option("header", "false")
        .option("encoding", "windows-1252")
        .option("quote", '"')
        .option("escape", '"')
        .schema(COMPTIME_SCHEMA)
        .csv(comptime_filepath)
    )

    src_count = raw_df.count()
    logger.info("Read %d row(s) from source file", src_count)

    # --- exp_Initial: determine RECORD_TYPE_FLAG ---
    # DECODE(TRUE, IS_NUMBER(SSN), 'D', 'NO')
    df = raw_df.withColumn(
        "RECORD_TYPE_FLAG",
        F.when(F.col("SSN").rlike("^[0-9]+$"), F.lit("D")).otherwise(F.lit("NO")),
    )

    # --- fil_Detail: filter RECORD_TYPE_FLAG = 'D' ---
    detail_df = df.filter(F.col("RECORD_TYPE_FLAG") == "D")
    detail_count = detail_df.count()
    logger.info("Detail records after filter: %d", detail_count)

    # --- lkp_PAY_PERIOD: lookup current pay period ---
    pay_period_df = read_oracle_table(
        spark, "PAY_PERIOD", jdbc_url=jdbc_url, predicate="CURR_PP_FLAG = 'Y'",
    ).select(
        F.col("PP_NUM").alias("lkp_PP_NUM"),
        F.col("PP_END_YEAR").alias("lkp_PP_END_YEAR"),
    )

    # Broadcast join (single-row lookup)
    detail_with_pp = detail_df.crossJoin(F.broadcast(pay_period_df))

    # --- exp_Convert: date conversions ---
    converted_df = detail_with_pp.withColumn(
        "PP_END_DATE_CONVERTED",
        F.when(
            F.col("PP_END_DATE").rlike("^[0-9]{8}$"),
            F.to_date(F.col("PP_END_DATE"), "yyyyMMdd"),
        ),
    ).withColumn(
        "DAILY_DATE_EARNED_CONVERTED",
        F.when(
            F.col("DAILY_DATE_EARNED").rlike("^[0-9]{8}$"),
            F.to_date(F.col("DAILY_DATE_EARNED"), "yyyyMMdd"),
        ),
    )

    # --- Build target DataFrame for COMP_TIME_DAILY_TBL ---
    target_df = converted_df.select(
        F.lit(pp_end_year).cast(DecimalType(4, 0)).alias("PP_END_YEAR"),
        F.lit(pp_num).cast(DecimalType(2, 0)).alias("PP_NUM"),
        F.lit(pp_year_num).cast(DecimalType(6, 0)).alias("PP_YEAR_NUM"),
        F.col("SSN").cast(StringType()),
        F.col("NAME").cast(StringType()),
        F.col("CURRENT_ACCT").cast(StringType()),
        F.col("CURRENT_ORG").cast(StringType()),
        F.col("FLSA_STATUS").cast(StringType()),
        F.col("COMP_TIME_CUR_BAL").cast(DecimalType(8, 2)),
        F.col("COMP_TIME_YEAR_EARNED").cast(DecimalType(4, 0)),
        F.col("PP_END_DATE_CONVERTED").cast(DateType()).alias("PP_END_DATE"),
        F.col("DAILY_DATE_EARNED_CONVERTED").cast(DateType()).alias("DAILY_DATE_EARNED"),
        F.col("COMP_TIME_RATE").cast(DecimalType(6, 2)),
        F.col("COMP_TIME_HOURS").cast(DecimalType(8, 2)),
        F.col("COMP_TIME_UNDEF").cast(DecimalType(6, 0)),
    )

    # --- Write to Oracle ---
    tgt_count = target_df.count()
    write_oracle_table(target_df, "COMP_TIME_DAILY_TBL", mode="append", jdbc_url=jdbc_url)
    logger.info("Wrote %d row(s) to COMP_TIME_DAILY_TBL", tgt_count)

    return tgt_count


def step_archive_source_file(
    comptime_filepath: str,
    param_root_directory: str,
    pp_year_num: int,
) -> None:
    """Post-session success: archive the source file.

    Replicates Informatica post-session success command.
    """
    archive_dir = os.path.join(param_root_directory, "data", "archive", "COMPTIME")
    os.makedirs(archive_dir, exist_ok=True)

    archive_filename = f"u0827d01_P{pp_year_num}.txt"
    archive_path = os.path.join(archive_dir, archive_filename)

    shutil.move(comptime_filepath, archive_path)
    logger.info("Archived source file to %s", archive_path)


# ===================================================================
# Step 3: Build Message & Counters
#         (s_COMPTIME_Build_Message_Counters)
# ===================================================================
def step_build_message_counters(
    spark: SparkSession,
    jdbc_url: str,
    detail_record_count: int,
    pp_end_year: int,
    pp_num: int,
) -> tuple:
    """Build counters and email message.

    Writes to COUNTER_TBL and COMPTIME_MESSAGE_FILE.
    Returns (subject, message).
    """
    logger.info("Step 3: Build Message & Counters")

    run_date = datetime.now()
    process_name = "m_COMPTIME_Build_Message_Counters"
    counter_description = "Number of detail records from the COMP TIME file."

    # --- Write counter to COUNTER_TBL ---
    write_counter(
        spark,
        process_name=process_name,
        counter_description=counter_description,
        counter_value=detail_record_count,
        pp_end_year=pp_end_year,
        pp_num=pp_num,
        run_date=run_date,
        jdbc_url=jdbc_url,
    )

    # --- Build email subject and message ---
    pp_num_str = format_pp_num(pp_num)
    subject = (
        f"{ENV_PREFIX}Comp Time File loaded successfully "
        f"for Pay Period: {pp_end_year}-{pp_num_str}"
    )
    message = (
        f"Number of Detail Records from Comp Time file\t= {detail_record_count}"
    )

    # --- Write flat file target COMPTIME_MESSAGE_FILE ---
    message_file = os.path.join(TARGET_FILE_DIR, "comptime_message.txt")
    os.makedirs(os.path.dirname(message_file) or ".", exist_ok=True)
    with open(message_file, "w") as fh:
        fh.write(f"{subject}\n{message}\n")
    logger.info("Message file written to %s", message_file)

    return (subject, message)


# ===================================================================
# Step 4: Email  (email_COMPTIME_Complete)
# ===================================================================
def step_email_comptime(
    subject: str,
    message: str,
    email_list: str,
) -> None:
    """Send COMPTIME completion notification email."""
    logger.info("Step 4: Email COMPTIME Complete")

    recipients = [addr.strip() for addr in email_list.split(";") if addr.strip()]
    if not recipients:
        recipients = [addr.strip() for addr in email_list.split() if addr.strip()]

    send_email(subject=subject, body=message, recipients=recipients or None)


# ===================================================================
# Main workflow
# ===================================================================
def run_comptime() -> None:
    """Execute the full wf_COMPTIME workflow."""
    spark = get_spark_session(JOB_NAME)
    jdbc_url = get_jdbc_url()

    # Load parameters
    params = parse_parameter_file(PARAM_FILE)
    pp_end_year_param = get_param(params, "$$WF_PP_END_YEAR", "WF_PP_END_YEAR")
    pp_num_param = get_param(params, "$$WF_PP_NUM", "WF_PP_NUM")
    pp_year_num_param = get_param(params, "$$WF_PP_YEAR_NUM", "WF_PP_YEAR_NUM")
    email_list = get_param(
        params,
        "$$WF_COMPTIME_EMAIL_LIST",
        "WF_COMPTIME_EMAIL_LIST",
        default="peter.chen@hhs.gov nathan.knight@hhs.gov marvin.simon@hhs.gov",
    )

    param_root_directory = get_param(
        params, "$Param_Root_Directory", "PARAM_ROOT_DIRECTORY",
        default="/data/BIISINT",
    )
    comptime_filename = get_param(
        params, "$Param_COMPTIME_filename", "PARAM_COMPTIME_FILENAME",
        default="u0287d01.txt",
    )
    comptime_filepath = os.path.join(
        param_root_directory, "data", "int", "in", "COMPTIME", comptime_filename,
    )

    perf = PerfTimer(JOB_NAME)
    src_rows = 0
    tgt_rows = 0

    with perf:
        try:
            # Step 1: Get current pay period
            pp_num_db, pp_end_year_db = step_get_current_pay_period(spark, jdbc_url)

            # Use DB values if params not set
            pp_end_year = int(pp_end_year_param) if pp_end_year_param else pp_end_year_db
            pp_num = int(pp_num_param) if pp_num_param else pp_num_db
            pp_year_num = (
                int(pp_year_num_param)
                if pp_year_num_param
                else pp_end_year * 100 + pp_num
            )

            # Step 2: Load COMP_TIME_DAILY_TBL
            tgt_rows = step_load_comp_time_daily(
                spark, jdbc_url, comptime_filepath,
                pp_end_year, pp_num, pp_year_num,
            )
            src_rows = tgt_rows

            # Post-session: archive source file
            step_archive_source_file(comptime_filepath, param_root_directory, pp_year_num)

            # Step 3: Build Message & Counters
            subject, message = step_build_message_counters(
                spark, jdbc_url, tgt_rows, pp_end_year, pp_num,
            )

            # Step 4: Email
            step_email_comptime(subject, message, email_list)

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
    run_comptime()
