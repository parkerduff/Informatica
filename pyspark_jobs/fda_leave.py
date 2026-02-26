"""
PySpark migration of Informatica workflow: wf_FDA_Leave
Source XML: XML/FDA_Leave

Multi-session workflow for FDA leave file processing. Loads FDA flat file
data to staging, sets pay calendar, validates against CPM staging tables,
extracts leave records, and sends notification emails.

Workflow sequence:
    Start -> s_0025_PM_FDA_Set_Pay_Calendar
          -> [FDA file load sessions]
          -> s_0150_PM_FDA_Error_Counter (validation)
          -> s_0200_PM_FDA_Extract_Leave
          -> email

Original schedule: ONDEMAND on Test_IS / Dom_dev (Test_Repo_Srvc)
"""

import os
from datetime import datetime
from typing import Dict, Tuple

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
    write_counter,
    write_error_df,
    write_oracle_table,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
JOB_NAME = "wf_FDA_Leave"
BAD_FILE_DIR = os.environ.get("PM_BAD_FILE_DIR", "/data/BIISINT/data/int/bad")
TARGET_FILE_DIR = os.environ.get("PM_TARGET_FILE_DIR", "/data/BIISINT/data/int/out")
PARAM_FILE = os.environ.get(
    "BIIS_PARAM_FILE", "/data/BIISINT/control/BIIS_parms.iparms"
)


# ===================================================================
# Step 1: Set Pay Calendar (m_0025_PM_FDA_Set_Pay_Calendar)
# ===================================================================
def step_set_pay_calendar(
    spark: SparkSession,
    jdbc_url: str,
    pp_end_year: int,
    pp_num: int,
) -> Tuple[int, int, int, int]:
    """Set pay period context from CPM_CYCLE_TBL.

    Reads CPM_CYCLE_TBL where PROCESS_NAME = 'FDA', updates it with
    current cycle info.

    Returns (pp_end_year, pp_num, cycle_id, map_cycle_id).
    """
    logger.info("Step 1: Set Pay Calendar (m_0025_PM_FDA_Set_Pay_Calendar)")

    # Read CPM_CYCLE_TBL for FDA process
    cycle_df = read_oracle_table(
        spark, "CPM_CYCLE_TBL", jdbc_url=jdbc_url,
        predicate="PROCESS_NAME = 'FDA'",
    )

    cycle_row = cycle_df.first()
    if cycle_row is None:
        raise RuntimeError("No CPM_CYCLE_TBL entry found for PROCESS_NAME = 'FDA'")

    cycle_id = int(cycle_row["CYCLE_ID"]) if cycle_row["CYCLE_ID"] is not None else 0
    map_cycle_id = cycle_id

    # If params are set, use them; otherwise use CPM_CYCLE_TBL values
    if pp_end_year > 0 and pp_num > 0:
        target_pp_end_year = pp_end_year
        target_pp_num = pp_num
    else:
        target_pp_end_year = int(cycle_row["PP_END_YEAR"]) if cycle_row["PP_END_YEAR"] else 0
        target_pp_num = int(cycle_row["PP_NUM"]) if cycle_row["PP_NUM"] else 0

    # Update CPM_CYCLE_TBL (DD_UPDATE)
    update_sql = (
        f"UPDATE CPM_CYCLE_TBL SET "
        f"PP_END_YEAR = {target_pp_end_year}, "
        f"PP_NUM = {target_pp_num}, "
        f"CYCLE_ID = {cycle_id} "
        f"WHERE PROCESS_NAME = 'FDA'"
    )
    execute_jdbc_statement(update_sql, jdbc_url=jdbc_url)
    logger.info(
        "Updated CPM_CYCLE_TBL: PP_END_YEAR=%d, PP_NUM=%d, CYCLE_ID=%d",
        target_pp_end_year, target_pp_num, cycle_id,
    )

    return (target_pp_end_year, target_pp_num, cycle_id, map_cycle_id)


# ===================================================================
# Step 2: Load FDA File (m_0010_PM_FDA_Load_FDA_File)
# ===================================================================
def step_load_fda_file(
    spark: SparkSession,
    jdbc_url: str,
    fda_file_path: str,
) -> int:
    """Load the ITAS flat file to HI_PM_FDA_TATRAN_TBL staging table.

    Filters out record types '01' and '99'.
    After loading, executes CRITICAL post-SQL DELETE to remove employees
    without a type '12' record.

    Returns the count of rows loaded.
    """
    logger.info("Step 2: Load FDA File (m_0010_PM_FDA_Load_FDA_File)")
    logger.info("Source file: %s", fda_file_path)

    # Read fixed-width flat file from ITAS
    # The file structure depends on the ITAS export; read as text lines
    raw_df = spark.read.text(fda_file_path)

    # Parse fixed-width fields - key fields for filtering
    # FDA_REC_TYPE is typically in a fixed position in the record
    # Parse the record type from the flat file (position depends on file format)
    parsed_df = raw_df.withColumn(
        "FDA_REC_TYPE", F.trim(F.substring(F.col("value"), 1, 2))
    ).withColumn(
        "FDA_EMP_ID", F.trim(F.substring(F.col("value"), 3, 10))
    ).withColumn(
        "FDA_BATCH_ID", F.trim(F.substring(F.col("value"), 13, 10))
    ).withColumn(
        "FDA_TK_NO", F.trim(F.substring(F.col("value"), 23, 10))
    ).withColumn(
        "FDA_SEQ", F.trim(F.substring(F.col("value"), 33, 5))
    ).withColumn(
        "FDA_DATA", F.col("value")
    )

    # Filter out record types 01 (header) and 99 (trailer)
    filtered_df = parsed_df.filter(
        ~F.col("FDA_REC_TYPE").isin("01", "99")
    )

    src_count = filtered_df.count()
    logger.info("Filtered %d record(s) (excluding types 01 and 99)", src_count)

    # Select columns for target table
    target_df = filtered_df.select(
        F.col("FDA_REC_TYPE"),
        F.col("FDA_EMP_ID"),
        F.col("FDA_BATCH_ID"),
        F.col("FDA_TK_NO"),
        F.col("FDA_SEQ"),
        F.col("FDA_DATA"),
    )

    # Write to staging table HI_PM_FDA_TATRAN_TBL
    write_oracle_table(target_df, "HI_PM_FDA_TATRAN_TBL", mode="append", jdbc_url=jdbc_url)
    logger.info("Loaded %d row(s) to HI_PM_FDA_TATRAN_TBL", src_count)

    # ---- CRITICAL Post SQL ----
    # DELETE employees who don't have a type '12' record
    # If this is missed, it is a functional regression.
    post_sql = (
        "DELETE FROM HI_PM_FDA_TATRAN_TBL "
        "WHERE fda_emp_id NOT IN ("
        "  SELECT DISTINCT fda_emp_id FROM HI_PM_FDA_TATRAN_TBL "
        "  WHERE fda_rec_type = '12'"
        ")"
    )
    execute_jdbc_statement(post_sql, jdbc_url=jdbc_url)
    logger.info("Executed CRITICAL post-SQL: removed employees without type '12' record")

    return src_count


# ===================================================================
# Step 3: Error Counter / Validation (m_0150_PM_FDA_Error_Counter)
# ===================================================================
def step_error_counter(
    spark: SparkSession,
    jdbc_url: str,
    pp_end_year: int,
    pp_num: int,
    cycle_id: int,
) -> Dict[str, int]:
    """4-way parallel validation against CPM staging tables.

    Validates FDA employees in HI_PM_FDA_TATRAN_TBL against:
    1. CPM_YTD_DETAIL_STG_TBL
    2. CPM_PAD_DETAIL_STG_TBL
    3. CPM_MER_STG_TBL
    4. CPM_NEWPAY_TBL

    Each validation: left anti join -> filter errors -> write to ERROR_TBL.
    Returns dict of counter names to counts.
    """
    logger.info("Step 3: Error Counter (m_0150_PM_FDA_Error_Counter)")

    # Read source table
    tatran_df = read_oracle_table(spark, "HI_PM_FDA_TATRAN_TBL", jdbc_url=jdbc_url)
    tatran_df.cache()
    count_read_in = tatran_df.count()
    logger.info("TATRAN records read: %d", count_read_in)

    # Count leave records (type 02)
    leave_rec_count = tatran_df.filter(F.col("FDA_REC_TYPE") == "02").count()
    logger.info("Leave records (type 02): %d", leave_rec_count)

    run_date = datetime.now()
    process_name = "m_0150_PM_FDA_Error_Counter"
    total_error_count = 0

    # --- Validation 1: CPM_YTD_DETAIL_STG_TBL ---
    validation_tables = [
        ("CPM_YTD_DETAIL_STG_TBL", "FDA employee not found in CPM_YTD_DETAIL_STG_TBL"),
        ("CPM_PAD_DETAIL_STG_TBL", "FDA employee not found in CPM_PAD_DETAIL_STG_TBL"),
        ("CPM_MER_STG_TBL", "FDA employee not found in CPM_MER_STG_TBL"),
        ("CPM_NEWPAY_TBL", "FDA employee not found in CPM_NEWPAY_TBL"),
    ]

    # Get distinct FDA employee IDs
    fda_employees = tatran_df.select("FDA_EMP_ID").distinct()

    for table_name, error_message in validation_tables:
        try:
            stg_df = read_oracle_table(spark, table_name, jdbc_url=jdbc_url)

            # Determine the join key column in the staging table
            # Try common column names
            stg_columns = [c.upper() for c in stg_df.columns]
            join_col = None
            for candidate in ["FDA_EMP_ID", "EMPLID", "EMP_ID", "DFAS_PSEUDO_SSN"]:
                if candidate in stg_columns:
                    join_col = candidate
                    break

            if join_col is None:
                logger.warning(
                    "Could not determine join column for %s, skipping validation",
                    table_name,
                )
                continue

            # Left anti join to find employees NOT in staging table
            error_df = fda_employees.join(
                stg_df.select(F.col(join_col).alias("FDA_EMP_ID")).distinct(),
                on="FDA_EMP_ID",
                how="left_anti",
            )

            error_count = write_error_df(
                error_df,
                process_name=process_name,
                pp_end_year=pp_end_year,
                pp_num=pp_num,
                cycle_id=cycle_id,
                source_key_col="FDA_EMP_ID",
                error_message=error_message,
                jdbc_url=jdbc_url,
            )
            total_error_count += error_count
            logger.info(
                "Validation against %s: %d error(s)", table_name, error_count
            )

        except Exception as exc:
            logger.warning(
                "Could not validate against %s: %s", table_name, exc
            )

    tatran_df.unpersist()

    # Write counters
    counters = {
        "COUNT_READ_IN": count_read_in,
        "LEAVE_REC_COUNT": leave_rec_count,
        "ERROR_REC_COUNT": total_error_count,
    }
    for desc, value in counters.items():
        write_counter(
            spark,
            process_name=process_name,
            counter_description=desc,
            counter_value=value,
            pp_end_year=pp_end_year,
            pp_num=pp_num,
            cycle_id=cycle_id,
            run_date=run_date,
            jdbc_url=jdbc_url,
        )

    return counters


# ===================================================================
# Step 4: Extract Leave Records (m_0200_PM_FDA_Extract_Leave)
# ===================================================================
def step_extract_leave(
    spark: SparkSession,
    jdbc_url: str,
    pp_end_year: int,
    pp_num: int,
    cycle_id: int,
) -> Tuple[int, str, str]:
    """Extract leave records (FDA_REC_TYPE = '02') from staging.

    Validates parameters, aggregates leave records, writes output files.
    Returns (count, subject, message).
    """
    logger.info("Step 4: Extract Leave Records (m_0200_PM_FDA_Extract_Leave)")

    # --- exp_Validate_Parameters ---
    if pp_end_year <= 0:
        raise ValueError(
            f"!!!! The value : {pp_end_year} is not a valid pay period year"
        )
    if pp_num <= 0:
        raise ValueError(
            f"!!!! The value : {pp_num} is not a valid pay period number"
        )

    # Read staging table
    tatran_df = read_oracle_table(spark, "HI_PM_FDA_TATRAN_TBL", jdbc_url=jdbc_url)

    # --- fil_Leave_Records: filter FDA_REC_TYPE = '02' ---
    leave_df = tatran_df.filter(F.col("FDA_REC_TYPE") == "02")

    # --- agg_All_Leave_Recs: COUNT(*) grouped by key fields ---
    leave_agg_df = leave_df.groupBy(
        "FDA_BATCH_ID", "FDA_TK_NO", "FDA_EMP_ID", "FDA_REC_TYPE", "FDA_SEQ"
    ).agg(F.count("*").alias("REC_COUNT"))

    leave_count = leave_agg_df.count()
    logger.info("Aggregated leave records: %d", leave_count)

    # --- Write output flat file ---
    output_file = os.path.join(TARGET_FILE_DIR, "CPM_FDA_PAY_PERIOD.txt")
    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)

    # Write leave data to flat file
    leave_pdf = leave_df.toPandas()
    leave_pdf.to_csv(output_file, index=False, sep="|")
    logger.info("Wrote FDA leave extract to %s", output_file)

    # --- Build email message ---
    pp_num_str = format_pp_num(pp_num)
    subject = (
        f"{ENV_PREFIX}FDA Extract completed successfully "
        f"for Pay Period: {pp_end_year}-{pp_num_str}"
    )
    message = (
        f"FDA leave extract completed.\n"
        f"Leave records extracted: {leave_count}\n"
        f"Pay Period: {pp_end_year}-{pp_num_str}\n"
        f"Output file: {output_file}"
    )

    # Write message file
    message_file = os.path.join(TARGET_FILE_DIR, "fda_extract_message.txt")
    with open(message_file, "w") as fh:
        fh.write(f"{subject}\n{message}\n")

    # Write counters
    run_date = datetime.now()
    process_name = "m_0200_PM_FDA_Extract_Leave"
    write_counter(
        spark,
        process_name=process_name,
        counter_description="Leave records extracted",
        counter_value=leave_count,
        pp_end_year=pp_end_year,
        pp_num=pp_num,
        cycle_id=cycle_id,
        run_date=run_date,
        jdbc_url=jdbc_url,
    )

    return (leave_count, subject, message)


# ===================================================================
# Main workflow
# ===================================================================
def run_fda_leave() -> None:
    """Execute the full wf_FDA_Leave workflow."""
    spark = get_spark_session(JOB_NAME)
    jdbc_url = get_jdbc_url()

    # Load parameters
    params = parse_parameter_file(PARAM_FILE)
    pp_end_year_param = get_param(params, "$$WF_PP_END_YEAR", "WF_PP_END_YEAR")
    pp_num_param = get_param(params, "$$WF_PP_NUM", "WF_PP_NUM")
    email_list = get_param(
        params,
        "$$WF_CPM_EMAIL_LIST",
        "WF_CPM_EMAIL_LIST",
        default="peter.chen@hhs.gov nathan.knight@hhs.gov marvin.simon@hhs.gov",
    )

    pp_end_year = int(pp_end_year_param) if pp_end_year_param else 0
    pp_num = int(pp_num_param) if pp_num_param else 0

    fda_file_path = get_param(
        params, "$Param_FDA_File", "PARAM_FDA_FILE",
        default="/data/BIISINT/data/int/in/FDA/HI_PM_FDA_TATRAN.dat",
    )

    perf = PerfTimer(JOB_NAME)
    src_rows = 0
    tgt_rows = 0
    error_rows = 0

    with perf:
        try:
            # Step 1: Set Pay Calendar
            pp_end_year, pp_num, cycle_id, map_cycle_id = step_set_pay_calendar(
                spark, jdbc_url, pp_end_year, pp_num,
            )

            # Step 2: Load FDA File
            src_rows = step_load_fda_file(spark, jdbc_url, fda_file_path)

            # Step 3: Error Counter / Validation
            counters = step_error_counter(
                spark, jdbc_url, pp_end_year, pp_num, cycle_id,
            )
            error_rows = counters.get("ERROR_REC_COUNT", 0)

            # Step 4: Extract Leave Records
            tgt_rows, subject, message = step_extract_leave(
                spark, jdbc_url, pp_end_year, pp_num, cycle_id,
            )

            # Email success notification
            recipients = [
                addr.strip()
                for addr in email_list.replace(";", " ").split()
                if addr.strip()
            ]
            send_email(
                subject=subject,
                body=message,
                recipients=recipients or None,
            )

            logger.info("Workflow %s completed successfully.", JOB_NAME)

        except Exception as exc:
            logger.error("Workflow %s FAILED: %s", JOB_NAME, exc)
            # Send failure email to $$WF_CPM_EMAIL_LIST
            failure_recipients = [
                addr.strip()
                for addr in email_list.replace(";", " ").split()
                if addr.strip()
            ]
            send_email(
                subject=f"{ENV_PREFIX}{JOB_NAME} FAILED",
                body=f"Workflow {JOB_NAME} failed with error:\n\n{exc}",
                recipients=failure_recipients or None,
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
    run_fda_leave()
