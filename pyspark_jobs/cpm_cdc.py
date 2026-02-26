"""
PySpark migration of Informatica workflow: CPM_CDC
Source XML: XML/CPM_CDC

CPM Agency Extract for CDC. Structurally identical to CPM_NIH but with
CDC-specific agency filters and output paths.

Reads from CPM_NEWPAY_TBL (keyed on PP_END_YEAR, PP_NUM, DFAS_PSEUDO_SSN,
LINE_TYPE) with CDC-specific agency filters, processes VSAM source files,
and writes agency-specific output files.

Errors go to ERROR_TBL, counters go to COUNTER_TBL.

Original schedule: ONDEMAND on Test_IS / Dom_dev
"""

import os
from datetime import datetime
from typing import Dict, Tuple

from pyspark.sql import DataFrame, SparkSession

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
)

# Import VSAM reader from NIH module (shared logic)
from cpm_nih import read_vsam_file

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
JOB_NAME = "wf_CPM_CDC"
PROCESS_NAME = "CPM_CDC"
AGENCY_FILTER = "CDC"
TARGET_FILE_DIR = os.environ.get("PM_TARGET_FILE_DIR", "/data/BIISINT/data/int/out")
PARAM_FILE = os.environ.get(
    "BIIS_PARAM_FILE", "/data/BIISINT/control/BIIS_parms.iparms"
)

# Output file paths (CDC-specific)
OUTPUT_FILE_NEWPAY = os.path.join(TARGET_FILE_DIR, "CPM", "CPM.CDC.TEST.DAT.TXT")
OUTPUT_FILE_YTD = os.path.join(TARGET_FILE_DIR, "CPM", "CPM.CDC.YTD.DAT.TXT")
OUTPUT_FILE_MER = os.path.join(TARGET_FILE_DIR, "CPM", "CPM.CDC.MER.DAT.TXT")
OUTPUT_FILE_PAD = os.path.join(TARGET_FILE_DIR, "CPM", "CPM.CDC.PAD.DAT.TXT")


# ===================================================================
# Step 1: Set Pay Calendar
# ===================================================================
def step_set_pay_calendar(
    spark: SparkSession,
    jdbc_url: str,
    pp_end_year: int,
    pp_num: int,
) -> Tuple[int, int, int]:
    """Read CPM_CYCLE_TBL for CDC to set pay period context.

    Returns (pp_end_year, pp_num, cycle_id).
    """
    logger.info("Step 1: Set Pay Calendar for %s", PROCESS_NAME)

    cycle_df = read_oracle_table(
        spark, "CPM_CYCLE_TBL", jdbc_url=jdbc_url,
        predicate=f"PROCESS_NAME = '{PROCESS_NAME}'",
    )

    cycle_row = cycle_df.first()
    if cycle_row is None:
        logger.warning("No CPM_CYCLE_TBL entry for %s, using params", PROCESS_NAME)
        return (pp_end_year, pp_num, 0)

    cycle_id = int(cycle_row["CYCLE_ID"]) if cycle_row["CYCLE_ID"] is not None else 0
    target_pp_end_year = pp_end_year if pp_end_year > 0 else int(cycle_row["PP_END_YEAR"] or 0)
    target_pp_num = pp_num if pp_num > 0 else int(cycle_row["PP_NUM"] or 0)

    # Update cycle table
    update_sql = (
        f"UPDATE CPM_CYCLE_TBL SET "
        f"PP_END_YEAR = {target_pp_end_year}, "
        f"PP_NUM = {target_pp_num} "
        f"WHERE PROCESS_NAME = '{PROCESS_NAME}'"
    )
    execute_jdbc_statement(update_sql, jdbc_url=jdbc_url)

    return (target_pp_end_year, target_pp_num, cycle_id)


# ===================================================================
# Step 2: Read CPM_NEWPAY_TBL with CDC filter
# ===================================================================
def step_read_newpay(
    spark: SparkSession,
    jdbc_url: str,
    pp_end_year: int,
    pp_num: int,
) -> DataFrame:
    """Read CPM_NEWPAY_TBL filtered for CDC agency records."""
    logger.info("Step 2: Read CPM_NEWPAY_TBL for %s", AGENCY_FILTER)

    newpay_df = read_oracle_table(
        spark, "CPM_NEWPAY_TBL", jdbc_url=jdbc_url,
        predicate=(
            f"PP_END_YEAR = {pp_end_year} AND PP_NUM = {pp_num} "
            f"AND AGENCY_CD = '{AGENCY_FILTER}'"
        ),
    )

    count = newpay_df.count()
    logger.info("Read %d record(s) from CPM_NEWPAY_TBL for %s", count, AGENCY_FILTER)
    return newpay_df


# ===================================================================
# Step 3: Process VSAM source files
# ===================================================================
def step_process_vsam_files(
    spark: SparkSession,
    jdbc_url: str,
    pp_end_year: int,
    pp_num: int,
    cycle_id: int,
    source_dir: str,
) -> Dict[str, int]:
    """Process VSAM source files (YTD and MER) for CDC."""
    logger.info("Step 3: Process VSAM source files for %s", AGENCY_FILTER)
    counters: Dict[str, int] = {}

    ytd_path = os.path.join(source_dir, "PC_DOEYTD_RDF.TXT")
    ytd_df = read_vsam_file(spark, ytd_path)
    counters["YTD_RECORDS"] = ytd_df.count()

    mer_path = os.path.join(source_dir, "PC_DOEMER_RDF.TXT")
    mer_df = read_vsam_file(spark, mer_path)
    counters["MER_RECORDS"] = mer_df.count()

    return counters


# ===================================================================
# Step 4: Validate and write output files
# ===================================================================
def step_validate_and_output(
    spark: SparkSession,
    jdbc_url: str,
    newpay_df: DataFrame,
    pp_end_year: int,
    pp_num: int,
    cycle_id: int,
) -> Tuple[int, int, int]:
    """Validate records and write CDC output files.

    Returns (output_count, error_count, total_count).
    """
    logger.info("Step 4: Validate and write output for %s", AGENCY_FILTER)

    total_count = newpay_df.count()
    error_count = 0

    # Validate against CPM_YTD_DETAIL_STG_TBL
    try:
        ytd_stg_df = read_oracle_table(
            spark, "CPM_YTD_DETAIL_STG_TBL", jdbc_url=jdbc_url,
        ).select("DFAS_PSEUDO_SSN").distinct()

        errors_ytd = newpay_df.select("DFAS_PSEUDO_SSN").distinct().join(
            ytd_stg_df, on="DFAS_PSEUDO_SSN", how="left_anti",
        )
        ytd_error_count = write_error_df(
            errors_ytd,
            process_name=PROCESS_NAME,
            pp_end_year=pp_end_year,
            pp_num=pp_num,
            cycle_id=cycle_id,
            source_key_col="DFAS_PSEUDO_SSN",
            error_message=f"{AGENCY_FILTER} employee not in CPM_YTD_DETAIL_STG_TBL",
            jdbc_url=jdbc_url,
        )
        error_count += ytd_error_count
    except Exception as exc:
        logger.warning("YTD validation skipped: %s", exc)

    # Validate against CPM_PAD_DETAIL_STG_TBL
    try:
        pad_stg_df = read_oracle_table(
            spark, "CPM_PAD_DETAIL_STG_TBL", jdbc_url=jdbc_url,
        ).select("DFAS_PSEUDO_SSN").distinct()

        errors_pad = newpay_df.select("DFAS_PSEUDO_SSN").distinct().join(
            pad_stg_df, on="DFAS_PSEUDO_SSN", how="left_anti",
        )
        pad_error_count = write_error_df(
            errors_pad,
            process_name=PROCESS_NAME,
            pp_end_year=pp_end_year,
            pp_num=pp_num,
            cycle_id=cycle_id,
            source_key_col="DFAS_PSEUDO_SSN",
            error_message=f"{AGENCY_FILTER} employee not in CPM_PAD_DETAIL_STG_TBL",
            jdbc_url=jdbc_url,
        )
        error_count += pad_error_count
    except Exception as exc:
        logger.warning("PAD validation skipped: %s", exc)

    # Validate against CPM_MER_STG_TBL
    try:
        mer_stg_df = read_oracle_table(
            spark, "CPM_MER_STG_TBL", jdbc_url=jdbc_url,
        ).select("DFAS_PSEUDO_SSN").distinct()

        errors_mer = newpay_df.select("DFAS_PSEUDO_SSN").distinct().join(
            mer_stg_df, on="DFAS_PSEUDO_SSN", how="left_anti",
        )
        mer_error_count = write_error_df(
            errors_mer,
            process_name=PROCESS_NAME,
            pp_end_year=pp_end_year,
            pp_num=pp_num,
            cycle_id=cycle_id,
            source_key_col="DFAS_PSEUDO_SSN",
            error_message=f"{AGENCY_FILTER} employee not in CPM_MER_STG_TBL",
            jdbc_url=jdbc_url,
        )
        error_count += mer_error_count
    except Exception as exc:
        logger.warning("MER validation skipped: %s", exc)

    # Write output file
    os.makedirs(os.path.dirname(OUTPUT_FILE_NEWPAY) or ".", exist_ok=True)
    output_pdf = newpay_df.toPandas()
    output_pdf.to_csv(OUTPUT_FILE_NEWPAY, index=False, sep="|")
    output_count = len(output_pdf)
    logger.info("Wrote %d record(s) to %s", output_count, OUTPUT_FILE_NEWPAY)

    return (output_count, error_count, total_count)


# ===================================================================
# Step 5: Write counters and send email
# ===================================================================
def step_finalize(
    spark: SparkSession,
    jdbc_url: str,
    pp_end_year: int,
    pp_num: int,
    cycle_id: int,
    output_count: int,
    error_count: int,
    total_count: int,
    email_list: str,
) -> None:
    """Write counter records and send completion email."""
    logger.info("Step 5: Finalize - write counters and send email")

    run_date = datetime.now()

    counters = {
        "Total records read from CPM_NEWPAY_TBL": total_count,
        "Records written to output file": output_count,
        "Error records": error_count,
    }
    for desc, value in counters.items():
        write_counter(
            spark,
            process_name=PROCESS_NAME,
            counter_description=desc,
            counter_value=value,
            pp_end_year=pp_end_year,
            pp_num=pp_num,
            cycle_id=cycle_id,
            run_date=run_date,
            jdbc_url=jdbc_url,
        )

    pp_num_str = format_pp_num(pp_num)
    subject = (
        f"{ENV_PREFIX}{PROCESS_NAME} Extract completed successfully "
        f"for Pay Period: {pp_end_year}-{pp_num_str}"
    )
    message = (
        f"CPM {AGENCY_FILTER} extract completed.\n"
        f"Total records: {total_count}\n"
        f"Output records: {output_count}\n"
        f"Error records: {error_count}\n"
        f"Pay Period: {pp_end_year}-{pp_num_str}\n"
        f"Output file: {OUTPUT_FILE_NEWPAY}"
    )

    recipients = [
        addr.strip()
        for addr in email_list.replace(";", " ").split()
        if addr.strip()
    ]
    send_email(subject=subject, body=message, recipients=recipients or None)


# ===================================================================
# Main workflow
# ===================================================================
def run_cpm_cdc() -> None:
    """Execute the full CPM_CDC workflow."""
    spark = get_spark_session(JOB_NAME)
    jdbc_url = get_jdbc_url()

    params = parse_parameter_file(PARAM_FILE)
    pp_end_year_param = get_param(params, "$$WF_PP_END_YEAR", "WF_PP_END_YEAR")
    pp_num_param = get_param(params, "$$WF_PP_NUM", "WF_PP_NUM")
    email_list = get_param(
        params,
        "$$WF_CPM_EMAIL_LIST",
        "WF_CPM_EMAIL_LIST",
        default="peter.chen@hhs.gov nathan.knight@hhs.gov marvin.simon@hhs.gov",
    )
    source_dir = get_param(
        params, "$Param_Root_Directory", "PARAM_ROOT_DIRECTORY",
        default="/data/BIISINT",
    )
    source_dir = os.path.join(source_dir, "data", "int", "in", "CPM")

    pp_end_year = int(pp_end_year_param) if pp_end_year_param else 0
    pp_num = int(pp_num_param) if pp_num_param else 0

    perf = PerfTimer(JOB_NAME)
    src_rows = 0
    tgt_rows = 0
    error_rows = 0

    with perf:
        try:
            pp_end_year, pp_num, cycle_id = step_set_pay_calendar(
                spark, jdbc_url, pp_end_year, pp_num,
            )

            newpay_df = step_read_newpay(spark, jdbc_url, pp_end_year, pp_num)
            newpay_df.cache()
            src_rows = newpay_df.count()

            step_process_vsam_files(
                spark, jdbc_url, pp_end_year, pp_num, cycle_id, source_dir,
            )

            tgt_rows, error_rows, total = step_validate_and_output(
                spark, jdbc_url, newpay_df, pp_end_year, pp_num, cycle_id,
            )

            newpay_df.unpersist()

            step_finalize(
                spark, jdbc_url, pp_end_year, pp_num, cycle_id,
                tgt_rows, error_rows, total, email_list,
            )

            logger.info("Workflow %s completed successfully.", JOB_NAME)

        except Exception as exc:
            logger.error("Workflow %s FAILED: %s", JOB_NAME, exc)
            send_email(
                subject=f"{ENV_PREFIX}{JOB_NAME} FAILED",
                body=f"Workflow {JOB_NAME} failed with error:\n\n{exc}",
            )
            raise

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
    run_cpm_cdc()
