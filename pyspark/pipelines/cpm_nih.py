"""
CPM NIH (National Institutes of Health) PySpark Pipeline.

Replaces: XML/CPM_NIH (Informatica PowerCenter workflow wf_CPM_NIH)

This pipeline processes NIH-specific payroll data from the CPM staging
tables and generates agency-specific output files for NIH.

Source Tables:
  - CPM_NEWPAY_TBL (from core CPM pipeline)
  - CPM_NEWPAY_STG_TYPE_1_2_TBL
  - CPM_NEWPAY_STG_TYPE_3_TBL
  - CPM_NEWPAY_STG_TYPE_3_FDR_TBL
  - CPM_NEWPAY_STG_DETAIL_TBL
  - CPM_NEWPAY_STG_YTD_STATE_TBL
  - PSEUDOSSN_TBL
  - PAY_PERIOD
  - HI_GENERIC_SRC_TBL

Target:
  - NIH-specific flat files for transfer to NIH systems
  - COUNTER_TBL (record counts)
  - ERROR_TBL (validation errors)
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

# NIH agency code used to filter records
NIH_AGENCY_CODE = "75"


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


def extract_nih_records(
    spark: SparkSession, config: AppConfig, pay_period: dict,
) -> dict:
    """Extract NIH-specific records from CPM staging tables.

    Filters records by NIH agency code and prepares them for
    NIH-specific output file generation.

    Returns:
        Dictionary with DataFrames for each record type.
    """
    logger.info("Extracting NIH records from CPM staging tables")

    # Read CPM newpay records filtered by NIH agency
    newpay_df = read_table(spark, config.db, table_name="CPM_NEWPAY_TBL")

    # Filter for NIH agency records
    nih_df = newpay_df
    if "AGENCY_CODE" in newpay_df.columns:
        nih_df = newpay_df.filter(F.col("AGENCY_CODE") == NIH_AGENCY_CODE)
    elif "PYF_AGENCY" in newpay_df.columns:
        nih_df = newpay_df.filter(F.col("PYF_AGENCY") == NIH_AGENCY_CODE)

    # Read type 1/2 staging records for NIH
    type12_df = read_table(
        spark, config.db, table_name="CPM_NEWPAY_STG_TYPE_1_2_TBL",
    )

    # Read type 3 staging records
    type3_df = read_table(
        spark, config.db, table_name="CPM_NEWPAY_STG_TYPE_3_TBL",
    )

    # Read type 3 feeder records
    type3_fdr_df = read_table(
        spark, config.db, table_name="CPM_NEWPAY_STG_TYPE_3_FDR_TBL",
    )

    # Read detail records
    detail_df = read_table(
        spark, config.db, table_name="CPM_NEWPAY_STG_DETAIL_TBL",
    )

    # Read YTD state records
    ytd_state_df = read_table(
        spark, config.db, table_name="CPM_NEWPAY_STG_YTD_STATE_TBL",
    )

    counts = {
        "newpay": nih_df.count(),
        "type12": type12_df.count(),
        "type3": type3_df.count(),
        "type3_fdr": type3_fdr_df.count(),
        "detail": detail_df.count(),
        "ytd_state": ytd_state_df.count(),
    }

    logger.info("NIH record counts: %s", counts)
    return {
        "newpay": nih_df,
        "type12": type12_df,
        "type3": type3_df,
        "type3_fdr": type3_fdr_df,
        "detail": detail_df,
        "ytd_state": ytd_state_df,
        "counts": counts,
    }


def generate_nih_output_files(
    spark: SparkSession,
    config: AppConfig,
    nih_data: dict,
    pay_period: dict,
) -> list:
    """Generate NIH-specific output flat files.

    Creates agency-formatted files in the CPM output directory
    for SFTP transfer to NIH systems.

    Returns:
        List of output file paths generated.
    """
    logger.info("Generating NIH output files")

    pp_num_str = str(pay_period["PP_NUM"]).zfill(2)
    pp_end_year = pay_period["PP_END_YEAR"]
    output_dir = config.paths.cpm_output_dir
    os.makedirs(output_dir, exist_ok=True)

    output_files = []

    # Generate NIH newpay file
    nih_newpay_df = nih_data["newpay"]
    if nih_newpay_df.count() > 0:
        nih_file = os.path.join(
            output_dir,
            f"NIH_CPM_{pp_end_year}_{pp_num_str}.txt",
        )
        nih_newpay_df.toPandas().to_csv(
            nih_file, sep="|", index=False, header=True,
        )
        output_files.append(nih_file)
        logger.info("NIH newpay file: %s", nih_file)

    logger.info("Generated %d NIH output files", len(output_files))
    return output_files


def build_counters_and_message(
    spark: SparkSession,
    config: AppConfig,
    pay_period: dict,
    nih_data: dict,
    output_files: list,
) -> tuple:
    """Build counters and notification message for NIH processing."""
    pp_num = pay_period["PP_NUM"]
    pp_end_year = pay_period["PP_END_YEAR"]
    pp_num_str = str(pp_num).zfill(2)
    run_date = datetime.now()
    counts = nih_data["counts"]

    counter_data = [
        (run_date, "wf_CPM_NIH", desc, float(count), pp_end_year, pp_num, None)
        for desc, count in [
            ("NIH Newpay Records", counts.get("newpay", 0)),
            ("NIH Type 1/2 Records", counts.get("type12", 0)),
            ("NIH Type 3 Records", counts.get("type3", 0)),
            ("NIH Detail Records", counts.get("detail", 0)),
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
        f"{env_prefix}CPM NIH Files generated for "
        f"Pay Period: {pp_end_year}-{pp_num_str}"
    )

    lines = [
        f"CPM NIH Processing Results for Pay Period {pp_end_year}-{pp_num_str}",
        "=" * 60,
        f"NIH Newpay Records:    {counts.get('newpay', 0)}",
        f"NIH Type 1/2 Records:  {counts.get('type12', 0)}",
        f"NIH Type 3 Records:    {counts.get('type3', 0)}",
        f"NIH Detail Records:    {counts.get('detail', 0)}",
        f"Output Files Generated: {len(output_files)}",
    ]
    for f_path in output_files:
        lines.append(f"  - {os.path.basename(f_path)}")

    message = "\n".join(lines)
    return subject, message


def run(config=None):
    """Execute the full CPM NIH workflow."""
    if config is None:
        config = AppConfig()

    log_file = setup_logging("cpm_nih", config.paths)
    spark = create_spark_session(config, "CPM_NIH")

    try:
        pay_period = get_current_pay_period(spark, config)
        nih_data = extract_nih_records(spark, config, pay_period)
        output_files = generate_nih_output_files(
            spark, config, nih_data, pay_period,
        )
        subject, message = build_counters_and_message(
            spark, config, pay_period, nih_data, output_files,
        )

        send_success_notification(
            config.email, process_name=subject,
            message=message, log_file=log_file,
        )
        logger.info("CPM NIH workflow completed successfully")

    except Exception as e:
        logger.exception("CPM NIH workflow failed")
        send_failure_notification(
            config.email, process_name="CPM NIH", error_message=str(e),
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
