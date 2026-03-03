"""
CPM AFPS (Agency Financial Processing System) PySpark Pipeline.

Replaces: XML/CPM_AFPS (Informatica PowerCenter workflow wf_CPM_AFPS)

Processes AFPS-specific payroll feeder data from CPM staging tables
and generates agency-specific output files.

Source Tables:
  - CPM_NEWPAY_TBL, CPM_NEWPAY_STG_TYPE_1_2_TBL
  - CPM_NEWPAY_STG_TYPE_3_TBL, CPM_NEWPAY_STG_TYPE_3_FDR_TBL
  - CPM_NEWPAY_STG_DETAIL_TBL, CPM_NEWPAY_STG_YTD_STATE_TBL
  - PSEUDOSSN_TBL, PAY_PERIOD, HI_GENERIC_SRC_TBL

Target:
  - AFPS-specific flat files for SFTP transfer
  - COUNTER_TBL, ERROR_TBL
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

AFPS_AGENCY_CODE = "HHS-AFPS"


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


def extract_afps_records(
    spark: SparkSession, config: AppConfig, pay_period: dict,
) -> dict:
    """Extract AFPS-specific records from CPM staging tables.

    Includes type 3 feeder records which are specific to the
    AFPS feeder processing system.
    """
    logger.info("Extracting AFPS records from CPM staging tables")

    newpay_df = read_table(spark, config.db, table_name="CPM_NEWPAY_TBL")

    afps_df = newpay_df
    if "AGENCY_CODE" in newpay_df.columns:
        afps_df = newpay_df.filter(F.col("AGENCY_CODE") == AFPS_AGENCY_CODE)
    elif "PYF_AGENCY" in newpay_df.columns:
        afps_df = newpay_df.filter(F.col("PYF_AGENCY") == AFPS_AGENCY_CODE)

    type12_df = read_table(
        spark, config.db, table_name="CPM_NEWPAY_STG_TYPE_1_2_TBL",
    )
    type3_df = read_table(
        spark, config.db, table_name="CPM_NEWPAY_STG_TYPE_3_TBL",
    )
    type3_fdr_df = read_table(
        spark, config.db, table_name="CPM_NEWPAY_STG_TYPE_3_FDR_TBL",
    )
    detail_df = read_table(
        spark, config.db, table_name="CPM_NEWPAY_STG_DETAIL_TBL",
    )
    ytd_state_df = read_table(
        spark, config.db, table_name="CPM_NEWPAY_STG_YTD_STATE_TBL",
    )

    counts = {
        "newpay": afps_df.count(),
        "type12": type12_df.count(),
        "type3": type3_df.count(),
        "type3_fdr": type3_fdr_df.count(),
        "detail": detail_df.count(),
        "ytd_state": ytd_state_df.count(),
    }

    logger.info("AFPS record counts: %s", counts)
    return {
        "newpay": afps_df,
        "type12": type12_df,
        "type3": type3_df,
        "type3_fdr": type3_fdr_df,
        "detail": detail_df,
        "ytd_state": ytd_state_df,
        "counts": counts,
    }


def generate_afps_output_files(
    spark: SparkSession,
    config: AppConfig,
    afps_data: dict,
    pay_period: dict,
) -> list:
    """Generate AFPS-specific output flat files."""
    logger.info("Generating AFPS output files")

    pp_num_str = str(pay_period["PP_NUM"]).zfill(2)
    pp_end_year = pay_period["PP_END_YEAR"]
    output_dir = config.paths.cpm_output_dir
    os.makedirs(output_dir, exist_ok=True)

    output_files = []

    afps_newpay_df = afps_data["newpay"]
    if afps_newpay_df.count() > 0:
        afps_file = os.path.join(
            output_dir, f"AFPS_CPM_{pp_end_year}_{pp_num_str}.txt",
        )
        afps_newpay_df.toPandas().to_csv(
            afps_file, sep="|", index=False, header=True,
        )
        output_files.append(afps_file)
        logger.info("AFPS newpay file: %s", afps_file)

    # Generate type 3 feeder file (specific to AFPS)
    type3_fdr_df = afps_data["type3_fdr"]
    if type3_fdr_df.count() > 0:
        fdr_file = os.path.join(
            output_dir, f"AFPS_CPM_FDR_{pp_end_year}_{pp_num_str}.txt",
        )
        type3_fdr_df.toPandas().to_csv(
            fdr_file, sep="|", index=False, header=True,
        )
        output_files.append(fdr_file)
        logger.info("AFPS feeder file: %s", fdr_file)

    logger.info("Generated %d AFPS output files", len(output_files))
    return output_files


def build_counters_and_message(
    spark: SparkSession,
    config: AppConfig,
    pay_period: dict,
    afps_data: dict,
    output_files: list,
) -> tuple:
    """Build counters and notification message for AFPS processing."""
    pp_num = pay_period["PP_NUM"]
    pp_end_year = pay_period["PP_END_YEAR"]
    pp_num_str = str(pp_num).zfill(2)
    run_date = datetime.now()
    counts = afps_data["counts"]

    counter_data = [
        (run_date, "wf_CPM_AFPS", desc, float(count), pp_end_year, pp_num, None)
        for desc, count in [
            ("AFPS Newpay Records", counts.get("newpay", 0)),
            ("AFPS Type 1/2 Records", counts.get("type12", 0)),
            ("AFPS Type 3 Records", counts.get("type3", 0)),
            ("AFPS Type 3 Feeder Records", counts.get("type3_fdr", 0)),
            ("AFPS Detail Records", counts.get("detail", 0)),
            ("AFPS YTD State Records", counts.get("ytd_state", 0)),
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
        f"{env_prefix}CPM AFPS Files generated for "
        f"Pay Period: {pp_end_year}-{pp_num_str}"
    )
    lines = [
        f"CPM AFPS Processing Results for Pay Period {pp_end_year}-{pp_num_str}",
        "=" * 60,
        f"AFPS Newpay Records:      {counts.get('newpay', 0)}",
        f"AFPS Type 1/2 Records:    {counts.get('type12', 0)}",
        f"AFPS Type 3 Records:      {counts.get('type3', 0)}",
        f"AFPS Type 3 Fdr Records:  {counts.get('type3_fdr', 0)}",
        f"AFPS Detail Records:      {counts.get('detail', 0)}",
        f"AFPS YTD State Records:   {counts.get('ytd_state', 0)}",
        f"Output Files Generated:   {len(output_files)}",
    ]
    message = "\n".join(lines)
    return subject, message


def run(config=None):
    """Execute the full CPM AFPS workflow."""
    if config is None:
        config = AppConfig()

    log_file = setup_logging("cpm_afps", config.paths)
    spark = create_spark_session(config, "CPM_AFPS")

    try:
        pay_period = get_current_pay_period(spark, config)
        afps_data = extract_afps_records(spark, config, pay_period)
        output_files = generate_afps_output_files(
            spark, config, afps_data, pay_period,
        )
        subject, message = build_counters_and_message(
            spark, config, pay_period, afps_data, output_files,
        )

        send_success_notification(
            config.email, process_name=subject,
            message=message, log_file=log_file,
        )
        logger.info("CPM AFPS workflow completed successfully")

    except Exception as e:
        logger.exception("CPM AFPS workflow failed")
        send_failure_notification(
            config.email, process_name="CPM AFPS", error_message=str(e),
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
