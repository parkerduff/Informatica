"""
CPM OIG (Office of Inspector General) PySpark Pipeline.

Replaces: XML/CPM_OIG (Informatica PowerCenter workflow wf_CPM_OIG)

Processes OIG-specific payroll data from CPM staging tables and
generates agency-specific output files for OIG.

Source Tables:
  - CPM_NEWPAY_TBL, CPM_NEWPAY_STG_TYPE_1_2_TBL
  - CPM_NEWPAY_STG_TYPE_3_TBL, CPM_NEWPAY_STG_TYPE_3_FDR_TBL
  - CPM_NEWPAY_STG_DETAIL_TBL, CPM_NEWPAY_STG_YTD_STATE_TBL
  - PSEUDOSSN_TBL, PAY_PERIOD, HI_GENERIC_SRC_TBL

Target:
  - OIG-specific flat files for SFTP transfer
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

OIG_AGENCY_CODE = "HHS-OIG"


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


def extract_oig_records(
    spark: SparkSession, config: AppConfig, pay_period: dict,
) -> dict:
    """Extract OIG-specific records from CPM staging tables.

    Filters records by OIG agency code and prepares them for
    OIG-specific output file generation.
    """
    logger.info("Extracting OIG records from CPM staging tables")

    newpay_df = read_table(spark, config.db, table_name="CPM_NEWPAY_TBL")

    # Filter for OIG agency records
    oig_df = newpay_df
    if "AGENCY_CODE" in newpay_df.columns:
        oig_df = newpay_df.filter(F.col("AGENCY_CODE") == OIG_AGENCY_CODE)
    elif "PYF_AGENCY" in newpay_df.columns:
        oig_df = newpay_df.filter(F.col("PYF_AGENCY") == OIG_AGENCY_CODE)

    type12_df = read_table(
        spark, config.db, table_name="CPM_NEWPAY_STG_TYPE_1_2_TBL",
    )
    type3_df = read_table(
        spark, config.db, table_name="CPM_NEWPAY_STG_TYPE_3_TBL",
    )
    detail_df = read_table(
        spark, config.db, table_name="CPM_NEWPAY_STG_DETAIL_TBL",
    )

    counts = {
        "newpay": oig_df.count(),
        "type12": type12_df.count(),
        "type3": type3_df.count(),
        "detail": detail_df.count(),
    }

    logger.info("OIG record counts: %s", counts)
    return {
        "newpay": oig_df,
        "type12": type12_df,
        "type3": type3_df,
        "detail": detail_df,
        "counts": counts,
    }


def generate_oig_output_files(
    spark: SparkSession,
    config: AppConfig,
    oig_data: dict,
    pay_period: dict,
) -> list:
    """Generate OIG-specific output flat files."""
    logger.info("Generating OIG output files")

    pp_num_str = str(pay_period["PP_NUM"]).zfill(2)
    pp_end_year = pay_period["PP_END_YEAR"]
    output_dir = config.paths.cpm_output_dir
    os.makedirs(output_dir, exist_ok=True)

    output_files = []

    oig_newpay_df = oig_data["newpay"]
    if oig_newpay_df.count() > 0:
        oig_file = os.path.join(
            output_dir, f"OIG_CPM_{pp_end_year}_{pp_num_str}.txt",
        )
        oig_newpay_df.toPandas().to_csv(
            oig_file, sep="|", index=False, header=True,
        )
        output_files.append(oig_file)
        logger.info("OIG newpay file: %s", oig_file)

    logger.info("Generated %d OIG output files", len(output_files))
    return output_files


def build_counters_and_message(
    spark: SparkSession,
    config: AppConfig,
    pay_period: dict,
    oig_data: dict,
    output_files: list,
) -> tuple:
    """Build counters and notification message for OIG processing."""
    pp_num = pay_period["PP_NUM"]
    pp_end_year = pay_period["PP_END_YEAR"]
    pp_num_str = str(pp_num).zfill(2)
    run_date = datetime.now()
    counts = oig_data["counts"]

    counter_data = [
        (run_date, "wf_CPM_OIG", desc, float(count), pp_end_year, pp_num, None)
        for desc, count in [
            ("OIG Newpay Records", counts.get("newpay", 0)),
            ("OIG Type 1/2 Records", counts.get("type12", 0)),
            ("OIG Type 3 Records", counts.get("type3", 0)),
            ("OIG Detail Records", counts.get("detail", 0)),
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
        f"{env_prefix}CPM OIG Files generated for "
        f"Pay Period: {pp_end_year}-{pp_num_str}"
    )
    lines = [
        f"CPM OIG Processing Results for Pay Period {pp_end_year}-{pp_num_str}",
        "=" * 60,
        f"OIG Newpay Records:    {counts.get('newpay', 0)}",
        f"OIG Type 1/2 Records:  {counts.get('type12', 0)}",
        f"OIG Type 3 Records:    {counts.get('type3', 0)}",
        f"OIG Detail Records:    {counts.get('detail', 0)}",
        f"Output Files Generated: {len(output_files)}",
    ]
    message = "\n".join(lines)
    return subject, message


def run(config=None):
    """Execute the full CPM OIG workflow."""
    if config is None:
        config = AppConfig()

    log_file = setup_logging("cpm_oig", config.paths)
    spark = create_spark_session(config, "CPM_OIG")

    try:
        pay_period = get_current_pay_period(spark, config)
        oig_data = extract_oig_records(spark, config, pay_period)
        output_files = generate_oig_output_files(
            spark, config, oig_data, pay_period,
        )
        subject, message = build_counters_and_message(
            spark, config, pay_period, oig_data, output_files,
        )

        send_success_notification(
            config.email, process_name=subject,
            message=message, log_file=log_file,
        )
        logger.info("CPM OIG workflow completed successfully")

    except Exception as e:
        logger.exception("CPM OIG workflow failed")
        send_failure_notification(
            config.email, process_name="CPM OIG", error_message=str(e),
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
