"""
CPM CDC Payroll Processing

Processes CPM_NEWPAY_TBL data for CDC with two-part header/detail output
(WS_CDC_HDR + WS_PAY_OUT_REC).
Reference: XML/CPM_CDC
"""

import argparse
import logging
import os
from datetime import datetime

from pyspark.sql import functions as F

from pyspark_migration.common.db_utils import read_oracle_table
from pyspark_migration.common.error_logging import log_counter
from pyspark_migration.common.notification import send_success_email
from pyspark_migration.common.pay_period import get_current_pay_period
from pyspark_migration.common.spark_session import get_spark_session
from pyspark_migration.config.settings import FILE_PATHS

logger = logging.getLogger(__name__)


def run(environment=None):
    """
    Execute CDC CPM processing.

    Parameters
    ----------
    environment : str, optional
    """
    logger.info("=" * 60)
    logger.info("Starting CPM CDC Processing")
    logger.info("=" * 60)

    spark = get_spark_session("cpm_cdc", environment)

    try:
        # Read CPM_NEWPAY_TBL
        newpay_df = read_oracle_table(spark, "CPM_NEWPAY_TBL")

        # Get current pay period
        pp_df = get_current_pay_period(spark)
        pp_row = pp_df.collect()[0]
        pp_num = int(pp_row["PP_NUM"])
        pp_end_year = int(pp_row["PP_END_YEAR"])

        # Filter for CDC employees
        cdc_df = newpay_df.filter(
            F.col("AGENCY_CODE").isin(["HE20", "HE21", "HE22", "HE23"])
            | F.col("AGENCY_CODE").startswith("HE2")
        )
        cdc_count = cdc_df.count()
        logger.info("CDC employee records: %d", cdc_count)

        # Map to CDC output schema
        cdc_output = cdc_df.select(
            F.col("DFAS_PSEUDO_SSN").alias("PSEUDO_SSN"),
            F.col("EMPLOYEE_NAME").alias("NAME"),
            F.col("PAY_PLAN"),
            F.col("GRADE"),
            F.col("STEP"),
            F.col("AGENCY_CODE"),
            F.col("DUTY_STATION"),
            F.col("YTD_GROSS_PAY"),
            F.col("YTD_FED_TAX_DED"),
            F.col("YTD_FICA_DED"),
            F.col("YTD_MEDC_DED"),
            F.col("CPP_GROSS_PAY"),
            F.col("CPP_BASE_PAY"),
            F.col("PP_NUM"),
            F.col("PP_END_YEAR"),
        )

        # Generate header record (WS_CDC_HDR)
        output_dir = FILE_PATHS["cpm_output_dir"]
        pp_padded = str(pp_num).zfill(2)
        run_date = datetime.now().strftime("%Y%m%d")

        # Write header file
        hdr_filename = f"cdchdr_WS_CDC_HDR_{pp_end_year}_{pp_padded}.dat"
        hdr_path = os.path.join(output_dir, hdr_filename)
        with open(hdr_path, "w") as f:
            f.write(f"HDR|CDC_PAYROLL|{pp_end_year}{pp_padded}|{run_date}|{cdc_count}\n")

        # Write skeleton/detail file (WS_PAY_OUT_REC)
        skel_filename = f"cdcskel_WS_PAY_OUT_REC_{pp_end_year}_{pp_padded}.dat"
        skel_path = os.path.join(output_dir, skel_filename)

        (
            cdc_output
            .coalesce(1)
            .write.mode("overwrite")
            .option("header", "false")
            .option("delimiter", "|")
            .csv(skel_path + "_tmp")
        )

        log_counter(
            spark, "cpm_cdc",
            "CDC payroll records", cdc_count,
            pp_end_year, pp_num,
        )

        send_success_email(
            "CPM CDC Processing",
            f"CDC payroll files generated with {cdc_count} records "
            f"for PP {pp_end_year}-{pp_padded}",
        )

        logger.info("CPM CDC Processing completed successfully")

    except Exception:
        logger.exception("CPM CDC Processing failed")
        from pyspark_migration.common.notification import send_failure_email
        send_failure_email("CPM CDC Processing", "Job failed - check logs")
        raise
    finally:
        spark.stop()


def main():
    """CLI entry point for CPM CDC processing."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="BIIS CPM CDC Processing")
    parser.add_argument("--environment", type=str, help="Environment (dev/test/prod)")
    args = parser.parse_args()

    run(environment=args.environment)


if __name__ == "__main__":
    main()
