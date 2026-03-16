"""
CPM OIG Payroll Processing

Processes CPM_NEWPAY_TBL data for OIG-specific payroll with SK-prefixed
surrogate key fields targeting SKPAYROLL_MASTER.
Reference: XML/CPM_OIG
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
    Execute OIG CPM processing.

    Parameters
    ----------
    environment : str, optional
    """
    logger.info("=" * 60)
    logger.info("Starting CPM OIG Processing")
    logger.info("=" * 60)

    spark = get_spark_session("cpm_oig", environment)

    try:
        # Read CPM_NEWPAY_TBL
        newpay_df = read_oracle_table(spark, "CPM_NEWPAY_TBL")

        # Get current pay period
        pp_df = get_current_pay_period(spark)
        pp_row = pp_df.collect()[0]
        pp_num = int(pp_row["PP_NUM"])
        pp_end_year = int(pp_row["PP_END_YEAR"])

        # Filter for OIG employees
        oig_df = newpay_df.filter(
            F.col("AGENCY_CODE").isin(["HE70", "HE71"])
            | F.col("AGENCY_CODE").startswith("HE7")
        )
        oig_count = oig_df.count()
        logger.info("OIG employee records: %d", oig_count)

        # Map to OIG-specific schema with SK-prefixed surrogate keys
        oig_output = oig_df.select(
            F.concat(F.lit("SK"), F.col("DFAS_PSEUDO_SSN")).alias("SK_PSEUDO_SSN"),
            F.col("EMPLOYEE_NAME").alias("SK_NAME"),
            F.col("PAY_PLAN"),
            F.col("GRADE"),
            F.col("STEP"),
            F.col("AGENCY_CODE"),
            F.col("DUTY_STATION"),
            F.col("RETIREMENT_CODE"),
            F.col("YTD_GROSS_PAY"),
            F.col("YTD_FED_TAX_DED"),
            F.col("YTD_FICA_DED"),
            F.col("YTD_MEDC_DED"),
            F.col("YTD_STATE_TAX_DED"),
            F.col("YTD_HLTH_DED"),
            F.col("YTD_FERS_EMP_DED"),
            F.col("CPP_GROSS_PAY"),
            F.col("CPP_BASE_PAY"),
            F.col("PP_NUM"),
            F.col("PP_END_YEAR"),
        )

        # Write output file
        output_dir = FILE_PATHS["cpm_output_dir"]
        pp_padded = str(pp_num).zfill(2)
        output_filename = f"oigsgndec_SKPAYROLL_MASTER_{pp_end_year}_{pp_padded}.dat"
        output_path = os.path.join(output_dir, output_filename)

        (
            oig_output
            .coalesce(1)
            .write.mode("overwrite")
            .option("header", "false")
            .option("delimiter", "|")
            .csv(output_path + "_tmp")
        )

        log_counter(
            spark, "cpm_oig",
            "OIG payroll master records", oig_count,
            pp_end_year, pp_num,
        )

        send_success_email(
            "CPM OIG Processing",
            f"OIG payroll master file generated with {oig_count} records "
            f"for PP {pp_end_year}-{pp_padded}",
        )

        logger.info("CPM OIG Processing completed successfully")

    except Exception:
        logger.exception("CPM OIG Processing failed")
        from pyspark_migration.common.notification import send_failure_email
        send_failure_email("CPM OIG Processing", "Job failed - check logs")
        raise
    finally:
        spark.stop()


def main():
    """CLI entry point for CPM OIG processing."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="BIIS CPM OIG Processing")
    parser.add_argument("--environment", type=str, help="Environment (dev/test/prod)")
    args = parser.parse_args()

    run(environment=args.environment)


if __name__ == "__main__":
    main()
