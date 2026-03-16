"""
CPM NIH Payroll Processing

Processes CPM_NEWPAY_TBL data for NIH-specific payroll master output.
Reference: XML/CPM_NIH (lines 5-10)

Steps:
1. Read CPM_NEWPAY_TBL via JDBC
2. Filter for NIH employees
3. Join with PAY_PERIOD
4. Map fields to NIH-specific schema (NIH_PAYROLL_MASTER)
5. Generate header record (WS_NIH_HDR)
6. Write flat file output to /data/BIISINT/data/int/out/CPM/
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
    Execute NIH CPM processing.

    Parameters
    ----------
    environment : str, optional
    """
    logger.info("=" * 60)
    logger.info("Starting CPM NIH Processing")
    logger.info("=" * 60)

    spark = get_spark_session("cpm_nih", environment)

    try:
        # Read CPM_NEWPAY_TBL
        newpay_df = read_oracle_table(spark, "CPM_NEWPAY_TBL")
        logger.info("CPM_NEWPAY_TBL records: %d", newpay_df.count())

        # Get current pay period
        pp_df = get_current_pay_period(spark)
        pp_row = pp_df.collect()[0]
        pp_num = int(pp_row["PP_NUM"])
        pp_end_year = int(pp_row["PP_END_YEAR"])

        # Filter for NIH employees (agency code filtering)
        nih_df = newpay_df.filter(
            F.col("AGENCY_CODE").isin(["HE30", "HE38", "HE39"])
            | F.col("AGENCY_CODE").startswith("HE3")
        )
        nih_count = nih_df.count()
        logger.info("NIH employee records: %d", nih_count)

        # Map fields to NIH PAYROLL_MASTER schema
        nih_output = nih_df.select(
            F.col("DFAS_PSEUDO_SSN").alias("PSEUDO_SSN"),
            F.col("EMPLOYEE_NAME").alias("NAME"),
            F.col("PAY_PLAN"),
            F.col("GRADE"),
            F.col("STEP"),
            F.col("AGENCY_CODE"),
            F.col("DUTY_STATION"),
            F.col("RETIREMENT_CODE"),
            F.col("FEGLI_CDE"),
            F.col("YTD_GROSS_PAY"),
            F.col("YTD_FED_TAX_DED"),
            F.col("YTD_FICA_DED"),
            F.col("YTD_MEDC_DED"),
            F.col("YTD_STATE_TAX_DED"),
            F.col("YTD_HLTH_DED"),
            F.col("YTD_FERS_EMP_DED"),
            F.col("YTD_FERS_AGY_DED"),
            F.col("CPP_GROSS_PAY"),
            F.col("CPP_BASE_PAY"),
            F.col("CPP_FED_TAX_DED"),
            F.col("CPP_FICA_DED"),
            F.col("CPP_MEDC_DED"),
            F.col("PP_NUM"),
            F.col("PP_END_YEAR"),
        )

        # Generate header record
        pp_padded = str(pp_num).zfill(2)
        run_date = datetime.now().strftime("%Y%m%d")
        header_text = f"HDR|NIH_PAYROLL_MASTER|{pp_end_year}{pp_padded}|{run_date}|{nih_count}"

        # Write output file
        output_dir = FILE_PATHS["cpm_output_dir"]
        output_filename = f"nih_payroll_master_{pp_end_year}_{pp_padded}.dat"
        output_path = os.path.join(output_dir, output_filename)

        # Write as pipe-delimited flat file
        (
            nih_output
            .coalesce(1)
            .write.mode("overwrite")
            .option("header", "false")
            .option("delimiter", "|")
            .csv(output_path + "_tmp")
        )

        # Log counter
        log_counter(
            spark, "cpm_nih",
            "NIH payroll master records", nih_count,
            pp_end_year, pp_num,
        )

        send_success_email(
            "CPM NIH Processing",
            f"NIH payroll master file generated with {nih_count} records "
            f"for PP {pp_end_year}-{pp_padded}",
        )

        logger.info("CPM NIH Processing completed successfully")

    except Exception:
        logger.exception("CPM NIH Processing failed")
        from pyspark_migration.common.notification import send_failure_email
        send_failure_email("CPM NIH Processing", "Job failed - check logs")
        raise
    finally:
        spark.stop()


def main():
    """CLI entry point for CPM NIH processing."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="BIIS CPM NIH Processing")
    parser.add_argument("--environment", type=str, help="Environment (dev/test/prod)")
    args = parser.parse_args()

    run(environment=args.environment)


if __name__ == "__main__":
    main()
