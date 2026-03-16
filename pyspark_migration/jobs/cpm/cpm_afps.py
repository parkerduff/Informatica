"""
CPM AFPS Payroll Processing

Processes CPM_NEWPAY_TBL data targeting HI_AFPS_FEEDER_TBL for the
external AFPS financial system.
Reference: XML/CPM_AFPS
"""

import argparse
import logging

from pyspark.sql import functions as F

from pyspark_migration.common.db_utils import read_oracle_table, write_oracle_table
from pyspark_migration.common.error_logging import log_counter
from pyspark_migration.common.notification import send_success_email
from pyspark_migration.common.pay_period import get_current_pay_period
from pyspark_migration.common.spark_session import get_spark_session

logger = logging.getLogger(__name__)


def run(environment=None):
    """
    Execute AFPS CPM processing.

    Parameters
    ----------
    environment : str, optional
    """
    logger.info("=" * 60)
    logger.info("Starting CPM AFPS Processing")
    logger.info("=" * 60)

    spark = get_spark_session("cpm_afps", environment)

    try:
        # Read CPM_NEWPAY_TBL
        newpay_df = read_oracle_table(spark, "CPM_NEWPAY_TBL")

        # Get current pay period
        pp_df = get_current_pay_period(spark)
        pp_row = pp_df.collect()[0]
        pp_num = int(pp_row["PP_NUM"])
        pp_end_year = int(pp_row["PP_END_YEAR"])

        # AFPS includes all agency codes - no agency filter
        afps_count = newpay_df.count()
        logger.info("AFPS feeder records: %d", afps_count)

        # Map to AFPS feeder schema
        afps_output = newpay_df.select(
            F.col("DFAS_PSEUDO_SSN"),
            F.col("EMPLOYEE_NAME"),
            F.col("PAY_PLAN"),
            F.col("GRADE"),
            F.col("STEP"),
            F.col("AGENCY_CODE"),
            F.col("DUTY_STATION"),
            F.col("FLSA_INDICATOR"),
            F.col("RETIREMENT_CODE"),
            F.col("YTD_GROSS_PAY"),
            F.col("YTD_FED_TAX_DED"),
            F.col("YTD_FICA_DED"),
            F.col("YTD_MEDC_DED"),
            F.col("YTD_STATE_TAX_DED"),
            F.col("YTD_LOCAL_TAX_DED"),
            F.col("YTD_HLTH_DED"),
            F.col("YTD_LI_REG_DED"),
            F.col("YTD_FERS_EMP_DED"),
            F.col("YTD_FERS_AGY_DED"),
            F.col("YTD_FERS_PAY_SUB"),
            F.col("YTD_BASE_PAY"),
            F.col("CPP_GROSS_PAY"),
            F.col("CPP_BASE_PAY"),
            F.col("CPP_FED_TAX_DED"),
            F.col("CPP_FICA_DED"),
            F.col("CPP_MEDC_DED"),
            F.col("PP_NUM"),
            F.col("PP_END_YEAR"),
        )

        # Write to HI_AFPS_FEEDER_TBL
        logger.info("Writing to HI_AFPS_FEEDER_TBL")
        write_oracle_table(afps_output, "HI_AFPS_FEEDER_TBL", mode="append")

        log_counter(
            spark, "cpm_afps",
            "AFPS feeder records", afps_count,
            pp_end_year, pp_num,
        )

        pp_padded = str(pp_num).zfill(2)
        send_success_email(
            "CPM AFPS Processing",
            f"AFPS feeder table loaded with {afps_count} records "
            f"for PP {pp_end_year}-{pp_padded}",
        )

        logger.info("CPM AFPS Processing completed successfully")

    except Exception:
        logger.exception("CPM AFPS Processing failed")
        from pyspark_migration.common.notification import send_failure_email
        send_failure_email("CPM AFPS Processing", "Job failed - check logs")
        raise
    finally:
        spark.stop()


def main():
    """CLI entry point for CPM AFPS processing."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="BIIS CPM AFPS Processing")
    parser.add_argument("--environment", type=str, help="Environment (dev/test/prod)")
    args = parser.parse_args()

    run(environment=args.environment)


if __name__ == "__main__":
    main()
