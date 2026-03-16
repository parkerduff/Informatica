"""
FDA Leave Validation Job

Implements m_0150_PM_FDA_Error_Counter mapping.

Validates FDA TATRAN records against CPM staging tables to ensure
leave records have corresponding payroll data.

Steps:
1. Read HI_PM_FDA_TATRAN_TBL filtered by FDA_REC_TYPE = '02'
2. Left join with 4 CPM staging tables
3. Log unmatched records to ERROR_TBL
4. Write validation counts to COUNTER_TBL
"""

import argparse
import logging

from pyspark.sql import functions as F

from pyspark_migration.common.db_utils import read_oracle_table
from pyspark_migration.common.error_logging import log_counter, log_errors
from pyspark_migration.common.pay_period import get_current_pay_period
from pyspark_migration.common.spark_session import get_spark_session

logger = logging.getLogger(__name__)

# Validation check definitions
VALIDATION_CHECKS = [
    {
        "table": "CPM_YTD_DETAIL_STG_TBL",
        "error_message": "DOES NOT HAVE A YTD RECORD (P6791X01)",
        "join_key": "DFAS_PSEUDO_SSN",
    },
    {
        "table": "CPM_PAD_DETAIL_STG_TBL",
        "error_message": "DOES NOT HAVE A PAD RECORD (P6331X01)",
        "join_key": "DFAS_PSEUDO_SSN",
    },
    {
        "table": "CPM_MER_DETAIL_STG_TBL",
        "error_message": "DOES NOT HAVE A MER RECORD (P6722D01)",
        "join_key": "DFAS_PSEUDO_SSN",
    },
    {
        "table": "CPM_NEWPAY_TBL",
        "error_message": "DOES NOT HAVE A NEWPAY RECORD (CPM_NEWPAY_TBL)",
        "join_key": "DFAS_PSEUDO_SSN",
    },
]


def validate_against_table(fda_df, spark, check_config):
    """
    Left join FDA records against a CPM staging table and find unmatched.

    Parameters
    ----------
    fda_df : DataFrame
        FDA TATRAN records.
    spark : SparkSession
    check_config : dict
        Contains 'table', 'error_message', 'join_key'.

    Returns
    -------
    tuple of (int, DataFrame)
        (match_count, unmatched_df with ERROR_MESSAGE and SOURCE_KEY columns)
    """
    table_name = check_config["table"]
    join_key = check_config["join_key"]
    error_msg = check_config["error_message"]

    logger.info("Validating against %s", table_name)

    staging_df = read_oracle_table(spark, table_name)

    # Left join
    joined_df = fda_df.alias("fda").join(
        staging_df.alias("stg"),
        on=F.col(f"fda.{join_key}") == F.col(f"stg.{join_key}"),
        how="left",
    )

    # Find unmatched (where staging side is null)
    unmatched_df = joined_df.filter(
        F.col(f"stg.{join_key}").isNull()
    ).select(
        F.col(f"fda.{join_key}").alias("SOURCE_KEY"),
        F.lit(error_msg).alias("ERROR_MESSAGE"),
    )

    unmatched_count = unmatched_df.count()
    matched_count = fda_df.count() - unmatched_count

    logger.info(
        "%s: matched=%d, unmatched=%d",
        table_name, matched_count, unmatched_count,
    )

    return unmatched_count, unmatched_df


def run(environment=None):
    """
    Execute the FDA Leave Validation workflow.

    Parameters
    ----------
    environment : str, optional
    """
    logger.info("=" * 60)
    logger.info("Starting FDA Leave Validation")
    logger.info("=" * 60)

    spark = get_spark_session("fda_validation", environment)

    try:
        # Get current pay period
        pp_df = get_current_pay_period(spark)
        pp_row = pp_df.collect()[0]
        pp_num = int(pp_row["PP_NUM"])
        pp_end_year = int(pp_row["PP_END_YEAR"])

        # Step 1: Read FDA TATRAN records with type '02'
        logger.info("Reading HI_PM_FDA_TATRAN_TBL (FDA_REC_TYPE='02')")
        fda_df = read_oracle_table(spark, "HI_PM_FDA_TATRAN_TBL")
        fda_filtered = fda_df.filter(F.col("FDA_REC_TYPE") == "02")
        fda_count = fda_filtered.count()
        logger.info("FDA TATRAN records (type 02): %d", fda_count)

        if fda_count == 0:
            logger.info("No FDA records to validate. Exiting.")
            return

        # Step 2-3: Validate against each CPM staging table
        total_errors = 0
        for check_config in VALIDATION_CHECKS:
            unmatched_count, unmatched_df = validate_against_table(
                fda_filtered, spark, check_config
            )

            # Log unmatched to ERROR_TBL
            if unmatched_count > 0:
                log_errors(
                    spark, unmatched_df,
                    "m_0150_PM_FDA_Error_Counter",
                    pp_end_year, pp_num,
                )
                total_errors += unmatched_count

            # Step 4: Log validation count to COUNTER_TBL
            log_counter(
                spark,
                "m_0150_PM_FDA_Error_Counter",
                f"FDA records without match in {check_config['table']}",
                unmatched_count,
                pp_end_year, pp_num,
            )

        # Overall counter
        log_counter(
            spark,
            "m_0150_PM_FDA_Error_Counter",
            "Total FDA TATRAN records validated",
            fda_count,
            pp_end_year, pp_num,
        )
        log_counter(
            spark,
            "m_0150_PM_FDA_Error_Counter",
            "Total FDA validation errors",
            total_errors,
            pp_end_year, pp_num,
        )

        logger.info(
            "FDA Validation complete: %d records validated, %d errors found",
            fda_count, total_errors,
        )

    except Exception:
        logger.exception("FDA Leave Validation failed")
        from pyspark_migration.common.notification import send_failure_email
        send_failure_email("FDA Leave Validation", "Job failed - check logs")
        raise
    finally:
        spark.stop()


def main():
    """CLI entry point for FDA Leave Validation."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="BIIS FDA Leave Validation")
    parser.add_argument("--environment", type=str, help="Environment (dev/test/prod)")
    args = parser.parse_args()

    run(environment=args.environment)


if __name__ == "__main__":
    main()
