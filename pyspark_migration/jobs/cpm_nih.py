"""
PySpark job: CPM NIH — Build Message

Replaces Informatica PowerCenter mapping ``m_CPM_NIH_Build_Message``
(source file: XML/CPM_NIH).

Processing flow:
  1. Read CPM_NEWPAY_TBL and filter for NIH-specific records
     (BUSINESS_UNIT = 'NIH00', matching PP_END_YEAR / PP_NUM, blank MP_POOL_DES).
  2. Aggregate to get a total record count (agg_Count_CPM_NIH).
  3. Lookup PAY_PERIOD to get the header total count (lkp_Pay_Period_Total).
  4. Build a completion message with environment prefix and totals.
  5. Send email notification.
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit

from pyspark_migration.utils.db import (
    get_src_jdbc_url,
    get_src_jdbc_properties,
    get_tgt_jdbc_url,
    get_tgt_jdbc_properties,
)
from pyspark_migration.utils.email import send_email

logger = logging.getLogger(__name__)


def _get_environment() -> str:
    """Derive the environment prefix (Dev/Test/Prod) from env var.

    Replaces the PowerCenter expression that derives v_ENVIRONMENT from
    the repository service name prefix.
    """
    return os.environ.get("BIIS_ENVIRONMENT", "Prod")


def _read_and_filter_cpm(
    spark: SparkSession,
    pp_end_year: int,
    pp_num: int,
) -> int:
    """Read CPM_NEWPAY_TBL filtered for NIH and return the record count.

    Replaces the Source Qualifier + Filter + Aggregator pipeline.
    """
    url = get_src_jdbc_url()
    props = get_src_jdbc_properties()

    df = (
        spark.read.jdbc(url, "CPM_NEWPAY_TBL", properties=props)
        .filter(
            (col("PP_END_YEAR") == pp_end_year)
            & (col("PP_NUM") == pp_num)
            & (col("MP_POOL_DES") == " ")
            & (col("BUSINESS_UNIT") == "NIH00")
        )
    )

    # agg_Count_CPM_NIH — count matching records
    agg_df = df.groupBy("PP_END_YEAR", "PP_NUM").agg(
        count("*").alias("CPM_NIH_TOTAL"),
        lit(1).alias("o_CONSTANT"),
    )

    rows = agg_df.collect()
    if rows:
        return rows[0]["CPM_NIH_TOTAL"]
    return 0


def _get_pay_period_total(spark: SparkSession, pp_end_year: int, pp_num: int) -> int:
    """Lookup PAY_PERIOD to get the header total count.

    Replaces ``lkp_Pay_Period_Total``:
        SELECT COUNT(*) as COUNT_TOTAL FROM PAY_PERIOD
        WHERE PP_END_YEAR = TO_NUMBER('$$MAP_PP_END_YEAR')
          AND PP_NUM = TO_NUMBER('$$MAP_PP_NUM')
    """
    url = get_tgt_jdbc_url()
    props = get_tgt_jdbc_properties()

    pp_count = (
        spark.read.jdbc(url, "PAY_PERIOD", properties=props)
        .filter(
            (col("PP_END_YEAR") == pp_end_year) & (col("PP_NUM") == pp_num)
        )
        .count()
    )

    return pp_count


def run(pp_end_year: int, pp_num: int) -> None:
    """Execute the CPM NIH Build Message job."""
    spark = (
        SparkSession.builder
        .appName("CPM_NIH_Build_Message")
        .getOrCreate()
    )

    try:
        logger.info("Starting CPM NIH Build Message job")

        # 1. Read, filter, aggregate
        cpm_nih_total = _read_and_filter_cpm(spark, pp_end_year, pp_num)
        logger.info("CPM NIH record count: %d", cpm_nih_total)

        # 2. Lookup pay period header total
        hdr_total = _get_pay_period_total(spark, pp_end_year, pp_num)
        total_records = cpm_nih_total + hdr_total

        # 3. Build message
        environment = _get_environment()
        pp_num_str = str(pp_num).zfill(2)

        subject = (
            f"{environment}CPM NIH Process Completed Successfully "
            f"for: {pp_end_year}-{pp_num_str}"
        )
        message = (
            f"Total number of CPM NIH records loaded is: {total_records}"
        )

        logger.info("Subject: %s", subject)
        logger.info("Message: %s", message)

        # 4. Send email
        send_email(subject=subject, body=message)

        logger.info("CPM NIH Build Message job completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="CPM NIH Build Message PySpark job")
    parser.add_argument("--pp-end-year", type=int, required=True, help="Pay period end year")
    parser.add_argument("--pp-num", type=int, required=True, help="Pay period number")
    args = parser.parse_args()

    run(pp_end_year=args.pp_end_year, pp_num=args.pp_num)
