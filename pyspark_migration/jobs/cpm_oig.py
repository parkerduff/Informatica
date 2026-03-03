"""
PySpark job: CPM OIG — Build Message

Replaces Informatica PowerCenter mapping for OIG agency processing
(source file: XML/CPM_OIG).

Same pattern as CPM NIH but filters for OIG-specific records.
The BUSINESS_UNIT filter value changes per agency.
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

# OIG-specific filter value
OIG_BUSINESS_UNIT = "OIG00"


def _get_environment() -> str:
    """Derive the environment prefix (Dev/Test/Prod) from env var."""
    return os.environ.get("BIIS_ENVIRONMENT", "Prod")


def _read_and_filter_cpm(
    spark: SparkSession,
    pp_end_year: int,
    pp_num: int,
) -> int:
    """Read CPM_NEWPAY_TBL filtered for OIG and return the record count."""
    url = get_src_jdbc_url()
    props = get_src_jdbc_properties()

    df = (
        spark.read.jdbc(url, "CPM_NEWPAY_TBL", properties=props)
        .filter(
            (col("PP_END_YEAR") == pp_end_year)
            & (col("PP_NUM") == pp_num)
            & (col("MP_POOL_DES") == " ")
            & (col("BUSINESS_UNIT") == OIG_BUSINESS_UNIT)
        )
    )

    agg_df = df.groupBy("PP_END_YEAR", "PP_NUM").agg(
        count("*").alias("CPM_OIG_TOTAL"),
        lit(1).alias("o_CONSTANT"),
    )

    rows = agg_df.collect()
    if rows:
        return rows[0]["CPM_OIG_TOTAL"]
    return 0


def _get_pay_period_total(spark: SparkSession, pp_end_year: int, pp_num: int) -> int:
    """Lookup PAY_PERIOD to get the header total count."""
    url = get_tgt_jdbc_url()
    props = get_tgt_jdbc_properties()

    return (
        spark.read.jdbc(url, "PAY_PERIOD", properties=props)
        .filter(
            (col("PP_END_YEAR") == pp_end_year) & (col("PP_NUM") == pp_num)
        )
        .count()
    )


def run(pp_end_year: int, pp_num: int) -> None:
    """Execute the CPM OIG Build Message job."""
    spark = (
        SparkSession.builder
        .appName("CPM_OIG_Build_Message")
        .getOrCreate()
    )

    try:
        logger.info("Starting CPM OIG Build Message job")

        # 1. Read, filter, aggregate
        cpm_oig_total = _read_and_filter_cpm(spark, pp_end_year, pp_num)
        logger.info("CPM OIG record count: %d", cpm_oig_total)

        # 2. Lookup pay period header total
        hdr_total = _get_pay_period_total(spark, pp_end_year, pp_num)
        total_records = cpm_oig_total + hdr_total

        # 3. Build message
        environment = _get_environment()
        pp_num_str = str(pp_num).zfill(2)

        subject = (
            f"{environment}CPM OIG Process Completed Successfully "
            f"for: {pp_end_year}-{pp_num_str}"
        )
        message = (
            f"Total number of CPM OIG records loaded is: {total_records}"
        )

        logger.info("Subject: %s", subject)
        logger.info("Message: %s", message)

        # 4. Send email
        send_email(subject=subject, body=message)

        logger.info("CPM OIG Build Message job completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="CPM OIG Build Message PySpark job")
    parser.add_argument("--pp-end-year", type=int, required=True, help="Pay period end year")
    parser.add_argument("--pp-num", type=int, required=True, help="Pay period number")
    args = parser.parse_args()

    run(pp_end_year=args.pp_end_year, pp_num=args.pp_num)
