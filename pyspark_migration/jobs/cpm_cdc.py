"""
PySpark job: CPM CDC — Build Message

Replaces Informatica PowerCenter mapping for CDC agency processing
(source file: XML/CPM_CDC).

Same pattern as CPM NIH/OIG but:
  - Filters for CDC-specific records
  - Additionally writes output to a flat file at
    /data/informatica/infa_shared/TgtFiles/CPM.CDC.HDR.TXT
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

# CDC-specific filter value
CDC_BUSINESS_UNIT = "CDC00"

# Output flat file path (replaces PowerCenter flat-file target)
CDC_HDR_OUTPUT_PATH = "/data/informatica/infa_shared/TgtFiles/CPM.CDC.HDR.TXT"


def _get_environment() -> str:
    """Derive the environment prefix (Dev/Test/Prod) from env var."""
    return os.environ.get("BIIS_ENVIRONMENT", "Prod")


def _read_and_filter_cpm(
    spark: SparkSession,
    pp_end_year: int,
    pp_num: int,
):
    """Read CPM_NEWPAY_TBL filtered for CDC, return DataFrame and count."""
    url = get_src_jdbc_url()
    props = get_src_jdbc_properties()

    df = (
        spark.read.jdbc(url, "CPM_NEWPAY_TBL", properties=props)
        .filter(
            (col("PP_END_YEAR") == pp_end_year)
            & (col("PP_NUM") == pp_num)
            & (col("MP_POOL_DES") == " ")
            & (col("BUSINESS_UNIT") == CDC_BUSINESS_UNIT)
        )
    )

    agg_df = df.groupBy("PP_END_YEAR", "PP_NUM").agg(
        count("*").alias("CPM_CDC_TOTAL"),
        lit(1).alias("o_CONSTANT"),
    )

    rows = agg_df.collect()
    total = rows[0]["CPM_CDC_TOTAL"] if rows else 0

    return df, total


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


def _write_cdc_flat_file(df, pp_end_year: int, pp_num: int) -> None:
    """Write CDC header data to the flat file target.

    Replaces the PowerCenter flat-file target that produces
    CPM.CDC.HDR.TXT.
    """
    output_path = os.environ.get("CDC_HDR_OUTPUT_PATH", CDC_HDR_OUTPUT_PATH)
    output_dir = os.path.dirname(output_path)

    logger.info("Writing CDC header flat file to %s", output_path)

    # Write as single CSV file (coalesce to 1 partition for single file)
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(output_dir + "/cdc_hdr_temp")
    )

    # Rename the part file to the expected output name
    # In production, consider using a Hadoop-compatible file move instead
    import glob
    temp_files = glob.glob(output_dir + "/cdc_hdr_temp/part-*.csv")
    if temp_files:
        os.rename(temp_files[0], output_path)
        # Clean up temp directory
        import shutil
        shutil.rmtree(output_dir + "/cdc_hdr_temp", ignore_errors=True)

    logger.info("CDC header flat file written successfully")


def run(pp_end_year: int, pp_num: int) -> None:
    """Execute the CPM CDC Build Message job."""
    spark = (
        SparkSession.builder
        .appName("CPM_CDC_Build_Message")
        .getOrCreate()
    )

    try:
        logger.info("Starting CPM CDC Build Message job")

        # 1. Read, filter, aggregate
        cdc_df, cpm_cdc_total = _read_and_filter_cpm(spark, pp_end_year, pp_num)
        logger.info("CPM CDC record count: %d", cpm_cdc_total)

        # 2. Write CDC flat file output
        _write_cdc_flat_file(cdc_df, pp_end_year, pp_num)

        # 3. Lookup pay period header total
        hdr_total = _get_pay_period_total(spark, pp_end_year, pp_num)
        total_records = cpm_cdc_total + hdr_total

        # 4. Build message
        environment = _get_environment()
        pp_num_str = str(pp_num).zfill(2)

        subject = (
            f"{environment}CPM CDC Process Completed Successfully "
            f"for: {pp_end_year}-{pp_num_str}"
        )
        message = (
            f"Total number of CPM CDC records loaded is: {total_records}"
        )

        logger.info("Subject: %s", subject)
        logger.info("Message: %s", message)

        # 5. Send email
        send_email(subject=subject, body=message)

        logger.info("CPM CDC Build Message job completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="CPM CDC Build Message PySpark job")
    parser.add_argument("--pp-end-year", type=int, required=True, help="Pay period end year")
    parser.add_argument("--pp-num", type=int, required=True, help="Pay period number")
    args = parser.parse_args()

    run(pp_end_year=args.pp_end_year, pp_num=args.pp_num)
