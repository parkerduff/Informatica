"""
CPM Staging

Reads VSAM YTD and MER flat files, parses COBOL fields, and consolidates
into CPM_NEWPAY_TBL.

Reference: XML/CPM (core staging layer for YTD and MER files)

Source files:
- PC_DOEYTD_RDF.TXT (Year-To-Date payroll VSAM file)
- PC_DOEMER_RDF.TXT (Master Employee Record VSAM file)

Target:
- CPM_NEWPAY_TBL with composite PK (PP_END_YEAR, PP_NUM, DFAS_PSEUDO_SSN, LINE_TYPE)
- CPM_YTD_DETAIL_STG_TBL
- CPM_MER_DETAIL_STG_TBL
"""

import argparse
import logging

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType

from pyspark_migration.common.cobol_parser import parse_signed_amount
from pyspark_migration.common.db_utils import read_oracle_table, write_oracle_table
from pyspark_migration.common.file_utils import parse_fixed_width
from pyspark_migration.common.pay_period import enrich_with_pay_period
from pyspark_migration.common.spark_session import get_spark_session
from pyspark_migration.common.validation import classify_record_type, is_valid_ssn
from pyspark_migration.config.settings import FILE_PATHS

logger = logging.getLogger(__name__)

# YTD VSAM field specs (representative subset - full specs from XML/CPM)
YTD_FIELD_SPECS = [
    ("RECORD_TYPE", 1, 2, "string"),
    ("DFAS_PSEUDO_SSN", 3, 9, "string"),
    ("LINE_TYPE", 12, 2, "string"),
    ("EMPLOYEE_NAME", 14, 30, "string"),
    ("PAY_PLAN", 44, 2, "string"),
    ("GRADE", 46, 2, "string"),
    ("STEP", 48, 2, "string"),
    ("AGENCY_CODE", 50, 4, "string"),
    ("DUTY_STATION", 54, 9, "string"),
    ("FLSA_INDICATOR", 63, 1, "string"),
    ("RETIREMENT_CODE", 64, 1, "string"),
    ("FEGLI_CDE", 65, 2, "string"),
    ("YTD_GROSS_PAY", 67, 11, "string"),
    ("YTD_FED_TAX_DED", 78, 11, "string"),
    ("YTD_FICA_DED", 89, 11, "string"),
    ("YTD_FICA_PAY", 100, 11, "string"),
    ("YTD_FICA_EMPLR", 111, 11, "string"),
    ("YTD_MEDC_DED", 122, 11, "string"),
    ("YTD_MEDC_PAY", 133, 11, "string"),
    ("YTD_MEDC_EMPLR", 144, 11, "string"),
    ("YTD_STATE_TAX_DED", 155, 11, "string"),
    ("YTD_LOCAL_TAX_DED", 166, 11, "string"),
    ("YTD_HLTH_DED", 177, 11, "string"),
    ("YTD_LI_REG_DED", 188, 11, "string"),
    ("YTD_FERS_EMP_DED", 199, 11, "string"),
    ("YTD_FERS_AGY_DED", 210, 11, "string"),
    ("YTD_FERS_PAY_SUB", 221, 11, "string"),
    ("YTD_CSRS_DED", 232, 11, "string"),
    ("YTD_CSRS_PAY_SUB", 243, 11, "string"),
    ("YTD_TSPA_PAY_SUB", 254, 11, "string"),
    ("YTD_BASE_PAY", 265, 11, "string"),
    ("CPP_GROSS_PAY", 276, 11, "string"),
    ("CPP_BASE_PAY", 287, 11, "string"),
    ("CPP_FED_TAX_DED", 298, 11, "string"),
    ("CPP_FICA_DED", 309, 11, "string"),
    ("CPP_MEDC_DED", 320, 11, "string"),
    ("CPP_STATE_TAX_DED", 331, 11, "string"),
    ("CPP_LOCAL_TAX_DED", 342, 11, "string"),
    ("CPP_HLTH_DED", 353, 11, "string"),
    ("CPP_FERS_EMP_DED", 364, 11, "string"),
    ("CPP_FERS_PAY_SUB", 375, 11, "string"),
    ("PP_END_DATE", 386, 8, "string"),
]

# MER VSAM field specs (representative subset - full specs from XML/CPM)
MER_FIELD_SPECS = [
    ("RECORD_TYPE", 1, 2, "string"),
    ("DFAS_PSEUDO_SSN", 3, 9, "string"),
    ("LINE_TYPE", 12, 2, "string"),
    ("EMPLOYEE_NAME", 14, 30, "string"),
    ("BIRTH_DATE", 44, 8, "string"),
    ("SEX_CODE", 52, 1, "string"),
    ("MARITAL_STATUS", 53, 1, "string"),
    ("SERVICE_COMP_DATE", 54, 8, "string"),
    ("TSP_ELECTION_PCT", 62, 5, "string"),
    ("TSP_ELECTION_AMT", 67, 9, "string"),
    ("FEHB_PLAN_CODE", 76, 6, "string"),
    ("FEHB_ENROLLMENT_CODE", 82, 3, "string"),
    ("LTC_INDICATOR", 85, 1, "string"),
    ("FSA_HC_INDICATOR", 86, 1, "string"),
    ("FSA_DC_INDICATOR", 87, 1, "string"),
]

# Signed numeric fields in YTD data (require COBOL parsing)
YTD_SIGNED_FIELDS = [
    "YTD_GROSS_PAY", "YTD_FED_TAX_DED", "YTD_FICA_DED", "YTD_FICA_PAY",
    "YTD_FICA_EMPLR", "YTD_MEDC_DED", "YTD_MEDC_PAY", "YTD_MEDC_EMPLR",
    "YTD_STATE_TAX_DED", "YTD_LOCAL_TAX_DED", "YTD_HLTH_DED",
    "YTD_LI_REG_DED", "YTD_FERS_EMP_DED", "YTD_FERS_AGY_DED",
    "YTD_FERS_PAY_SUB", "YTD_CSRS_DED", "YTD_CSRS_PAY_SUB",
    "YTD_TSPA_PAY_SUB", "YTD_BASE_PAY",
    "CPP_GROSS_PAY", "CPP_BASE_PAY", "CPP_FED_TAX_DED",
    "CPP_FICA_DED", "CPP_MEDC_DED", "CPP_STATE_TAX_DED",
    "CPP_LOCAL_TAX_DED", "CPP_HLTH_DED",
    "CPP_FERS_EMP_DED", "CPP_FERS_PAY_SUB",
]


def load_ytd_data(spark, ytd_file):
    """
    Read and parse the YTD VSAM flat file.

    Parameters
    ----------
    spark : SparkSession
    ytd_file : str

    Returns
    -------
    DataFrame
    """
    logger.info("Loading YTD data from %s", ytd_file)

    raw_df = parse_fixed_width(spark, ytd_file, YTD_FIELD_SPECS)

    # Classify records and filter details
    classified_df = raw_df.withColumn(
        "RECORD_TYPE_FLAG",
        classify_record_type(F.col("DFAS_PSEUDO_SSN")),
    )
    detail_df = classified_df.filter(F.col("RECORD_TYPE_FLAG") == "D")

    # Parse all signed numeric fields
    for field in YTD_SIGNED_FIELDS:
        if field in detail_df.columns:
            detail_df = detail_df.withColumn(
                field, parse_signed_amount(F.col(field), 2)
            )

    count = detail_df.count()
    logger.info("YTD detail records: %d", count)
    return detail_df


def load_mer_data(spark, mer_file):
    """
    Read and parse the MER VSAM flat file.

    Parameters
    ----------
    spark : SparkSession
    mer_file : str

    Returns
    -------
    DataFrame
    """
    logger.info("Loading MER data from %s", mer_file)

    raw_df = parse_fixed_width(spark, mer_file, MER_FIELD_SPECS)

    # Classify and filter
    classified_df = raw_df.withColumn(
        "RECORD_TYPE_FLAG",
        classify_record_type(F.col("DFAS_PSEUDO_SSN")),
    )
    detail_df = classified_df.filter(F.col("RECORD_TYPE_FLAG") == "D")

    # Parse signed numeric fields in MER
    signed_fields = ["TSP_ELECTION_PCT", "TSP_ELECTION_AMT"]
    for field in signed_fields:
        if field in detail_df.columns:
            detail_df = detail_df.withColumn(
                field, parse_signed_amount(F.col(field), 2)
            )

    count = detail_df.count()
    logger.info("MER detail records: %d", count)
    return detail_df


def consolidate_to_newpay(ytd_df, mer_df, spark):
    """
    Consolidate YTD and MER data into CPM_NEWPAY_TBL.

    Join on DFAS_PSEUDO_SSN and enrich with pay period.
    Composite PK: (PP_END_YEAR, PP_NUM, DFAS_PSEUDO_SSN, LINE_TYPE)

    Reference: XML/CPM_NIH lines 7-10

    Parameters
    ----------
    ytd_df : DataFrame
    mer_df : DataFrame
    spark : SparkSession

    Returns
    -------
    DataFrame
    """
    logger.info("Consolidating YTD and MER into CPM_NEWPAY_TBL")

    # Join YTD and MER on DFAS_PSEUDO_SSN
    consolidated_df = ytd_df.alias("ytd").join(
        mer_df.alias("mer"),
        on=F.col("ytd.DFAS_PSEUDO_SSN") == F.col("mer.DFAS_PSEUDO_SSN"),
        how="left",
    )

    # Keep YTD side DFAS_PSEUDO_SSN and LINE_TYPE
    result = consolidated_df.select(
        F.col("ytd.DFAS_PSEUDO_SSN").alias("DFAS_PSEUDO_SSN"),
        F.col("ytd.LINE_TYPE").alias("LINE_TYPE"),
        F.col("ytd.*"),
        F.col("mer.BIRTH_DATE"),
        F.col("mer.SEX_CODE"),
        F.col("mer.MARITAL_STATUS"),
        F.col("mer.SERVICE_COMP_DATE"),
        F.col("mer.TSP_ELECTION_PCT"),
        F.col("mer.TSP_ELECTION_AMT"),
        F.col("mer.FEHB_PLAN_CODE"),
        F.col("mer.FEHB_ENROLLMENT_CODE"),
    ).dropDuplicates(["DFAS_PSEUDO_SSN", "LINE_TYPE"])

    # Enrich with pay period
    enriched_df = enrich_with_pay_period(result, spark)

    # Join with PSEUDOSSN_TBL for SSN de-identification
    pseudossn_df = read_oracle_table(spark, "PSEUDOSSN_TBL")
    final_df = enriched_df.join(
        pseudossn_df.select(
            F.col("PSEUDOSSN").alias("PSEUDO_SSN"),
            F.col("SSN").alias("REAL_SSN"),
        ),
        on=enriched_df["DFAS_PSEUDO_SSN"] == pseudossn_df["PSEUDOSSN"],
        how="left",
    )

    return final_df


def run(ytd_file=None, mer_file=None, environment=None):
    """
    Execute the CPM staging workflow.

    Parameters
    ----------
    ytd_file : str, optional
    mer_file : str, optional
    environment : str, optional
    """
    logger.info("=" * 60)
    logger.info("Starting CPM Staging")
    logger.info("=" * 60)

    if ytd_file is None:
        ytd_file = FILE_PATHS["cpm_ytd_input"]
    if mer_file is None:
        mer_file = FILE_PATHS["cpm_mer_input"]

    spark = get_spark_session("cpm_staging", environment)

    try:
        ytd_df = load_ytd_data(spark, ytd_file)
        mer_df = load_mer_data(spark, mer_file)

        # Write staging tables
        logger.info("Writing CPM_YTD_DETAIL_STG_TBL")
        write_oracle_table(ytd_df, "CPM_YTD_DETAIL_STG_TBL", mode="overwrite")

        logger.info("Writing CPM_MER_DETAIL_STG_TBL")
        write_oracle_table(mer_df, "CPM_MER_DETAIL_STG_TBL", mode="overwrite")

        # Consolidate to NEWPAY
        newpay_df = consolidate_to_newpay(ytd_df, mer_df, spark)

        logger.info("Writing CPM_NEWPAY_TBL")
        write_oracle_table(newpay_df, "CPM_NEWPAY_TBL", mode="overwrite")

        logger.info("CPM Staging completed successfully")

    except Exception:
        logger.exception("CPM Staging failed")
        from pyspark_migration.common.notification import send_failure_email
        send_failure_email("CPM Staging", "Job failed - check logs")
        raise
    finally:
        spark.stop()


def main():
    """CLI entry point for CPM staging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="BIIS CPM Staging")
    parser.add_argument("--ytd-file", type=str, help="Path to YTD VSAM file")
    parser.add_argument("--mer-file", type=str, help="Path to MER VSAM file")
    parser.add_argument("--environment", type=str, help="Environment (dev/test/prod)")
    args = parser.parse_args()

    run(ytd_file=args.ytd_file, mer_file=args.mer_file, environment=args.environment)


if __name__ == "__main__":
    main()
