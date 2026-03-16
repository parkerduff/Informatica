"""
LES (Leave and Earnings Statement) Processing

Implements 8 record type parsers, one for each LES file type.
Reads EMP_REC_TYPE_*.TXT fixed-width files and loads them into
corresponding target tables.

Record Types:
- 0-9: Header and primary data -> LES_HEADER_TBL, LES_PRIMARY_DATA_TBL
- C: Earnings (3 per record, needs unpivot) -> LES_EMP_DETAIL_RECTYPE_C_TBL
- D: Deductions (2 per record) -> LES_EMP_DETAIL_RECTYPE_D_TBL
- L: Leave data -> LES_EMP_DETAIL_RECTYPE_L_TBL
- M: Remarks (90-char) -> LES_EMP_DETAIL_RECTYPE_M_TBL
- R: Retirement -> LES_EMP_DETAIL_RECTYPE_R_TBL
- T: Tax data -> LES_EMP_DETAIL_RECTYPE_T_TBL
- U: Union data -> LES_EMP_DETAIL_RECTYPE_U_TBL
"""

import argparse
import logging
import os

from pyspark.sql import functions as F

from pyspark_migration.common.cobol_parser import parse_signed_amount
from pyspark_migration.common.db_utils import write_oracle_table
from pyspark_migration.common.error_logging import log_counter, log_errors
from pyspark_migration.common.file_utils import parse_fixed_width
from pyspark_migration.common.pay_period import enrich_with_pay_period
from pyspark_migration.common.spark_session import get_spark_session
from pyspark_migration.common.validation import (
    classify_record_type,
    validate_and_convert_date,
)
from pyspark_migration.config.settings import FILE_PATHS

logger = logging.getLogger(__name__)

# Common fields present in all LES record types (first 50 positions)
COMMON_PREFIX_SPECS = [
    ("DFAS_PSEUDO_SSN", 1, 9, "string"),
    ("REC_TYPE", 10, 1, "string"),
    ("PP_END_DATE", 11, 8, "string"),
    ("AGENCY_CODE", 19, 4, "string"),
]

# Record Type 0-9: Header/Primary fields after common prefix
HEADER_FIELD_SPECS = COMMON_PREFIX_SPECS + [
    ("EMPLOYEE_NAME", 23, 30, "string"),
    ("PAY_PLAN", 53, 2, "string"),
    ("GRADE", 55, 2, "string"),
    ("STEP", 57, 2, "string"),
    ("DUTY_STATION", 59, 9, "string"),
    ("PAY_BASIS", 68, 2, "string"),
    ("BASIC_PAY", 70, 11, "string"),
    ("GROSS_PAY", 81, 11, "string"),
    ("NET_PAY", 92, 11, "string"),
    ("FED_TAX", 103, 11, "string"),
    ("STATE_TAX", 114, 11, "string"),
    ("FICA_TAX", 125, 11, "string"),
    ("MEDICARE_TAX", 136, 11, "string"),
    ("RETIREMENT", 147, 11, "string"),
    ("FEGLI", 158, 11, "string"),
    ("FEHB", 169, 11, "string"),
    ("TSP_TAX_DEF", 180, 11, "string"),
    ("TSP_ROTH", 191, 11, "string"),
    ("ALLOTMENTS", 202, 11, "string"),
    ("ADJUSTMENTS", 213, 11, "string"),
]

# Record Type C: Earnings (3 earnings per record)
RECTYPE_C_SPECS = COMMON_PREFIX_SPECS + [
    ("EARN1_TYPE", 23, 6, "string"),
    ("EARN1_HOURS", 29, 7, "string"),
    ("EARN1_AMOUNT", 36, 11, "string"),
    ("EARN2_TYPE", 47, 6, "string"),
    ("EARN2_HOURS", 53, 7, "string"),
    ("EARN2_AMOUNT", 60, 11, "string"),
    ("EARN3_TYPE", 71, 6, "string"),
    ("EARN3_HOURS", 77, 7, "string"),
    ("EARN3_AMOUNT", 84, 11, "string"),
]

# Record Type D: Deductions (2 deductions per record)
RECTYPE_D_SPECS = COMMON_PREFIX_SPECS + [
    ("DED1_TYPE", 23, 6, "string"),
    ("DED1_CURRENT", 29, 11, "string"),
    ("DED1_YTD", 40, 11, "string"),
    ("DED2_TYPE", 51, 6, "string"),
    ("DED2_CURRENT", 57, 11, "string"),
    ("DED2_YTD", 68, 11, "string"),
]

# Record Type L: Leave data
RECTYPE_L_SPECS = COMMON_PREFIX_SPECS + [
    ("LEAVE_TYPE", 23, 6, "string"),
    ("LEAVE_BEG_BAL", 29, 7, "string"),
    ("LEAVE_EARNED", 36, 7, "string"),
    ("LEAVE_USED", 43, 7, "string"),
    ("LEAVE_ADJ", 50, 7, "string"),
    ("LEAVE_END_BAL", 57, 7, "string"),
    ("LEAVE_USE_OR_LOSE", 64, 7, "string"),
    ("LEAVE_PROJ_YEAR_END", 71, 7, "string"),
]

# Record Type M: Remarks (90-char block)
RECTYPE_M_SPECS = COMMON_PREFIX_SPECS + [
    ("REMARK_LINE", 23, 90, "string"),
]

# Record Type R: Retirement
RECTYPE_R_SPECS = COMMON_PREFIX_SPECS + [
    ("RETIREMENT_TYPE", 23, 6, "string"),
    ("RET_CURRENT", 29, 11, "string"),
    ("RET_YTD", 40, 11, "string"),
    ("RET_GOV_MATCH", 51, 11, "string"),
    ("RET_GOV_MATCH_YTD", 62, 11, "string"),
]

# Record Type T: Tax data
RECTYPE_T_SPECS = COMMON_PREFIX_SPECS + [
    ("TAX_TYPE", 23, 6, "string"),
    ("TAX_CURRENT", 29, 11, "string"),
    ("TAX_YTD", 40, 11, "string"),
    ("TAX_EXEMPT", 51, 3, "string"),
    ("TAX_ADD_WITHHOLD", 54, 11, "string"),
    ("TAX_STATUS", 65, 2, "string"),
    ("TAX_STATE_CODE", 67, 2, "string"),
]

# Record Type U: Union data
RECTYPE_U_SPECS = COMMON_PREFIX_SPECS + [
    ("UNION_CODE", 23, 6, "string"),
    ("UNION_CURRENT", 29, 11, "string"),
    ("UNION_YTD", 40, 11, "string"),
]

# Map record type to specs and target table
RECORD_TYPE_CONFIG = {
    "header": {
        "file_pattern": "EMP_REC_TYPE_0.TXT",
        "field_specs": HEADER_FIELD_SPECS,
        "target_table": "LES_HEADER_TBL",
        "signed_fields": [
            "BASIC_PAY", "GROSS_PAY", "NET_PAY", "FED_TAX", "STATE_TAX",
            "FICA_TAX", "MEDICARE_TAX", "RETIREMENT", "FEGLI", "FEHB",
            "TSP_TAX_DEF", "TSP_ROTH", "ALLOTMENTS", "ADJUSTMENTS",
        ],
    },
    "C": {
        "file_pattern": "EMP_REC_TYPE_C.TXT",
        "field_specs": RECTYPE_C_SPECS,
        "target_table": "LES_EMP_DETAIL_RECTYPE_C_TBL",
        "signed_fields": [
            "EARN1_AMOUNT", "EARN2_AMOUNT", "EARN3_AMOUNT",
            "EARN1_HOURS", "EARN2_HOURS", "EARN3_HOURS",
        ],
    },
    "D": {
        "file_pattern": "EMP_REC_TYPE_D.TXT",
        "field_specs": RECTYPE_D_SPECS,
        "target_table": "LES_EMP_DETAIL_RECTYPE_D_TBL",
        "signed_fields": [
            "DED1_CURRENT", "DED1_YTD", "DED2_CURRENT", "DED2_YTD",
        ],
    },
    "L": {
        "file_pattern": "EMP_REC_TYPE_L.TXT",
        "field_specs": RECTYPE_L_SPECS,
        "target_table": "LES_EMP_DETAIL_RECTYPE_L_TBL",
        "signed_fields": [
            "LEAVE_BEG_BAL", "LEAVE_EARNED", "LEAVE_USED",
            "LEAVE_ADJ", "LEAVE_END_BAL", "LEAVE_USE_OR_LOSE",
            "LEAVE_PROJ_YEAR_END",
        ],
    },
    "M": {
        "file_pattern": "EMP_REC_TYPE_M.TXT",
        "field_specs": RECTYPE_M_SPECS,
        "target_table": "LES_EMP_DETAIL_RECTYPE_M_TBL",
        "signed_fields": [],
    },
    "R": {
        "file_pattern": "EMP_REC_TYPE_R.TXT",
        "field_specs": RECTYPE_R_SPECS,
        "target_table": "LES_EMP_DETAIL_RECTYPE_R_TBL",
        "signed_fields": [
            "RET_CURRENT", "RET_YTD", "RET_GOV_MATCH", "RET_GOV_MATCH_YTD",
        ],
    },
    "T": {
        "file_pattern": "EMP_REC_TYPE_T.TXT",
        "field_specs": RECTYPE_T_SPECS,
        "target_table": "LES_EMP_DETAIL_RECTYPE_T_TBL",
        "signed_fields": [
            "TAX_CURRENT", "TAX_YTD", "TAX_ADD_WITHHOLD",
        ],
    },
    "U": {
        "file_pattern": "EMP_REC_TYPE_U.TXT",
        "field_specs": RECTYPE_U_SPECS,
        "target_table": "LES_EMP_DETAIL_RECTYPE_U_TBL",
        "signed_fields": [
            "UNION_CURRENT", "UNION_YTD",
        ],
    },
}


def process_record_type(spark, rec_type_key, config, input_dir, pp_num, pp_end_year):
    """
    Process a single LES record type file.

    Parameters
    ----------
    spark : SparkSession
    rec_type_key : str
    config : dict
    input_dir : str
    pp_num : int
    pp_end_year : int

    Returns
    -------
    int
        Number of records processed.
    """
    file_path = os.path.join(input_dir, config["file_pattern"])
    logger.info("Processing %s: %s", rec_type_key, file_path)

    if not os.path.exists(file_path):
        logger.warning("File not found: %s — skipping record type %s", file_path, rec_type_key)
        return 0

    # Parse fixed-width file
    raw_df = parse_fixed_width(spark, file_path, config["field_specs"])

    # Classify records and filter details
    classified_df = raw_df.withColumn(
        "RECORD_TYPE_FLAG",
        classify_record_type(F.col("DFAS_PSEUDO_SSN")),
    )
    detail_df = classified_df.filter(F.col("RECORD_TYPE_FLAG") == "D")

    # Convert PP_END_DATE
    detail_df = detail_df.withColumn(
        "PP_END_DATE_CONV",
        validate_and_convert_date(F.col("PP_END_DATE"), F.lit("YYYYMMDD")),
    )

    # Parse signed numeric fields
    for field in config["signed_fields"]:
        if field in detail_df.columns:
            detail_df = detail_df.withColumn(
                field, parse_signed_amount(F.col(field), 2)
            )

    # Enrich with pay period
    enriched_df = enrich_with_pay_period(detail_df, spark)

    count = enriched_df.count()

    # Handle special unpivot for type C (3 earnings per record)
    if rec_type_key == "C":
        earnings_dfs = []
        for i in range(1, 4):
            earn_df = enriched_df.select(
                "DFAS_PSEUDO_SSN", "REC_TYPE", "PP_END_DATE_CONV",
                "AGENCY_CODE", "PP_NUM", "PP_END_YEAR",
                F.col(f"EARN{i}_TYPE").alias("EARN_TYPE"),
                F.col(f"EARN{i}_HOURS").alias("EARN_HOURS"),
                F.col(f"EARN{i}_AMOUNT").alias("EARN_AMOUNT"),
            ).filter(F.col("EARN_TYPE").isNotNull() & (F.trim(F.col("EARN_TYPE")) != ""))
            earnings_dfs.append(earn_df)

        if earnings_dfs:
            unpivoted = earnings_dfs[0]
            for df in earnings_dfs[1:]:
                unpivoted = unpivoted.unionByName(df)
            write_oracle_table(unpivoted, config["target_table"], mode="append")
            count = unpivoted.count()
    elif rec_type_key == "D":
        # Unpivot 2 deductions per record
        ded_dfs = []
        for i in range(1, 3):
            ded_df = enriched_df.select(
                "DFAS_PSEUDO_SSN", "REC_TYPE", "PP_END_DATE_CONV",
                "AGENCY_CODE", "PP_NUM", "PP_END_YEAR",
                F.col(f"DED{i}_TYPE").alias("DED_TYPE"),
                F.col(f"DED{i}_CURRENT").alias("DED_CURRENT"),
                F.col(f"DED{i}_YTD").alias("DED_YTD"),
            ).filter(F.col("DED_TYPE").isNotNull() & (F.trim(F.col("DED_TYPE")) != ""))
            ded_dfs.append(ded_df)

        if ded_dfs:
            unpivoted = ded_dfs[0]
            for df in ded_dfs[1:]:
                unpivoted = unpivoted.unionByName(df)
            write_oracle_table(unpivoted, config["target_table"], mode="append")
            count = unpivoted.count()
    else:
        write_oracle_table(enriched_df, config["target_table"], mode="append")

    logger.info("Record type %s: %d records written to %s",
                rec_type_key, count, config["target_table"])
    return count


def run(input_dir=None, environment=None):
    """
    Execute the full LES processing workflow.

    Parameters
    ----------
    input_dir : str, optional
    environment : str, optional
    """
    logger.info("=" * 60)
    logger.info("Starting LES Processing")
    logger.info("=" * 60)

    if input_dir is None:
        input_dir = FILE_PATHS["les_input_dir"]

    spark = get_spark_session("les", environment)

    try:
        # Get current pay period
        from pyspark_migration.common.pay_period import get_current_pay_period
        pp_df = get_current_pay_period(spark)
        pp_row = pp_df.collect()[0]
        pp_num = int(pp_row["PP_NUM"])
        pp_end_year = int(pp_row["PP_END_YEAR"])

        total_records = 0
        for rec_type_key, config in RECORD_TYPE_CONFIG.items():
            count = process_record_type(
                spark, rec_type_key, config, input_dir, pp_num, pp_end_year
            )
            total_records += count

            log_counter(
                spark, "les_processing",
                f"LES records type {rec_type_key}", count,
                pp_end_year, pp_num,
            )

        log_counter(
            spark, "les_processing",
            "Total LES records processed", total_records,
            pp_end_year, pp_num,
        )

        logger.info(
            "LES Processing completed: %d total records across all types",
            total_records,
        )

    except Exception:
        logger.exception("LES Processing failed")
        from pyspark_migration.common.notification import send_failure_email
        send_failure_email("LES Processing", "Job failed - check logs")
        raise
    finally:
        spark.stop()


def main():
    """CLI entry point for LES processing."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="BIIS LES Processing")
    parser.add_argument("--input-dir", type=str, help="Path to LES input directory")
    parser.add_argument("--environment", type=str, help="Environment (dev/test/prod)")
    args = parser.parse_args()

    run(input_dir=args.input_dir, environment=args.environment)


if __name__ == "__main__":
    main()
