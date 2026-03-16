"""
PseudoSSN Job

Implements mapping m_Pseudossn_Load_Pseudossn_From_SDA_Tbl.
Reference: Pseudossn file (root level, 800+ lines of Informatica XML)

Processes SDA flat files to create SSN-to-PseudoSSN mappings for
employee de-identification across the BIIS data warehouse.
"""

import argparse
import logging

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from pyspark_migration.common.cobol_parser import parse_signed_amount
from pyspark_migration.common.db_utils import write_oracle_table
from pyspark_migration.common.error_logging import log_counter, log_errors
from pyspark_migration.common.file_utils import parse_fixed_width
from pyspark_migration.common.pay_period import enrich_with_pay_period
from pyspark_migration.common.spark_session import get_spark_session
from pyspark_migration.common.validation import (
    classify_record_type,
    determine_output_flag,
    is_valid_ssn,
    validate_and_convert_date,
)
from pyspark_migration.config.settings import FILE_PATHS

logger = logging.getLogger(__name__)

# Field specifications for the SDA flat file
# Extracted from Pseudossn XML source definition (lines 6-9+)
# Format: (field_name, start_pos, length, data_type)
SDA_FIELD_SPECS = [
    ("SSN", 1, 9, "string"),
    ("CAN_CD", 10, 8, "string"),
    ("PSEUDOSSN", 18, 9, "string"),
    ("LAST_NAME", 27, 30, "string"),
    ("FIRST_NAME", 57, 20, "string"),
    ("MIDDLE_NAME", 77, 20, "string"),
    ("NAME_SUFFIX", 97, 4, "string"),
    ("BIRTH_DATE", 101, 8, "string"),
    ("SEX_CD", 109, 1, "string"),
    ("MARITAL_STATUS", 110, 1, "string"),
    ("RACE_CD", 111, 2, "string"),
    ("CITIZENSHIP_CD", 113, 3, "string"),
    ("VETERANS_PREF", 116, 2, "string"),
    ("VETERANS_STATUS", 118, 1, "string"),
    ("HANDICAP_CD", 119, 2, "string"),
    ("EDUCATION_LEVEL", 121, 2, "string"),
    ("FUNCTIONAL_CLASS", 123, 2, "string"),
    ("OCC_SERIES", 125, 4, "string"),
    ("GRADE", 129, 2, "string"),
    ("STEP_OR_RATE", 131, 2, "string"),
    ("PAY_PLAN", 133, 2, "string"),
    ("PAY_BASIS", 135, 2, "string"),
    ("BASIC_PAY", 137, 10, "string"),
    ("ADJ_BASIC_PAY", 147, 10, "string"),
    ("LOCALITY_PAY", 157, 10, "string"),
    ("TOTAL_PAY", 167, 10, "string"),
    ("DUTY_STATION", 177, 9, "string"),
    ("POSITION_TITLE", 186, 70, "string"),
    ("POSITION_NUMBER", 256, 8, "string"),
    ("SUPERVISORY_STATUS", 264, 2, "string"),
    ("TENURE", 266, 1, "string"),
    ("SERVICE_COMP_DATE", 267, 8, "string"),
    ("APPOINTMENT_TYPE", 275, 2, "string"),
    ("APPOINTMENT_AUTH_1", 277, 4, "string"),
    ("APPOINTMENT_AUTH_2", 281, 4, "string"),
    ("WORK_SCHEDULE", 285, 1, "string"),
    ("PART_TIME_HOURS", 286, 4, "string"),
    ("HIRE_DATE", 290, 8, "string"),
    ("CAREER_START_DATE", 298, 8, "string"),
    ("AGENCY_CODE", 306, 4, "string"),
    ("PERSONNEL_OFFICE_ID", 310, 4, "string"),
    ("ORG_COMPONENT", 314, 8, "string"),
    ("BARGAINING_UNIT", 322, 4, "string"),
    ("FLSA_STATUS", 326, 1, "string"),
    ("APPROPRIATION_CD", 327, 8, "string"),
    ("FEGLI_CD", 335, 2, "string"),
    ("FEHB_CD", 337, 2, "string"),
    ("RETIREMENT_CD", 339, 1, "string"),
    ("ANNUITANT_IND", 340, 1, "string"),
    ("NOA_CODE_1", 341, 4, "string"),
    ("NOA_CODE_2", 345, 4, "string"),
    ("EFFECTIVE_DATE", 349, 8, "string"),
    ("SEPARATION_DATE", 357, 8, "string"),
    ("SEPARATION_CODE", 365, 4, "string"),
    ("AWARD_AMOUNT", 369, 10, "string"),
    ("UNIF_ALLOW_AMT", 379, 10, "string"),
    ("RETENTION_ALLOW_AMT", 389, 10, "string"),
    ("SUPER_DIFF_AMT", 399, 10, "string"),
    ("DANGER_PAY_AMT", 409, 10, "string"),
    ("COST_OF_LIVING_AMT", 419, 10, "string"),
    ("POST_DIFF_AMT", 429, 10, "string"),
    ("OTHER_PAY_AMT", 439, 10, "string"),
    ("RECORD_STATUS", 449, 1, "string"),
    ("FILLER1", 450, 10, "string"),
    ("FILLER2", 460, 10, "string"),
    ("FILLER3", 470, 10, "string"),
    ("FILLER4", 480, 20, "string"),
]


def run(input_file=None, environment=None):
    """
    Execute the PseudoSSN ETL job.

    Parameters
    ----------
    input_file : str, optional
        Path to SDA input file. Defaults to config setting.
    environment : str, optional
    """
    logger.info("=" * 60)
    logger.info("Starting PseudoSSN Job")
    logger.info("=" * 60)

    if input_file is None:
        input_file = FILE_PATHS["pseudossn_input"]

    spark = get_spark_session("pseudossn", environment)

    try:
        # Step 1: Read SDA file using fixed-width parser
        logger.info("Step 1: Reading SDA file: %s", input_file)
        raw_df = parse_fixed_width(spark, input_file, SDA_FIELD_SPECS)
        total_records = raw_df.count()
        logger.info("Read %d records from SDA file", total_records)

        # Step 2: Classify records (Pseudossn line 790)
        logger.info("Step 2: Classifying record types")
        classified_df = raw_df.withColumn(
            "RECORD_TYPE_FLAG",
            classify_record_type(F.col("SSN")),
        )

        # Step 3: Determine output flag (Pseudossn line 791)
        logger.info("Step 3: Determining output flags")
        flagged_df = classified_df.withColumn(
            "OUTPUT_FLAG",
            determine_output_flag(F.col("RECORD_TYPE_FLAG"), F.col("SSN")),
        )

        # Count record types for logging
        header_count = flagged_df.filter(F.col("RECORD_TYPE_FLAG") == "H").count()
        trailer_count = flagged_df.filter(F.col("RECORD_TYPE_FLAG") == "T").count()
        detail_count = flagged_df.filter(F.col("RECORD_TYPE_FLAG") == "D").count()
        logger.info(
            "Record counts - Header: %d, Trailer: %d, Detail: %d",
            header_count, trailer_count, detail_count,
        )

        # Step 4: Filter detail records with valid SSNs (Pseudossn line 724)
        logger.info("Step 4: Filtering valid detail records")
        valid_df = flagged_df.filter(
            (F.col("RECORD_TYPE_FLAG") == "D") & is_valid_ssn(F.col("SSN"))
        )
        valid_count = valid_df.count()

        # Rejected records for error logging
        rejected_df = flagged_df.filter(
            (F.col("OUTPUT_FLAG") == "R")
            | ((F.col("RECORD_TYPE_FLAG") == "D") & ~is_valid_ssn(F.col("SSN")))
        ).filter(F.col("RECORD_TYPE_FLAG") == "D")

        logger.info("Valid detail records: %d", valid_count)

        # Step 5: Convert dates (Pseudossn lines 795-802)
        logger.info("Step 5: Converting date fields")
        dated_df = (
            valid_df
            .withColumn(
                "HIRE_DATE_CONV",
                validate_and_convert_date(F.col("HIRE_DATE"), F.lit("MMDDYYYY")),
            )
            .withColumn(
                "CAREER_START_DATE_CONV",
                validate_and_convert_date(F.col("CAREER_START_DATE"), F.lit("MMDDYYYY")),
            )
            .withColumn(
                "EFFECTIVE_DATE_CONV",
                validate_and_convert_date(F.col("EFFECTIVE_DATE"), F.lit("YYYYDDMM")),
            )
            .withColumn(
                "SEPARATION_DATE_CONV",
                validate_and_convert_date(F.col("SEPARATION_DATE"), F.lit("YYYYMMDD")),
            )
        )

        # Step 6: Parse signed numerics (Pseudossn lines 832-834)
        logger.info("Step 6: Parsing COBOL signed numeric fields")
        parsed_df = (
            dated_df
            .withColumn("UNIF_ALLOW_AMT_NUM", parse_signed_amount(F.col("UNIF_ALLOW_AMT"), 2))
            .withColumn("RETENTION_ALLOW_AMT_NUM", parse_signed_amount(F.col("RETENTION_ALLOW_AMT"), 2))
            .withColumn("SUPER_DIFF_AMT_NUM", parse_signed_amount(F.col("SUPER_DIFF_AMT"), 2))
            .withColumn("DANGER_PAY_AMT_NUM", parse_signed_amount(F.col("DANGER_PAY_AMT"), 2))
            .withColumn("COST_OF_LIVING_AMT_NUM", parse_signed_amount(F.col("COST_OF_LIVING_AMT"), 2))
            .withColumn("POST_DIFF_AMT_NUM", parse_signed_amount(F.col("POST_DIFF_AMT"), 2))
            .withColumn("OTHER_PAY_AMT_NUM", parse_signed_amount(F.col("OTHER_PAY_AMT"), 2))
            .withColumn("AWARD_AMOUNT_NUM", parse_signed_amount(F.col("AWARD_AMOUNT"), 2))
            .withColumn("BASIC_PAY_NUM", parse_signed_amount(F.col("BASIC_PAY"), 2))
            .withColumn("ADJ_BASIC_PAY_NUM", parse_signed_amount(F.col("ADJ_BASIC_PAY"), 2))
            .withColumn("LOCALITY_PAY_NUM", parse_signed_amount(F.col("LOCALITY_PAY"), 2))
            .withColumn("TOTAL_PAY_NUM", parse_signed_amount(F.col("TOTAL_PAY"), 2))
        )

        # Step 7: Enrich with pay period
        logger.info("Step 7: Enriching with current pay period")
        enriched_df = enrich_with_pay_period(parsed_df, spark)

        # Step 8: Write outputs
        logger.info("Step 8: Writing output tables")

        # Write to staging table (all records)
        logger.info("Writing to PSEUDOSSN_FROM_SDA_TBL (staging)")
        staging_cols = [
            "SSN", "CAN_CD", "PSEUDOSSN", "LAST_NAME", "FIRST_NAME",
            "MIDDLE_NAME", "NAME_SUFFIX", "SEX_CD", "MARITAL_STATUS",
            "RACE_CD", "CITIZENSHIP_CD", "VETERANS_PREF", "VETERANS_STATUS",
            "HANDICAP_CD", "EDUCATION_LEVEL", "FUNCTIONAL_CLASS", "OCC_SERIES",
            "GRADE", "STEP_OR_RATE", "PAY_PLAN", "PAY_BASIS",
            "DUTY_STATION", "POSITION_TITLE", "POSITION_NUMBER",
            "SUPERVISORY_STATUS", "TENURE", "APPOINTMENT_TYPE",
            "APPOINTMENT_AUTH_1", "APPOINTMENT_AUTH_2", "WORK_SCHEDULE",
            "PART_TIME_HOURS", "AGENCY_CODE", "PERSONNEL_OFFICE_ID",
            "ORG_COMPONENT", "BARGAINING_UNIT", "FLSA_STATUS",
            "APPROPRIATION_CD", "FEGLI_CD", "FEHB_CD", "RETIREMENT_CD",
            "ANNUITANT_IND", "NOA_CODE_1", "NOA_CODE_2",
            "SEPARATION_CODE", "RECORD_STATUS",
            "HIRE_DATE_CONV", "CAREER_START_DATE_CONV",
            "EFFECTIVE_DATE_CONV", "SEPARATION_DATE_CONV",
            "BASIC_PAY_NUM", "ADJ_BASIC_PAY_NUM", "LOCALITY_PAY_NUM",
            "TOTAL_PAY_NUM", "AWARD_AMOUNT_NUM", "UNIF_ALLOW_AMT_NUM",
            "RETENTION_ALLOW_AMT_NUM", "SUPER_DIFF_AMT_NUM",
            "DANGER_PAY_AMT_NUM", "COST_OF_LIVING_AMT_NUM",
            "POST_DIFF_AMT_NUM", "OTHER_PAY_AMT_NUM",
            "PP_NUM", "PP_END_YEAR",
        ]
        write_oracle_table(
            enriched_df.select(*staging_cols),
            "PSEUDOSSN_FROM_SDA_TBL",
            mode="overwrite",
        )

        # Write to production table (PK=PSEUDOSSN, Pseudossn lines 451-454)
        logger.info("Writing to PSEUDOSSN_TBL (production)")
        write_oracle_table(
            enriched_df.select(*staging_cols),
            "PSEUDOSSN_TBL",
            mode="overwrite",
        )

        # Write to archive table
        logger.info("Writing to HI_ARCH_PSEUDOSSN_TBL (archive)")
        write_oracle_table(
            enriched_df.select(*staging_cols),
            "HI_ARCH_PSEUDOSSN_TBL",
            mode="append",
        )

        # Get pay period info for counter/error logging
        pp_row = enriched_df.select("PP_END_YEAR", "PP_NUM").first()
        pp_end_year = int(pp_row["PP_END_YEAR"]) if pp_row else 0
        pp_num = int(pp_row["PP_NUM"]) if pp_row else 0

        # Write errors
        if rejected_df.count() > 0:
            error_records = (
                rejected_df
                .withColumn("ERROR_MESSAGE", F.lit("Invalid SSN or rejected record"))
                .withColumn("SOURCE_KEY", F.col("SSN"))
                .select("ERROR_MESSAGE", "SOURCE_KEY")
            )
            log_errors(spark, error_records, "m_Pseudossn_Load", pp_end_year, pp_num)

        # Write counters
        log_counter(
            spark, "m_Pseudossn_Load",
            "Total records read from SDA file", total_records,
            pp_end_year, pp_num,
        )
        log_counter(
            spark, "m_Pseudossn_Load",
            "Valid detail records loaded", valid_count,
            pp_end_year, pp_num,
        )
        log_counter(
            spark, "m_Pseudossn_Load",
            "Rejected records", rejected_df.count(),
            pp_end_year, pp_num,
        )

        logger.info("PseudoSSN Job completed successfully")

    except Exception:
        logger.exception("PseudoSSN Job failed")
        from pyspark_migration.common.notification import send_failure_email
        send_failure_email("PseudoSSN Load", "Job failed - check logs")
        raise
    finally:
        spark.stop()


def main():
    """CLI entry point for PseudoSSN job."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="BIIS PseudoSSN Load Job")
    parser.add_argument("--input-file", type=str, help="Path to SDA input file")
    parser.add_argument("--environment", type=str, help="Environment (dev/test/prod)")
    args = parser.parse_args()

    run(input_file=args.input_file, environment=args.environment)


if __name__ == "__main__":
    main()
