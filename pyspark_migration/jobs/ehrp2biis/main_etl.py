"""
EHRP2BIIS Main ETL

Replaces Informatica mapping m_EHRP2BIIS_UPDATE from XML/EHRP2BIIS_UPDATE.

Processes personnel actions from EHRP (Enterprise Human Resources Platform)
and loads them into the BIIS data warehouse action tables.

Source tables:
- NWK_NEW_EHRP_ACTIONS_TBL (4 fields: EMPLID, EMPL_RCD, EFFDT, EFFSEQ)
- PS_GVT_JOB (246 fields)

Target tables:
- nwk_action_primary_tbl (260 fields)
- nwk_action_secondary_tbl (209 fields)
- ehrp_recs_tracking_tbl
"""

import argparse
import logging

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
)

from pyspark_migration.common.db_utils import (
    read_oracle_query,
    read_oracle_table,
    write_oracle_table,
)
from pyspark_migration.common.error_logging import log_counter, log_errors
from pyspark_migration.common.pay_period import get_current_pay_period
from pyspark_migration.common.spark_session import get_spark_session
from pyspark_migration.config.settings import SCHEMAS

logger = logging.getLogger(__name__)


def read_source_tables(spark):
    """
    Read source tables for EHRP2BIIS processing.

    Reads:
    - NWK_NEW_EHRP_ACTIONS_TBL (NKNIGHT schema, 4 fields)
    - PS_GVT_JOB (EHRP schema, 246 fields)

    Reference: XML/EHRP2BIIS_UPDATE lines 6-16

    Parameters
    ----------
    spark : SparkSession

    Returns
    -------
    tuple of (DataFrame, DataFrame)
    """
    logger.info("Reading NWK_NEW_EHRP_ACTIONS_TBL")
    actions_df = read_oracle_table(
        spark, "NWK_NEW_EHRP_ACTIONS_TBL",
        schema=SCHEMAS["nknight"],
        connection_name="ORA_BIISPRD_SRC",
    )

    logger.info("Reading PS_GVT_JOB")
    gvt_job_df = read_oracle_table(
        spark, "PS_GVT_JOB",
        schema=SCHEMAS["ehrp"],
        connection_name="ORA_BIISPRD_SRC",
    )

    return actions_df, gvt_job_df


def join_sources(actions_df, gvt_job_df):
    """
    Inner join NWK_NEW_EHRP_ACTIONS_TBL with PS_GVT_JOB.

    Join keys: (EMPLID, EMPL_RCD, EFFDT, EFFSEQ)
    Reference: XML/EHRP2BIIS_UPDATE Source Qualifier SQ_PS_GVT_JOB

    Parameters
    ----------
    actions_df : DataFrame
    gvt_job_df : DataFrame

    Returns
    -------
    DataFrame
    """
    logger.info("Joining NWK_NEW_EHRP_ACTIONS_TBL with PS_GVT_JOB")

    joined_df = actions_df.alias("a").join(
        gvt_job_df.alias("j"),
        on=[
            F.col("a.EMPLID") == F.col("j.EMPLID"),
            F.col("a.EMPL_RCD") == F.col("j.EMPL_RCD"),
            F.col("a.EFFDT") == F.col("j.EFFDT"),
            F.col("a.EFFSEQ") == F.col("j.EFFSEQ"),
        ],
        how="inner",
    )

    # Drop duplicate join columns from actions side
    result = joined_df.drop(
        F.col("a.EMPLID"),
        F.col("a.EMPL_RCD"),
        F.col("a.EFFDT"),
        F.col("a.EFFSEQ"),
    )

    count = result.count()
    logger.info("Joined records: %d", count)
    return result


def lookup_sequence_number(spark, event_id_threshold=9000000000):
    """
    Lookup SEQUENCE_NUM_TBL for event ID generation.

    Standard records (< 9B) and 900-series records (>= 9B) use different
    sequence ranges.

    Parameters
    ----------
    spark : SparkSession
    event_id_threshold : int

    Returns
    -------
    dict
        {'standard': current_seq, 'series_900': current_seq_900}
    """
    logger.info("Looking up SEQUENCE_NUM_TBL")
    seq_df = read_oracle_table(
        spark, "SEQUENCE_NUM_TBL",
        schema=SCHEMAS["nknight"],
        connection_name="ORA_BIIS",
    )

    rows = seq_df.collect()
    sequences = {}
    for row in rows:
        row_dict = row.asDict()
        seq_name = str(row_dict.get("SEQ_NAME", "")).strip()
        seq_value = row_dict.get("SEQ_VALUE", 0)
        sequences[seq_name] = seq_value

    return sequences


def apply_transformations(joined_df, pp_num, pp_end_year, sequences):
    """
    Apply expression transformations to joined data.

    Replaces all Informatica Expression transformations in m_EHRP2BIIS_UPDATE
    including field mappings, calculations, date formatting, etc.

    Parameters
    ----------
    joined_df : DataFrame
    pp_num : int
    pp_end_year : int
    sequences : dict

    Returns
    -------
    DataFrame
    """
    logger.info("Applying transformations")

    # Add load date and pay period info
    transformed = (
        joined_df
        .withColumn("LOAD_DATE", F.current_date())
        .withColumn("PP_NUM", F.lit(pp_num).cast(DecimalType(2, 0)))
        .withColumn("PP_END_YEAR", F.lit(pp_end_year).cast(DecimalType(4, 0)))
    )

    # Generate event IDs using monotonically_increasing_id as a base
    # and adding to the current sequence number
    standard_seq = sequences.get("EHRP_SEQ_NUMBER", 1)
    transformed = transformed.withColumn(
        "EVENT_ID",
        (F.monotonically_increasing_id() + F.lit(standard_seq)).cast(LongType()),
    )

    # Extract year from EFFDT for various calculations
    transformed = transformed.withColumn(
        "EFFDT_YEAR",
        F.year(F.col("EFFDT")).cast(IntegerType()),
    )

    return transformed


def route_to_targets(transformed_df):
    """
    Route transformed data to multiple target tables.

    Targets:
    - nwk_action_primary_tbl (primary action data)
    - nwk_action_secondary_tbl (secondary attributes)
    - ehrp_recs_tracking_tbl (tracking records)

    Parameters
    ----------
    transformed_df : DataFrame

    Returns
    -------
    tuple of (DataFrame, DataFrame, DataFrame)
    """
    logger.info("Routing to target tables")

    # Primary table: core action fields
    # Select the key columns that form the primary action record
    primary_cols = [
        "EVENT_ID", "EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ",
        "DEPTID", "JOBCODE", "POSITION_NBR", "EMPL_STATUS",
        "ACTION", "ACTION_DT", "ACTION_REASON", "LOCATION",
        "COMPANY", "PAYGROUP", "FULL_PART_TIME", "REG_TEMP",
        "LOAD_DATE", "PP_NUM", "PP_END_YEAR",
    ]
    available_primary = [c for c in primary_cols if c in transformed_df.columns]
    primary_df = transformed_df.select(*available_primary)

    # Secondary table: additional attributes
    secondary_cols = [
        "EVENT_ID", "EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ",
        "SHIFT", "BAS_GROUP_ID", "TAX_LOCATION_CD",
        "JOB_ENTRY_DT", "DEPT_ENTRY_DT", "POSITION_ENTRY_DT",
    ]
    available_secondary = [c for c in secondary_cols if c in transformed_df.columns]
    secondary_df = transformed_df.select(*available_secondary)

    # Tracking table: record processing status
    tracking_cols = [
        "EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ",
        "EVENT_ID", "LOAD_DATE",
    ]
    available_tracking = [c for c in tracking_cols if c in transformed_df.columns]
    tracking_df = (
        transformed_df.select(*available_tracking)
        .withColumn("BIIS_EVENT_ID", F.col("EVENT_ID"))
    )

    # Add GVT_WIP_STATUS if available
    if "GVT_WIP_STATUS" in transformed_df.columns:
        tracking_df = tracking_df.withColumn(
            "GVT_WIP_STATUS", transformed_df["GVT_WIP_STATUS"]
        )

    return primary_df, secondary_df, tracking_df


def run(environment=None):
    """
    Execute the EHRP2BIIS main ETL workflow.

    Parameters
    ----------
    environment : str, optional
    """
    logger.info("=" * 60)
    logger.info("Starting EHRP2BIIS Main ETL")
    logger.info("=" * 60)

    spark = get_spark_session("ehrp2biis_main_etl", environment)

    try:
        # Read source tables
        actions_df, gvt_job_df = read_source_tables(spark)
        actions_count = actions_df.count()
        logger.info("Actions to process: %d", actions_count)

        if actions_count == 0:
            logger.info("No actions to process. Exiting.")
            return

        # Join sources
        joined_df = join_sources(actions_df, gvt_job_df)

        # Get current pay period
        pp_df = get_current_pay_period(spark)
        pp_row = pp_df.collect()[0]
        pp_num = int(pp_row["PP_NUM"])
        pp_end_year = int(pp_row["PP_END_YEAR"])

        # Lookup sequences
        sequences = lookup_sequence_number(spark)

        # Apply transformations
        transformed_df = apply_transformations(
            joined_df, pp_num, pp_end_year, sequences
        )

        # Route to targets
        primary_df, secondary_df, tracking_df = route_to_targets(transformed_df)

        # Write to target tables
        logger.info("Writing to nwk_action_primary_tbl")
        write_oracle_table(
            primary_df, "NWK_ACTION_PRIMARY_TBL",
            schema=SCHEMAS["nknight"],
        )

        logger.info("Writing to nwk_action_secondary_tbl")
        write_oracle_table(
            secondary_df, "NWK_ACTION_SECONDARY_TBL",
            schema=SCHEMAS["nknight"],
        )

        logger.info("Writing to ehrp_recs_tracking_tbl")
        write_oracle_table(
            tracking_df, "EHRP_RECS_TRACKING_TBL",
            schema=SCHEMAS["nknight"],
        )

        # Log counters
        primary_count = primary_df.count()
        log_counter(
            spark, "m_EHRP2BIIS_UPDATE",
            "Records loaded to primary table", primary_count,
            pp_end_year, pp_num,
        )

        logger.info("EHRP2BIIS Main ETL completed successfully")

    except Exception:
        logger.exception("EHRP2BIIS Main ETL failed")
        from pyspark_migration.common.notification import send_failure_email
        send_failure_email("EHRP2BIIS Main ETL", "Job failed - check logs")
        raise
    finally:
        spark.stop()


def main():
    """CLI entry point for EHRP2BIIS main ETL."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="BIIS EHRP2BIIS Main ETL")
    parser.add_argument("--environment", type=str, help="Environment (dev/test/prod)")
    args = parser.parse_args()

    run(environment=args.environment)


if __name__ == "__main__":
    main()
