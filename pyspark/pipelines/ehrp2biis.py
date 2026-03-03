"""
EHRP2BIIS PySpark Pipeline.

Replaces:
  - ehrp2biis_preload (KSH script -> step01 SQL)
  - XML/EHRP2BIIS_UPDATE (Informatica PowerCenter mapping)
  - ehrp2biis_afterload.sql (SQL post-load script)
  - actstage_load (KSH script -> action_stage_load SQL)

This is the primary integration pipeline that extracts government employee
personnel actions from EHRP and loads them into BIIS.

Processing Flow:
  EHRP Source -> Preload -> Main ETL -> Afterload -> Action Staging -> BIIS DB

Source Tables:
  - EHRP.PS_GVT_JOB (joined with NWK_NEW_EHRP_ACTIONS_TBL)
  - NKNIGHT.NWK_NEW_EHRP_ACTIONS_TBL
  - NKNIGHT.EHRP_RECS_TRACKING_TBL
  - NKNIGHT.SEQUENCE_NUM_TBL

Target Tables:
  - NKNIGHT.NWK_ACTION_PRIMARY_TBL
  - NKNIGHT.NWK_ACTION_SECONDARY_TBL
  - ACTION_PRIMARY_ALL, ACTION_SECONDARY_ALL, ACTION_REMARKS_ALL
"""

import logging

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType

from pyspark.utils.config import AppConfig
from pyspark.utils.spark_session import create_spark_session
from pyspark.utils.db_utils import (
    read_table,
    read_ehrp_table,
    write_table,
    execute_sql,
    call_stored_procedure,
)
from pyspark.utils.logging_utils import setup_logging
from pyspark.utils.notifications import (
    send_success_notification,
    send_failure_notification,
)

logger = logging.getLogger(__name__)


def preload(spark: SparkSession, config: AppConfig) -> None:
    """Execute the EHRP2BIIS preload step.

    Replaces: ehrp2biis_preload KSH script which calls step01 SQL.
    Prepares the database environment before the main ETL runs.
    """
    logger.info("=== EHRP2BIIS Preload ===")
    logger.info("Executing preload step01 - preparing database environment")

    # The preload script runs step01 SQL which prepares staging tables
    # and resets tracking state for the new load
    execute_sql(spark, config.db, [
        # Clear any temporary data from previous runs
        "DELETE FROM NKNIGHT.NWK_NEW_EHRP_ACTIONS_TBL WHERE 1=1",
        "COMMIT",
    ])
    logger.info("Preload step01 completed")


def extract_ehrp_actions(spark: SparkSession, config: AppConfig):
    """Extract new EHRP actions by joining PS_GVT_JOB with tracking table.

    Replaces: The Source Qualifier SQ_PS_GVT_JOB in the Informatica
    EHRP2BIIS_UPDATE mapping which joins PS_GVT_JOB with
    NWK_NEW_EHRP_ACTIONS_TBL on EMPLID, EMPL_RCD, EFFDT, EFFSEQ.

    Returns:
        DataFrame of new EHRP action records to process.
    """
    logger.info("Extracting new EHRP actions from source")

    # Read the new actions list from BIIS database
    new_actions_df = read_table(
        spark,
        config.db,
        table_name="NKNIGHT.NWK_NEW_EHRP_ACTIONS_TBL",
    )

    if new_actions_df.count() == 0:
        logger.info("No new EHRP actions to process")
        return new_actions_df

    # Read the full job records from the EHRP source
    ehrp_job_df = read_ehrp_table(
        spark,
        config.ehrp_source,
        table_name="EHRP.PS_GVT_JOB",
    )

    # Join on key fields (Source Qualifier join condition)
    joined_df = ehrp_job_df.join(
        new_actions_df,
        on=["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"],
        how="inner",
    )

    record_count = joined_df.count()
    logger.info("Extracted %d EHRP action records", record_count)
    return joined_df


def transform_ehrp_actions(
    spark: SparkSession,
    config: AppConfig,
    ehrp_df,
) -> tuple:
    """Transform EHRP records into BIIS action format.

    Replaces: The Expression, Lookup, and Filter transformations in the
    Informatica EHRP2BIIS_UPDATE mapping including:
      - exp_GET_EFFDT_YEAR (extract year from EFFDT)
      - lkp_OLD_SEQUENCE_NUMBER (look up existing event IDs)
      - exp_SET_EVENT_ID (assign new event IDs from sequence)
      - Various field formatting expressions

    Args:
        spark: Active SparkSession.
        config: Application configuration.
        ehrp_df: DataFrame from extract_ehrp_actions.

    Returns:
        Tuple of (primary_df, secondary_df) DataFrames ready for loading.
    """
    logger.info("Transforming EHRP action records")

    if ehrp_df.count() == 0:
        return spark.createDataFrame([], StructType()), spark.createDataFrame([], StructType())

    # Get current sequence number for event ID assignment
    read_table(
        spark,
        config.db,
        table_name="NKNIGHT.SEQUENCE_NUM_TBL",
    )

    # Extract EFFDT year (exp_GET_EFFDT_YEAR equivalent)
    transformed = ehrp_df.withColumn(
        "EFFDT_YEAR",
        F.year(F.col("EFFDT")),
    )

    # Add load date (TRUNC(SYSDATE) equivalent)
    transformed = transformed.withColumn(
        "LOAD_DATE",
        F.current_date(),
    )

    # Look up existing sequence numbers for tracking
    tracking_df = read_table(
        spark,
        config.db,
        table_name="NKNIGHT.EHRP_RECS_TRACKING_TBL",
    )

    # Left join to find records that already exist
    transformed = transformed.join(
        tracking_df.select("EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ", "BIIS_EVENT_ID"),
        on=["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"],
        how="left",
    )

    # Assign new event IDs using monotonically_increasing_id for new records
    # For records with existing BIIS_EVENT_ID, keep it; for new ones, assign
    transformed = transformed.withColumn(
        "EVENT_ID",
        F.when(
            F.col("BIIS_EVENT_ID").isNotNull(),
            F.col("BIIS_EVENT_ID"),
        ).otherwise(
            F.monotonically_increasing_id() + 1
        ),
    )

    # Build primary action record
    # Selecting the key columns that map to nwk_action_primary_tbl
    primary_cols = [
        "EVENT_ID", "EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ",
        "DEPTID", "JOBCODE", "POSITION_NBR", "EMPL_STATUS",
        "ACTION", "ACTION_DT", "ACTION_REASON", "LOCATION",
        "REG_TEMP", "FULL_PART_TIME", "COMPANY", "PAYGROUP",
        "SAL_ADMIN_PLAN", "GRADE", "GRADE_ENTRY_DT", "STEP",
        "STEP_ENTRY_DT", "COMP_FREQUENCY", "COMPRATE",
        "CHANGE_AMT", "CHANGE_PCT", "ANNUAL_RT", "MONTHLY_RT",
        "DAILY_RT", "HOURLY_RT", "ANNL_BENEF_BASE_RT",
        "CURRENCY_CD", "BUSINESS_UNIT", "GVT_NOA_CODE",
        "GVT_WIP_STATUS", "GVT_STATUS_TYPE",
        "GVT_LEG_AUTH_1", "GVT_PAR_AUTH_D1", "GVT_PAR_AUTH_D1_2",
        "GVT_LEG_AUTH_2", "GVT_PAR_AUTH_D2", "GVT_PAR_AUTH_D2_2",
        "GVT_PAR_NTE_DATE", "GVT_WORK_SCHED", "GVT_SUB_AGENCY",
        "LOAD_DATE",
    ]

    # Filter to columns that exist in the DataFrame
    available_primary_cols = [
        c for c in primary_cols if c in transformed.columns
    ]
    primary_df = transformed.select(*available_primary_cols)

    # Build secondary action record
    secondary_cols = [
        "EVENT_ID", "EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ",
        "POSITION_OVERRIDE", "POSN_CHANGE_RECORD", "SHIFT",
        "BAS_GROUP_ID", "ELIG_CONFIG1", "ELIG_CONFIG2", "ELIG_CONFIG3",
        "ELIG_CONFIG4", "ELIG_CONFIG5", "ELIG_CONFIG6", "ELIG_CONFIG7",
        "ELIG_CONFIG8", "ELIG_CONFIG9", "BEN_STATUS", "BAS_ACTION",
        "COBRA_ACTION", "EMPL_TYPE", "HOLIDAY_SCHEDULE",
        "STD_HOURS", "STD_HRS_FREQUENCY", "OFFICER_CD", "EMPL_CLASS",
        "GL_PAY_TYPE", "ACCT_CD", "EARNS_DIST_TYPE",
        "SHIFT_RT", "SHIFT_FACTOR", "SETID_DEPT", "SETID_JOBCODE",
        "SETID_LOCATION", "SETID_SALARY", "REG_REGION",
        "DIRECTLY_TIPPED", "FLSA_STATUS", "EEO_CLASS", "FUNCTION_CD",
    ]

    available_secondary_cols = [
        c for c in secondary_cols if c in transformed.columns
    ]
    secondary_df = transformed.select(*available_secondary_cols)

    logger.info(
        "Transformed %d primary and %d secondary records",
        primary_df.count(),
        secondary_df.count(),
    )
    return primary_df, secondary_df


def load_action_records(
    spark: SparkSession,
    config: AppConfig,
    primary_df,
    secondary_df,
) -> None:
    """Load transformed action records into staging tables.

    Replaces: Target load in Informatica EHRP2BIIS_UPDATE mapping.

    Args:
        spark: Active SparkSession.
        config: Application configuration.
        primary_df: Primary action records DataFrame.
        secondary_df: Secondary action records DataFrame.
    """
    logger.info("Loading action records to staging tables")

    if primary_df.count() == 0:
        logger.info("No records to load")
        return

    write_table(
        primary_df, config.db,
        "NKNIGHT.NWK_ACTION_PRIMARY_TBL", mode="append",
    )
    logger.info("Loaded primary action records")

    write_table(
        secondary_df, config.db,
        "NKNIGHT.NWK_ACTION_SECONDARY_TBL", mode="append",
    )
    logger.info("Loaded secondary action records")


def afterload(spark: SparkSession, config: AppConfig) -> None:
    """Execute the EHRP2BIIS afterload processing.

    Replaces: ehrp2biis_afterload.sql

    Steps:
      1. Update retained step codes (Step 04)
      2. Update sequence number table (Step 05)
      3. Run HISTDBA stored procedures for record formatting
      4. Update PROCESS_TABLE for WIP status checking
      5. Insert today's records into historical tables
      6. Process cancelled actions
      7. Truncate new actions table for next load
    """
    logger.info("=== EHRP2BIIS Afterload ===")

    # Step 04: Update retained step codes with '0.0000000000000' to NULL
    logger.info("Step 04: Updating retained step codes")
    execute_sql(spark, config.db, [
        """UPDATE NKNIGHT.NWK_ACTION_SECONDARY_TBL a
           SET a.RETND1_STEP_CD = NULL
           WHERE a.EVENT_ID IN (
               SELECT b.EVENT_ID
               FROM NKNIGHT.NWK_ACTION_PRIMARY_TBL b
               WHERE b.LOAD_DATE = TRUNC(SYSDATE)
               AND b.EVENT_ID < 9000000000
           )
           AND a.RETND1_STEP_CD = '0.0000000000000'""",
        "COMMIT",
    ])

    # Step 05: Update sequence number table
    logger.info("Step 05: Updating sequence number table")
    call_stored_procedure(spark, config.db, "UPDATE_SEQUENCE_NUMBER_TBL_P")

    # Run HISTDBA formatting procedures
    logger.info("Running HISTDBA formatting procedures")

    call_stored_procedure(
        spark, config.db, "HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P"
    )
    call_stored_procedure(
        spark, config.db, "HISTDBA.UPDATE_ERP2BIIS_NO900S01_P"
    )
    call_stored_procedure(
        spark, config.db, "HISTDBA.ERP2BIIS_CRE8_REMARKS_900S01"
    )
    call_stored_procedure(
        spark, config.db, "HISTDBA.UPDATE_ERP2BIIS_900SONLY01_P"
    )
    logger.info("Completed all 4 formatting procedures")

    # Update original cancelled transactions
    call_stored_procedure(
        spark, config.db, "HISTDBA.UPDT_ORIG_CANCELLED_TRANS01_P"
    )

    # Update PROCESS_TABLE for WIP status checking
    logger.info("Updating PROCESS_TABLE")
    execute_sql(spark, config.db, [
        "UPDATE PROCESS_TABLE SET P_STARTDT = NULL",
        "COMMIT",
        """UPDATE PROCESS_TABLE SET P_STARTDT = (
            SELECT EFFDT FROM (
                SELECT a.BIIS_EVENT_ID, a.EMPLID, a.EMPL_RCD,
                       a.EFFDT, a.EFFSEQ, b.DEPTID,
                       a.GVT_WIP_STATUS, b.GVT_WIP_STATUS
                FROM NKNIGHT.EHRP_RECS_TRACKING_TBL a,
                     EHRP.PS_GVT_JOB b
                WHERE a.EMPLID = b.EMPLID
                AND a.EMPL_RCD = b.EMPL_RCD
                AND a.EFFDT = b.EFFDT
                AND a.EFFSEQ = b.EFFSEQ
                AND a.GVT_WIP_STATUS <> b.GVT_WIP_STATUS
                AND a.CHANGED_WIP_STATUS IS NULL
                ORDER BY 4, 1
            ) WHERE ROWNUM < 2
        )""",
        "COMMIT",
        """UPDATE PROCESS_TABLE
           SET P_STARTDT = TRUNC(SYSDATE) + 10000
           WHERE P_STARTDT IS NULL""",
        "COMMIT",
    ])

    # Compile and execute WIP status check procedure
    execute_sql(spark, config.db, [
        "ALTER PROCEDURE CHK_EHRP2BIIS_WIP_STATUS_P COMPILE",
    ])
    call_stored_procedure(spark, config.db, "CHK_EHRP2BIIS_WIP_STATUS_P")

    # Insert today's records into historical tables
    logger.info("Inserting today's records into historical tables")
    execute_sql(spark, config.db, [
        """INSERT INTO ACTION_PRIMARY_ALL
           SELECT * FROM NKNIGHT.NWK_ACTION_PRIMARY_TBL
           WHERE LOAD_DATE = TRUNC(SYSDATE)""",
        """INSERT INTO ACTION_SECONDARY_ALL
           SELECT * FROM NKNIGHT.NWK_ACTION_SECONDARY_TBL
           WHERE EVENT_ID IN (
               SELECT EVENT_ID FROM NKNIGHT.NWK_ACTION_PRIMARY_TBL
               WHERE LOAD_DATE = TRUNC(SYSDATE)
           )""",
        """INSERT INTO ACTION_REMARKS_ALL
           SELECT * FROM NKNIGHT.NWK_ACTION_REMARKS_TBL
           WHERE EVENT_ID IN (
               SELECT EVENT_ID FROM NKNIGHT.NWK_ACTION_PRIMARY_TBL
               WHERE LOAD_DATE = TRUNC(SYSDATE)
           )""",
        "COMMIT",
    ])

    # Gather run counts
    call_stored_procedure(
        spark, config.db, "HISTDBA.GATHER_EHRP2BIIS_RUNCOUNTS_P", "NULL"
    )

    # Process cancelled actions
    logger.info("Processing cancelled actions")
    execute_sql(spark, config.db, [
        # Delete old versions of cancelled actions from historical tables
        """DELETE FROM ACTION_SECONDARY_ALL
           WHERE EVENT_ID IN (
               SELECT BIIS_EVENT_ID FROM NKNIGHT.EHRP_RECS_TRACKING_TBL
               WHERE BIIS_WIP_STATUS_CHANGED_DT = TRUNC(SYSDATE)
           )""",
        """DELETE FROM ACTION_REMARKS_ALL
           WHERE EVENT_ID IN (
               SELECT BIIS_EVENT_ID FROM NKNIGHT.EHRP_RECS_TRACKING_TBL
               WHERE BIIS_WIP_STATUS_CHANGED_DT = TRUNC(SYSDATE)
           )""",
        """DELETE FROM ACTION_PRIMARY_ALL
           WHERE EVENT_ID IN (
               SELECT BIIS_EVENT_ID FROM NKNIGHT.EHRP_RECS_TRACKING_TBL
               WHERE BIIS_WIP_STATUS_CHANGED_DT = TRUNC(SYSDATE)
           )""",
        # Re-insert current versions
        """INSERT INTO ACTION_PRIMARY_ALL
           SELECT * FROM NKNIGHT.NWK_ACTION_PRIMARY_TBL
           WHERE EVENT_ID IN (
               SELECT BIIS_EVENT_ID FROM NKNIGHT.EHRP_RECS_TRACKING_TBL
               WHERE BIIS_WIP_STATUS_CHANGED_DT = TRUNC(SYSDATE)
           )""",
        """INSERT INTO ACTION_SECONDARY_ALL
           SELECT * FROM NKNIGHT.NWK_ACTION_SECONDARY_TBL
           WHERE EVENT_ID IN (
               SELECT BIIS_EVENT_ID FROM NKNIGHT.EHRP_RECS_TRACKING_TBL
               WHERE BIIS_WIP_STATUS_CHANGED_DT = TRUNC(SYSDATE)
           )""",
        """INSERT INTO ACTION_REMARKS_ALL
           SELECT * FROM NKNIGHT.NWK_ACTION_REMARKS_TBL
           WHERE EVENT_ID IN (
               SELECT BIIS_EVENT_ID FROM NKNIGHT.EHRP_RECS_TRACKING_TBL
               WHERE BIIS_WIP_STATUS_CHANGED_DT = TRUNC(SYSDATE)
           )""",
        "COMMIT",
    ])

    # Truncate new actions table for next load
    logger.info("Truncating NWK_NEW_EHRP_ACTIONS_TBL for next load")
    execute_sql(spark, config.db, [
        "TRUNCATE TABLE NKNIGHT.NWK_NEW_EHRP_ACTIONS_TBL",
    ])

    logger.info("Afterload completed successfully")


def run(config=None):
    """Execute the full EHRP2BIIS pipeline.

    Replaces the complete EHRP2BIIS workflow:
      1. ehrp2biis_preload (KSH)
      2. EHRP2BIIS_UPDATE (Informatica mapping)
      3. ehrp2biis_afterload.sql
      4. actstage_load (KSH)
    """
    if config is None:
        config = AppConfig()

    log_file = setup_logging("ehrp2biis", config.paths)
    spark = create_spark_session(config, "EHRP2BIIS")

    try:
        # Step 1: Preload
        preload(spark, config)

        # Step 2: Extract EHRP actions
        ehrp_df = extract_ehrp_actions(spark, config)

        # Step 3: Transform
        primary_df, secondary_df = transform_ehrp_actions(
            spark, config, ehrp_df
        )

        # Step 4: Load to staging
        load_action_records(spark, config, primary_df, secondary_df)

        # Step 5: Afterload processing
        afterload(spark, config)

        send_success_notification(
            config.email,
            process_name="EHRP2BIIS",
            message="EHRP2BIIS pipeline completed successfully",
            log_file=log_file,
        )
        logger.info("EHRP2BIIS pipeline completed successfully")

    except Exception as e:
        logger.exception("EHRP2BIIS pipeline failed")
        send_failure_notification(
            config.email,
            process_name="EHRP2BIIS",
            error_message=str(e),
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
