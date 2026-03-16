"""
Pay Period Utilities

PAY_PERIOD broadcast join helper.
Replaces lkp_Current_Pay_Period lookups used across all Informatica mappings
(Pseudossn lines 548-565, COMPTIME, CPM, LES, etc.).
"""

import logging

from pyspark.sql import functions as F

from pyspark_migration.common.db_utils import read_oracle_table

logger = logging.getLogger(__name__)


def get_current_pay_period(spark, connection_name="ORA_BIIS"):
    """
    Read PAY_PERIOD table and filter for the current pay period.

    Replicates lkp_Current_Pay_Period from Pseudossn lines 548-565:
        SELECT * FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'

    Parameters
    ----------
    spark : SparkSession
    connection_name : str

    Returns
    -------
    DataFrame
        Single-row DataFrame with PP_NUM, PP_END_YEAR, PP_START_DTE,
        PP_END_DTE, LV_NUM, LV_YEAR, PAY_DTE, CURR_PP_FLAG.
    """
    pay_period_df = read_oracle_table(
        spark, "PAY_PERIOD", schema="HISTDBA", connection_name=connection_name
    )

    current_pp = pay_period_df.filter(F.col("CURR_PP_FLAG") == "Y")

    count = current_pp.count()
    if count == 0:
        raise RuntimeError(
            "No current pay period found. PAY_PERIOD has no rows with CURR_PP_FLAG='Y'."
        )
    if count > 1:
        raise RuntimeError(
            f"Multiple current pay periods found ({count}). "
            "PAY_PERIOD should have exactly one row with CURR_PP_FLAG='Y'."
        )

    logger.info("Current pay period loaded successfully")
    return current_pp


def broadcast_pay_period(spark, connection_name="ORA_BIIS"):
    """
    Return a broadcast-wrapped DataFrame of the current pay period.

    Used for efficient broadcast joins in all ETL jobs that need
    pay period enrichment.

    Parameters
    ----------
    spark : SparkSession
    connection_name : str

    Returns
    -------
    DataFrame
        Broadcast-wrapped single-row DataFrame.
    """
    current_pp = get_current_pay_period(spark, connection_name)
    return F.broadcast(current_pp)


def enrich_with_pay_period(df, spark, connection_name="ORA_BIIS"):
    """
    Add pay period columns to a DataFrame via cross join with broadcast.

    Adds PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE to the DataFrame.
    This is the standard pattern used across CPM, LES, COMPTIME, Pseudossn jobs.

    Parameters
    ----------
    df : DataFrame
        Source DataFrame to enrich.
    spark : SparkSession
    connection_name : str

    Returns
    -------
    DataFrame
        DataFrame with pay period columns added.
    """
    pp_df = broadcast_pay_period(spark, connection_name)

    pp_cols = pp_df.select(
        F.col("PP_NUM").alias("PP_NUM"),
        F.col("PP_END_YEAR").alias("PP_END_YEAR"),
        F.col("PP_START_DTE").alias("PP_START_DTE"),
        F.col("PP_END_DTE").alias("PP_END_DTE"),
    )

    return df.crossJoin(pp_cols)
