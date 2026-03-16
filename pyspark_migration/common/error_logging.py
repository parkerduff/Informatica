"""
Error and Counter Logging

ERROR_TBL and COUNTER_TBL DataFrame writers.
Replaces Informatica error/counter target transformations used across all jobs.
"""

import logging
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from pyspark_migration.common.db_utils import write_oracle_table

logger = logging.getLogger(__name__)

# ERROR_TBL schema from Pseudossn lines 441-449
ERROR_TBL_SCHEMA = StructType([
    StructField("PROCESS_NAME", StringType(), True),
    StructField("ERROR_MESSAGE", StringType(), True),
    StructField("SOURCE_KEY", StringType(), True),
    StructField("ERROR_DATE", DateType(), True),
    StructField("PP_END_YEAR", DecimalType(4, 0), True),
    StructField("PP_NUM", DecimalType(2, 0), True),
    StructField("CYCLE_ID", DecimalType(1, 0), True),
    StructField("ERROR_CODE", StringType(), True),
])

# COUNTER_TBL schema from Pseudossn lines 521-529
COUNTER_TBL_SCHEMA = StructType([
    StructField("RUN_DATE", DateType(), True),
    StructField("PROCESS_NAME", StringType(), True),
    StructField("COUNTER_DESCRIPTION", StringType(), True),
    StructField("COUNTER_VALUE", LongType(), True),
    StructField("PP_END_YEAR", DecimalType(4, 0), True),
    StructField("PP_NUM", DecimalType(2, 0), True),
    StructField("CYCLE_ID", DecimalType(1, 0), True),
])


def log_errors(spark, error_df, process_name, pp_end_year, pp_num,
               cycle_id=None, connection_name="ORA_BIIS"):
    """
    Write error records to ERROR_TBL.

    The error_df should have columns: ERROR_MESSAGE, SOURCE_KEY, ERROR_CODE.
    This function adds PROCESS_NAME, ERROR_DATE, PP_END_YEAR, PP_NUM, CYCLE_ID.

    Parameters
    ----------
    spark : SparkSession
    error_df : DataFrame
        Must contain ERROR_MESSAGE and SOURCE_KEY columns at minimum.
    process_name : str
    pp_end_year : int
    pp_num : int
    cycle_id : int, optional
    connection_name : str
    """
    if error_df.count() == 0:
        logger.info("No errors to log for %s", process_name)
        return

    enriched = (
        error_df
        .withColumn("PROCESS_NAME", F.lit(process_name))
        .withColumn("ERROR_DATE", F.current_date())
        .withColumn("PP_END_YEAR", F.lit(pp_end_year).cast(DecimalType(4, 0)))
        .withColumn("PP_NUM", F.lit(pp_num).cast(DecimalType(2, 0)))
        .withColumn("CYCLE_ID", F.lit(cycle_id).cast(DecimalType(1, 0)))
    )

    if "ERROR_CODE" not in enriched.columns:
        enriched = enriched.withColumn("ERROR_CODE", F.lit(None).cast(StringType()))

    final = enriched.select(
        "PROCESS_NAME", "ERROR_MESSAGE", "SOURCE_KEY",
        "ERROR_DATE", "PP_END_YEAR", "PP_NUM", "CYCLE_ID", "ERROR_CODE",
    )

    write_oracle_table(final, "ERROR_TBL", connection_name=connection_name, mode="append")
    logger.info(
        "Logged %d errors for process %s (PP %d/%d)",
        final.count(), process_name, pp_end_year, pp_num,
    )


def log_counter(spark, process_name, description, value, pp_end_year, pp_num,
                cycle_id=None, connection_name="ORA_BIIS"):
    """
    Write a counter record to COUNTER_TBL.

    Replaces Informatica exp_Final -> COUNTER_TBL target pattern.

    Parameters
    ----------
    spark : SparkSession
    process_name : str
    description : str
    value : int
    pp_end_year : int
    pp_num : int
    cycle_id : int, optional
    connection_name : str
    """
    from decimal import Decimal as D

    row = [(
        datetime.now().date(),
        process_name,
        description,
        int(value),
        D(str(pp_end_year)),
        D(str(pp_num)),
        D(str(cycle_id)) if cycle_id is not None else None,
    )]

    counter_df = spark.createDataFrame(row, schema=COUNTER_TBL_SCHEMA)

    write_oracle_table(
        counter_df, "COUNTER_TBL", connection_name=connection_name, mode="append"
    )
    logger.info(
        "Logged counter: %s = %d for %s (PP %d/%d)",
        description, value, process_name, pp_end_year, pp_num,
    )
