"""
COUNTER_TBL and ERROR_TBL management.

Replaces Informatica target writes to:
  - NKNIGHT.COUNTER_TBL (used by COMPTIME, Pseudossn, FDA_Leave, CPM, EHRP2BIIS)
  - NKNIGHT.ERROR_TBL (used by Pseudossn, FDA_Leave)

COUNTER_TBL columns:
  PROCESS_NAME, COUNTER_DESCRIPTION, COUNTER_VALUE, PP_END_YEAR, PP_NUM, CYCLE_ID

ERROR_TBL columns:
  PROCESS_NAME, ERROR_MESSAGE, SOURCE_KEY, ERROR_DATE, PP_END_YEAR, PP_NUM, CYCLE_ID
"""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pyspark_migration.common.db_manager import DatabaseManager

logger = logging.getLogger(__name__)

COUNTER_TBL_SCHEMA = StructType([
    StructField("PROCESS_NAME", StringType(), False),
    StructField("COUNTER_DESCRIPTION", StringType(), True),
    StructField("COUNTER_VALUE", DecimalType(18, 0), True),
    StructField("PP_END_YEAR", StringType(), True),
    StructField("PP_NUM", StringType(), True),
    StructField("CYCLE_ID", StringType(), True),
])

ERROR_TBL_SCHEMA = StructType([
    StructField("PROCESS_NAME", StringType(), False),
    StructField("ERROR_MESSAGE", StringType(), True),
    StructField("SOURCE_KEY", StringType(), True),
    StructField("ERROR_DATE", TimestampType(), True),
    StructField("PP_END_YEAR", StringType(), True),
    StructField("PP_NUM", StringType(), True),
    StructField("CYCLE_ID", StringType(), True),
])


class CounterErrorManager:
    """Manages COUNTER_TBL and ERROR_TBL writes.

    Provides methods matching Informatica's counter/error tracking patterns
    found across COMPTIME, Pseudossn, FDA_Leave, CPM, and EHRP2BIIS workflows.
    """

    def __init__(
        self,
        spark: SparkSession,
        db_manager: DatabaseManager,
        counter_table: str = "COUNTER_TBL",
        error_table: str = "ERROR_TBL",
    ):
        self._spark = spark
        self._db = db_manager
        self._counter_table = counter_table
        self._error_table = error_table

    def write_counter(
        self,
        process_name: str,
        description: str,
        value: int,
        pp_end_year: str = "",
        pp_num: str = "",
        cycle_id: str = "",
    ) -> int:
        """Write a counter record to COUNTER_TBL.

        Replaces Informatica target writes like:
          s_COMPTIME -> NKNIGHT.COUNTER_TBL (COUNTER_VALUE = $$WF_RECORD_COUNT)
          s_Pseudossn_Dedup -> COUNTER_TBL (count of dedup'd records)

        Args:
            process_name: Name of the process/job.
            description: Description of what is being counted.
            value: Counter value.
            pp_end_year: Pay period end year.
            pp_num: Pay period number.
            cycle_id: Processing cycle ID.

        Returns:
            Number of rows written (1).
        """
        data = [(
            process_name,
            description,
            Decimal(value),
            pp_end_year,
            pp_num,
            cycle_id,
        )]
        df = self._spark.createDataFrame(data, schema=COUNTER_TBL_SCHEMA)
        rows_written = self._db.write_jdbc(df, self._counter_table)
        logger.info(
            "Counter written: %s / %s = %d (pp=%s/%s, cycle=%s)",
            process_name,
            description,
            value,
            pp_end_year,
            pp_num,
            cycle_id,
        )
        return rows_written

    def write_error(
        self,
        process_name: str,
        error_message: str,
        source_key: str = "",
        pp_end_year: str = "",
        pp_num: str = "",
        cycle_id: str = "",
        error_date: Optional[datetime] = None,
    ) -> int:
        """Write an error record to ERROR_TBL.

        Replaces Informatica ERROR_TBL writes from:
          - s_Pseudossn sessions (validation errors)
          - s_FDA_Leave sessions (validation errors)
          - EHRP2BIIS error tracking

        Args:
            process_name: Name of the process/job.
            error_message: Error description.
            source_key: Source record identifier (e.g., SSN).
            pp_end_year: Pay period end year.
            pp_num: Pay period number.
            cycle_id: Processing cycle ID.
            error_date: Timestamp of the error (defaults to now).

        Returns:
            Number of rows written (1).
        """
        if error_date is None:
            error_date = datetime.now()

        data = [(
            process_name,
            error_message,
            source_key,
            error_date,
            pp_end_year,
            pp_num,
            cycle_id,
        )]
        df = self._spark.createDataFrame(data, schema=ERROR_TBL_SCHEMA)
        rows_written = self._db.write_jdbc(df, self._error_table)
        logger.info(
            "Error written: %s - %s (key=%s, pp=%s/%s)",
            process_name,
            error_message[:100],
            source_key,
            pp_end_year,
            pp_num,
        )
        return rows_written

    def write_error_dataframe(
        self,
        df: DataFrame,
        process_name: str,
        error_message_col: str,
        source_key_col: str,
        pp_end_year: str = "",
        pp_num: str = "",
        cycle_id: str = "",
    ) -> int:
        """Write multiple error records from a DataFrame.

        For bulk error logging when multiple rows fail validation.

        Args:
            df: DataFrame containing error rows.
            process_name: Name of the process/job.
            error_message_col: Column name containing error messages.
            source_key_col: Column name containing source keys.
            pp_end_year: Pay period end year.
            pp_num: Pay period number.
            cycle_id: Processing cycle ID.

        Returns:
            Number of error rows written.
        """
        error_df = df.select(
            F.lit(process_name).alias("PROCESS_NAME"),
            F.col(error_message_col).cast(StringType()).alias("ERROR_MESSAGE"),
            F.col(source_key_col).cast(StringType()).alias("SOURCE_KEY"),
            F.current_timestamp().alias("ERROR_DATE"),
            F.lit(pp_end_year).alias("PP_END_YEAR"),
            F.lit(pp_num).alias("PP_NUM"),
            F.lit(cycle_id).alias("CYCLE_ID"),
        )
        return self._db.write_jdbc(error_df, self._error_table)
