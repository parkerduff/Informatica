"""
COUNTER_TBL and ERROR_TBL management for PySpark migration.

Replaces Informatica counter and error tracking patterns used across all jobs:
- COUNTER_TBL writes via INFO_TARGET (e.g., XML/COMPTIME lines 786-796)
- ERROR_TBL writes for validation failures (e.g., XML/FDA_Leave lines 1321-1356)
- exp_Counters transformations that compile counter values
- exp_Final transformations that set RUN_DATE = SESSSTARTTIME and PROCESS_NAME = $PMMappingName
"""

import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
)

from pyspark_migration.common.db_manager import DatabaseManager

logger = logging.getLogger(__name__)

# COUNTER_TBL schema
# Columns: RUN_DATE, PROCESS_NAME, COUNTER_DESCRIPTION, COUNTER_VALUE, PP_END_YEAR, PP_NUM, CYCLE_ID
COUNTER_SCHEMA = StructType([
    StructField("RUN_DATE", TimestampType(), False),
    StructField("PROCESS_NAME", StringType(), False),
    StructField("COUNTER_DESCRIPTION", StringType(), False),
    StructField("COUNTER_VALUE", DoubleType(), False),
    StructField("PP_END_YEAR", IntegerType(), True),
    StructField("PP_NUM", IntegerType(), True),
    StructField("CYCLE_ID", IntegerType(), True),
])

# ERROR_TBL schema
# Columns: PROCESS_NAME, ERROR_MESSAGE, SOURCE_KEY, ERROR_DATE, PP_END_YEAR, PP_NUM, CYCLE_ID
ERROR_SCHEMA = StructType([
    StructField("PROCESS_NAME", StringType(), False),
    StructField("ERROR_MESSAGE", StringType(), True),
    StructField("SOURCE_KEY", StringType(), True),
    StructField("ERROR_DATE", TimestampType(), False),
    StructField("PP_END_YEAR", IntegerType(), True),
    StructField("PP_NUM", IntegerType(), True),
    StructField("CYCLE_ID", IntegerType(), True),
])


class CounterManager:
    """Manages COUNTER_TBL writes.
    
    Replaces Informatica exp_Counters and exp_Final transformations:
    - exp_Final.o_RUN_DATE = SESSSTARTTIME → datetime.now()
    - exp_Final.o_PROCESS_NAME = $PMMappingName → mapping_name constant
    - exp_Counters compiles DETAIL_RECORD_COUNT, lkp_PP_NUM, lkp_PP_END_YEAR
    """

    def __init__(self, db_manager: DatabaseManager):
        self.spark = db_manager.spark
        self.db_manager = db_manager

    def write_counter(
        self,
        process_name: str,
        counter_description: str,
        counter_value: float,
        pp_end_year: Optional[int] = None,
        pp_num: Optional[int] = None,
        cycle_id: Optional[int] = None,
        run_date: Optional[datetime] = None
    ) -> None:
        """Write a single counter record to COUNTER_TBL.
        
        Replaces Informatica target write to COUNTER_TBL via INFO_TARGET.
        Reject file equivalent: counter_tbl1.bad → logged errors
        """
        if run_date is None:
            run_date = datetime.now()

        row = [(run_date, process_name, counter_description, float(counter_value),
                pp_end_year, pp_num, cycle_id)]
        df = self.spark.createDataFrame(row, schema=COUNTER_SCHEMA)
        self.db_manager.write_jdbc(df, "COUNTER_TBL", mode="append", connection="target")
        logger.info(
            f"Counter written: {process_name} - {counter_description} = {counter_value} "
            f"(PP: {pp_end_year}-{pp_num}, Cycle: {cycle_id})"
        )

    def write_counters_batch(
        self,
        process_name: str,
        counters: dict,
        pp_end_year: Optional[int] = None,
        pp_num: Optional[int] = None,
        cycle_id: Optional[int] = None,
        run_date: Optional[datetime] = None
    ) -> None:
        """Write multiple counter records at once.
        
        Args:
            counters: Dict of {counter_description: counter_value}
        """
        if run_date is None:
            run_date = datetime.now()

        rows = [
            (run_date, process_name, desc, float(val), pp_end_year, pp_num, cycle_id)
            for desc, val in counters.items()
        ]
        df = self.spark.createDataFrame(rows, schema=COUNTER_SCHEMA)
        self.db_manager.write_jdbc(df, "COUNTER_TBL", mode="append", connection="target")
        logger.info(f"Wrote {len(counters)} counters for {process_name}")


class ErrorManager:
    """Manages ERROR_TBL writes.
    
    Replaces Informatica error tracking patterns:
    - m_0150_PM_FDA_Error_Counter: 4 parallel lookup validations writing to ERROR_TBL
    - General error capture across all jobs
    """

    def __init__(self, db_manager: DatabaseManager):
        self.spark = db_manager.spark
        self.db_manager = db_manager

    def write_error(
        self,
        process_name: str,
        error_message: str,
        source_key: str = "",
        pp_end_year: Optional[int] = None,
        pp_num: Optional[int] = None,
        cycle_id: Optional[int] = None,
        error_date: Optional[datetime] = None
    ) -> None:
        """Write a single error record to ERROR_TBL."""
        if error_date is None:
            error_date = datetime.now()

        row = [(process_name, error_message, source_key, error_date,
                pp_end_year, pp_num, cycle_id)]
        df = self.spark.createDataFrame(row, schema=ERROR_SCHEMA)
        self.db_manager.write_jdbc(df, "ERROR_TBL", mode="append", connection="target")
        logger.info(f"Error written: {process_name} - {error_message}")

    def write_errors_from_df(
        self,
        error_df: DataFrame,
        process_name: str,
        error_message_col: str,
        source_key_col: str,
        pp_end_year: Optional[int] = None,
        pp_num: Optional[int] = None,
        cycle_id: Optional[int] = None
    ) -> int:
        """Write error records from a DataFrame (e.g., left-anti join results).
        
        Replaces Informatica m_0150_PM_FDA_Error_Counter pattern:
        4 separate left-anti joins, each writing to ERROR_TBL_* targets.
        
        Returns:
            Number of error records written
        """
        error_count = error_df.count()
        if error_count == 0:
            logger.info(f"No errors to write for {process_name}")
            return 0

        formatted_df = error_df.select(
            lit(process_name).alias("PROCESS_NAME"),
            error_df[error_message_col].alias("ERROR_MESSAGE"),
            error_df[source_key_col].alias("SOURCE_KEY"),
            current_timestamp().alias("ERROR_DATE"),
            lit(pp_end_year).alias("PP_END_YEAR"),
            lit(pp_num).alias("PP_NUM"),
            lit(cycle_id).alias("CYCLE_ID"),
        )

        self.db_manager.write_jdbc(formatted_df, "ERROR_TBL", mode="append", connection="target")
        logger.info(f"Wrote {error_count} error records for {process_name}")
        return error_count
