"""Replaces the core of the ``FDA_Leave`` workflow (XML/FDA_Leave).

The original workflow is a chain of small mappings (verify file, load TATRAN to
DB, counters, create output file, send email). The two data-bearing steps are
reproduced here:

* ``m_0100_PM_FDA_Load_TATRAN_To_DB`` -- parse the fixed-width TATRAN flat file
  and load ``HI_PM_FDA_TATRAN_TBL`` (a batch id + sequence are stamped on each
  row).
* ``m_0300_PM_FDA_Create_Output_File`` -- emit the detail (``FDA_REC_TYPE = '02'``)
  rows to a fixed-width output file.

The bookkeeping mappings (counters, CPM_CYCLE_TBL update, email) are handled by
the orchestrator / utils.notifications.
"""
from __future__ import annotations

import logging
from typing import Optional

from pyspark_etl.config import connections
from pyspark_etl.config.schemas import FixedField
from pyspark_etl.utils.oracle_jdbc import read_table, write_table

logger = logging.getLogger(__name__)

TATRAN_TABLE = "HI_PM_FDA_TATRAN_TBL"

# HI_PM_FDA_TATRAN_FLAT layout.
TATRAN_FLAT = [
    FixedField("FDA_TK_NO", 0, 5),
    FixedField("FDA_EMP_ID", 5, 9),
    FixedField("FDA_PP_YEAR", 14, 4),
    FixedField("FDA_PP_NUM", 18, 2),
    FixedField("FDA_REC_TYPE", 20, 2),
    FixedField("FDA_DATA", 22, 118),
]


def load_tatran_to_db(spark, input_file: str, batch_id: int,
                      target_schema: Optional[str] = None, write: bool = True):
    """m_0100: parse the flat file and load HI_PM_FDA_TATRAN_TBL."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    schema = target_schema or connections.SCHEMA_INFO_TARGET
    raw = spark.read.text(input_file)
    df = raw.select(*[
        F.substring(F.col("value"), f.spark_start, f.length).alias(f.name)
        for f in TATRAN_FLAT
    ])
    seq = Window.orderBy(F.monotonically_increasing_id())
    df = (
        df.withColumn("FDA_BATCH_ID", F.lit(batch_id))
          .withColumn("FDA_SEQ", F.row_number().over(seq))
          .select("FDA_BATCH_ID", "FDA_TK_NO", "FDA_EMP_ID", "FDA_PP_YEAR",
                  "FDA_PP_NUM", "FDA_REC_TYPE", "FDA_SEQ", "FDA_DATA")
    )
    if write:
        write_table(df, "ORA_BIIS", schema, TATRAN_TABLE)
    logger.info("Loaded TATRAN flat file %s (batch %s)", input_file, batch_id)
    return df


def create_output_file(spark, output_path: str, batch_id: Optional[int] = None,
                       source_schema: Optional[str] = None, write: bool = True):
    """m_0300: emit FDA_REC_TYPE = '02' detail rows to a fixed-width file."""
    from pyspark.sql import functions as F

    schema = source_schema or connections.SCHEMA_INFO_TARGET
    df = read_table(spark, "ORA_BIIS", schema, TATRAN_TABLE)
    df = df.filter(F.col("FDA_REC_TYPE") == "02")
    if batch_id is not None:
        df = df.filter(F.col("FDA_BATCH_ID") == batch_id)
    out = df.select(
        F.concat(F.coalesce(F.col("FDA_TK_NO"), F.lit("")),
                 F.coalesce(F.col("FDA_DATA"), F.lit(""))).alias("value")
    )
    if write:
        out.coalesce(1).write.mode("overwrite").text(output_path)
    logger.info("Wrote FDA leave extract to %s", output_path)
    return out


def run(spark, input_file: str, output_path: str, batch_id: int,
        write: bool = True):
    load_tatran_to_db(spark, input_file, batch_id, write=write)
    return create_output_file(spark, output_path, batch_id, write=write)
