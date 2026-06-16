"""Record-type detection for the PSEUDOSSN fixed-width feed.

Mirrors the ``v_RECORD_TYPE_FLAG`` / ``o_RECORD_TYPE_FLAG`` logic in the
Pseudossn mapping (lines ~790-791 and ~803-804): header rows start with
``HEADER``, trailer rows with ``TRAILER``, everything else is a detail row, and
detail rows are only kept when the SSN is numeric.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_record_type_columns(df: DataFrame, ssn_col: str = "SSN") -> DataFrame:
    """Add record-type classification columns.

    - ``record_type``: 'H' (header), 'T' (trailer), 'D' (detail)
    - ``ssn_is_numeric``: True if the SSN value is numeric
    """
    return df.withColumn(
        "record_type",
        F.when(F.substring(F.col(ssn_col), 1, 6) == "HEADER", F.lit("H"))
        .when(F.substring(F.col(ssn_col), 1, 7) == "TRAILER", F.lit("T"))
        .otherwise(F.lit("D")),
    ).withColumn(
        "ssn_is_numeric",
        F.col(ssn_col).rlike("^[0-9]+$"),
    )


def filter_detail_records(df: DataFrame) -> DataFrame:
    """Keep only detail records with valid numeric SSNs."""
    return df.filter(
        (F.col("record_type") == "D") & (F.col("ssn_is_numeric"))
    )
