"""Reusable PySpark date-conversion expressions.

These mirror the ``IS_DATE`` / ``TO_DATE`` transformation patterns in the
Informatica mappings (see the Pseudossn export, lines ~795-865), where source
dates arrive as fixed-width strings in MMDDYYYY, YYYYMMDD or YYYYDDMM order.
"""
from pyspark.sql import Column
from pyspark.sql import functions as F


def parse_mmddyyyy(col: Column) -> Column:
    """Convert an MMDDYYYY string to a date. Returns null on invalid dates."""
    formatted = F.concat(
        F.substring(col, 1, 2),
        F.lit("/"),
        F.substring(col, 3, 2),
        F.lit("/"),
        F.substring(col, 5, 4),
    )
    return F.to_date(formatted, "MM/dd/yyyy")


def parse_yyyymmdd(col: Column) -> Column:
    """Convert a YYYYMMDD string to a date. Returns null on invalid dates."""
    return F.to_date(col, "yyyyMMdd")


def parse_yyyyddmm(col: Column) -> Column:
    """Convert a YYYYDDMM string to a date (note: day before month).

    Returns null on invalid dates.
    """
    formatted = F.concat(
        F.substring(col, 7, 2),  # MM
        F.lit("/"),
        F.substring(col, 5, 2),  # DD
        F.lit("/"),
        F.substring(col, 1, 4),  # YYYY
    )
    return F.to_date(formatted, "MM/dd/yyyy")
