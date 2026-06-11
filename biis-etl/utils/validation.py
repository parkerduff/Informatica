"""Lightweight data-quality validators shared across jobs."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame


class ValidationError(Exception):
    """Raised when a data-quality assertion fails."""


def validate_non_empty(df: "DataFrame", name: str = "dataframe") -> "DataFrame":
    """Fail if ``df`` has zero rows."""
    if df.rdd.isEmpty():
        raise ValidationError(f"{name} is empty (expected at least one row)")
    return df


def validate_exactly_one(value: int, name: str = "row") -> int:
    """Fail unless exactly one ``value`` is present (used by Pay Calendar)."""
    if value != 1:
        raise ValidationError(
            f"Expected exactly one {name}, found {value}"
        )
    return value


def validate_row_count(df: "DataFrame", expected: int, name: str = "dataframe") -> "DataFrame":
    actual = df.count()
    if actual != expected:
        raise ValidationError(f"{name}: expected {expected} rows, found {actual}")
    return df


def validate_no_nulls(df: "DataFrame", columns) -> "DataFrame":
    from pyspark.sql import functions as F

    for col in columns:
        nulls = df.filter(F.col(col).isNull()).count()
        if nulls:
            raise ValidationError(f"Column {col} has {nulls} null value(s)")
    return df
