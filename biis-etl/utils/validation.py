"""Validation helpers used by tests and the HTML report generator."""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils import db


def validate_row_count(actual_df: DataFrame, expected_count: int, table_name: str) -> None:
    actual = actual_df.count()
    if actual != expected_count:
        raise AssertionError(
            f"{table_name}: expected {expected_count} rows, got {actual}"
        )


def validate_schema(actual_df: DataFrame, expected_schema: Dict[str, str]) -> None:
    """Compare column names (and, when provided, simple type names)."""
    actual = {f.name: f.dataType.simpleString() for f in actual_df.schema.fields}
    for col, expected_type in expected_schema.items():
        if col not in actual:
            raise AssertionError(f"Missing column {col}; have {sorted(actual)}")
        if expected_type and expected_type not in actual[col]:
            raise AssertionError(
                f"Column {col}: expected type ~{expected_type}, got {actual[col]}"
            )


def validate_no_nulls(df: DataFrame, columns: List[str]) -> None:
    for col in columns:
        nulls = df.filter(F.col(col).isNull()).count()
        if nulls > 0:
            raise AssertionError(f"Column {col} has {nulls} NULL values")


def validate_single_current_pp(spark, config: Dict[str, Any]) -> int:
    """Assert exactly one PAY_PERIOD row has CURR_PP_FLAG='Y'; return the count."""
    count = db.execute_scalar(
        config, "SELECT COUNT(*) FROM PAY_PERIOD WHERE CURR_PP_FLAG = 'Y'"
    )
    if count != 1:
        raise AssertionError(f"Expected exactly 1 current pay period, found {count}")
    return count


def compare_dataframes(
    actual_df: DataFrame, expected_df: DataFrame, key_columns: List[str]
) -> Dict[str, Any]:
    """Row-level comparison keyed on ``key_columns``. Returns a diff report dict."""
    actual_only = actual_df.join(expected_df, key_columns, "left_anti")
    expected_only = expected_df.join(actual_df, key_columns, "left_anti")
    return {
        "matches": actual_only.count() == 0 and expected_only.count() == 0,
        "actual_only_count": actual_only.count(),
        "expected_only_count": expected_only.count(),
    }
