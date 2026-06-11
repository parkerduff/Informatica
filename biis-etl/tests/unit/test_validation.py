"""Unit tests for the data-quality validation helpers."""
import pytest

from utils import validation
from utils.validation import (ValidationError, validate_no_nulls,
                              validate_row_count, validate_schema)

pytestmark = pytest.mark.unit


def _df(spark, rows, cols):
    return spark.createDataFrame(rows, cols)


def test_validate_row_count_ok_and_bounds(spark):
    df = _df(spark, [(1,), (2,)], ["x"])
    assert validate_row_count(df, expected_min=1, expected_max=5) == 2
    with pytest.raises(ValidationError):
        validate_row_count(df, expected_min=3)
    with pytest.raises(ValidationError):
        validate_row_count(df, expected_max=1)


def test_validate_schema(spark):
    df = _df(spark, [(1, 2)], ["a", "b"])
    validate_schema(df, ["a", "b"])
    with pytest.raises(ValidationError):
        validate_schema(df, ["a", "c"])


def test_validate_no_nulls(spark):
    good = _df(spark, [(1, 2)], ["a", "b"])
    validate_no_nulls(good, ["a", "b"])
    bad = spark.createDataFrame([(1, None)], "a int, b int")
    with pytest.raises(ValidationError):
        validate_no_nulls(bad, ["b"])
    validate_no_nulls(good, [])  # no columns -> no-op


def test_log_row_count_inserts_counter():
    captured = {}

    class Cur:
        rowcount = 1

        def execute(self, sql, params=None):
            captured["sql"], captured["params"] = sql, params
            return self

    class Conn:
        def cursor(self):
            return Cur()

    validation.log_row_count(Conn(), "COMP_TIME_DAILY_TBL", "COMPTIME", 100,
                             {"pp_end_year": 2026, "pp_num": 12})
    assert "COUNTER_TBL" in captured["sql"]
    assert 100 in captured["params"]
