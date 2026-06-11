"""Unit tests for the EHRP2BIIS ETL target builder."""
import datetime as dt

import pytest

from jobs.ehrp2biis.etl import _build_target

pytestmark = pytest.mark.unit


TARGET_COLS = [
    ("EVENT_ID", "number", "15", "0"),
    ("LOAD_ID", "varchar2", "8", "0"),
    ("ACTION", "varchar2", "3", "0"),
    ("DERIVED_ONLY", "varchar2", "10", "0"),
    ("LOAD_DATE", "date", "19", "0"),
]


def test_build_target_copies_nulls_and_stamps_load_id(spark):
    joined = spark.createDataFrame(
        [(1001, "PAY")], ["EVENT_ID", "ACTION"]
    )
    plan = {"copy": ["ACTION"], "null": ["DERIVED_ONLY"]}
    out = _build_target(joined, TARGET_COLS, plan, dt.date(2026, 6, 11))
    row = out.collect()[0]
    assert int(row["EVENT_ID"]) == 1001
    assert row["ACTION"] == "PAY"
    assert row["DERIVED_ONLY"] is None
    assert row["LOAD_ID"] == "20260611"
    assert row["LOAD_DATE"].year == 2026
