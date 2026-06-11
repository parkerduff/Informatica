"""Unit tests for the COMPTIME transform (Spark, in-memory)."""
import hashlib

import pytest

from jobs.comptime import transform

pytestmark = pytest.mark.unit


def _raw(spark):
    return spark.createDataFrame(
        [("999000001", "DOE JOHN", "ACCT", "ORG", "E", "10.00", "2026",
          "20260614", "20260601", "1.00", "8.00", "0")],
        ["SSN", "NAME", "CURRENT_ACCT", "CURRENT_ORG", "FLSA_STATUS",
         "COMP_TIME_CUR_BAL", "COMP_TIME_YEAR_EARNED", "PP_END_DATE",
         "DAILY_DATE_EARNED", "COMP_TIME_RATE", "COMP_TIME_HOURS", "COMP_TIME_UNDEF"],
    )


def test_transform_derives_pay_period_and_hash(spark):
    pp = {"pp_end_year": 2026, "pp_num": 12, "pp_year_num": 202612}
    out = transform(_raw(spark), pp).collect()[0]
    assert int(out["PP_END_YEAR"]) == 2026
    assert int(out["PP_NUM"]) == 12
    assert int(out["PP_YEAR_NUM"]) == 202612
    assert out["SSN_HASH"] == hashlib.sha256(b"999000001").hexdigest()


def test_transform_casts_dates(spark):
    pp = {"pp_end_year": 2026, "pp_num": 12, "pp_year_num": 202612}
    out = transform(_raw(spark), pp).collect()[0]
    assert out["PP_END_DATE"].year == 2026 and out["PP_END_DATE"].month == 6
    assert out["DAILY_DATE_EARNED"].day == 1
    assert out["LOAD_DATE"] is not None
